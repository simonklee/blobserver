package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/log"
)

var (
	serverAddr = flag.String("addr", "http://localhost:6064/v1/api/blobserver", "server addr")
	outputType = flag.String("output", "json", "print result")
	baseURL    string
)

func main() {
	flag.Parse()
	log.Println("start uploader")

	if flag.NArg() == 0 {
		log.Fatal("Expected file argument missing")
	}

	bURL, err := findBaseURL()

	if err != nil {
		log.Fatal(err)
	}

	baseURL = bURL
	res, _ := multiUploader(flag.Args())
	var enc resourceEncoder

	switch *outputType {
	case "json":
		enc = newJSONEncoder(os.Stdout)
	default:
		os.Exit(0)
	}

	err = enc.Encode(res)

	if err != nil {
		log.Fatal(err)
	}
}

type resourceEncoder interface {
	Encode(res resources) error
}

type JSONEncoder struct {
	w *json.Encoder
}

func newJSONEncoder(w io.Writer) *JSONEncoder {
	return &JSONEncoder{w: json.NewEncoder(w)}
}

func (e *JSONEncoder) Encode(res resources) error {
	out := make(map[string]*resource, len(res))

	for _, v := range res {
		out[v.Path] = v
	}

	return e.w.Encode(out)
}

func findBaseURL() (string, error) {
	res, err := http.Get(absURL("/config/", nil))

	if err != nil {
		return "", err
	}

	cr := new(protocol.ConfigResponse)

	if err := parseResponse(res, cr); err != nil {
		return "", err
	}

	return cr.Data.CDNUrl, nil
}

type resource struct {
	Filename string
	Path     string `json:"-"`
	URL      string
	MD5      string
	Created  bool
}

func (r *resource) setURL(path string) {
	r.URL = baseURL + path
}

type resources []*resource

func (res resources) findByPath(path string) *resource {
	filename := pathToFilename(path)

	for j := 0; j < len(res); j++ {
		if res[j].Filename == filename {
			return res[j]
		}
	}
	return nil
}

func multiUploader(paths []string) (files resources, err error) {
	files, err = multiStat(paths)

	if err != nil {
		return nil, err
	}

	toUpload := make([]string, 0, len(paths))

	for _, res := range files {
		if res.Created {
			toUpload = append(toUpload, res.Path)
		} else {
			log.Printf("%s exists - skipping", res.Path)
		}
	}

	if len(toUpload) == 0 {
		return files, nil
	}

	log.Printf("upload %d %v", len(toUpload), toUpload)
	err = multiUpload(files, toUpload)
	return files, err
}

func pathToFilename(path string) string {
	pc := strings.SplitN(path, "/", 2)

	if len(pc) == 2 {
		return pc[1]
	}

	return path
}

func multiStat(paths []string) (resources, error) {
	results := make(resources, 0, len(paths))
	values := url.Values{}

	for _, path := range paths {
		fp, err := os.Open(path)

		if err != nil {
			return nil, err
		}

		hasher := md5.New()
		io.Copy(hasher, fp)
		resMD5 := hex.EncodeToString(hasher.Sum(nil))
		err = fp.Close()

		if err != nil {
			return nil, err
		}

		res := &resource{
			MD5:      resMD5,
			Filename: filepath.Base(path),
			Path:     path,
			Created:  true,
		}

		results = append(results, res)
		values.Add("blob", res.Filename)
	}

	uri := absURL("/blob/stat/", values)
	req, err := http.NewRequest("GET", uri, nil)

	if err != nil {
		return results, err
	}

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		return results, err
	}

	sr := new(protocol.StatResponse)

	if err := parseResponse(res, sr); err != nil {
		return nil, err
	}

	for _, si := range sr.Stat {
		cur := results.findByPath(si.Path)

		if cur == nil {
			log.Errorf("unexpected result %v", si)
			continue
		}

		cur.Created = si.MD5 != cur.MD5
		cur.URL = baseURL + si.Path
	}

	return results, nil
}

func multiUpload(results resources, toUpload []string) error {
	req, err := multiMultipartRequest("/blob/upload/", toUpload)

	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		return err
	}

	if res.StatusCode != 201 {
		return fmt.Errorf("Unexpected status code %d", res.StatusCode)
	}

	ur := new(protocol.UploadResponse)
	parseResponse(res, ur)

	if len(ur.Received) != len(toUpload) {
		return fmt.Errorf("Expected %d received got %d", len(toUpload), len(ur.Received))
	}

	for _, rec := range ur.Received {
		log.Println("got:", rec.Path)
		cur := results.findByPath(rec.Path)

		if cur == nil {
			log.Errorf("unexpected result %v", rec)
			continue
		}

		cur.URL = baseURL + rec.Path
	}

	return nil
}

func multiMultipartRequest(endpoint string, paths []string) (*http.Request, error) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	for _, path := range paths {
		file, err := os.Open(path)

		if err != nil {
			return nil, err
		}

		filename := filepath.Base(path)
		part, err := w.CreateFormFile("file", filename)

		if err != nil {
			file.Close()
			w.Close()
			return nil, err
		}

		if _, err = io.Copy(part, file); err != nil {
			file.Close()
			w.Close()
			return nil, err
		}

		file.Close()
	}

	w.Close()

	uri := absURL(endpoint, url.Values{"use-filename": []string{"true"}})
	req, err := http.NewRequest("POST", uri, &b)

	if err != nil {
		return nil, err
	}

	// content type, contains the boundary.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func parseResponse(res *http.Response, v interface{}) error {
	defer res.Body.Close()

	if c := res.Header.Get("Content-Type"); !strings.Contains(c, "application/json") {
		log.Error("Unexpected Content-Type")
		return fmt.Errorf("Unexpected Content-Type")
	}

	reader := bufio.NewReader(res.Body)
	buf, _ := ioutil.ReadAll(reader)
	err := json.Unmarshal(buf, v)
	//fmt.Printf("%s\n", buf)
	//err := json.NewDecoder(res.Body).Decode(v)
	return err
}

func absURL(endpoint string, args url.Values) string {
	var params string

	if args != nil && len(args) > 0 {
		params = fmt.Sprintf("?%s", args.Encode())
	}

	return fmt.Sprintf("%s%s%s", *serverAddr, endpoint, params)
}
