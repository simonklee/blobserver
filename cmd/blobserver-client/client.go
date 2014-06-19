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
	"time"

	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/log"
)

var (
	serverAddr = flag.String("addr", "http://localhost:6064/v1/api/blobserver", "server addr")
	counter    int
	lastPrint  time.Time
)

func main() {
	flag.Parse()
	log.Println("start uploader")

	if flag.NArg() == 0 {
		log.Fatal("Expected file argument missing")
	}
	urls, _ := multiUploader(flag.Args())
	log.Println(urls)
	return
}

func count() {
	counter++

	if time.Since(lastPrint) > time.Second {
		log.Printf("uploads per second %d\n", counter)
		lastPrint = time.Now()
		counter = 0
	}
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
	Path     string
	URL      string
	Created  bool
	MD5      string
}

func multiUploader(paths []string) (files []*resource, err error) {
	files, err = multiStat(paths)

	if err != nil {
		return nil, err
	}

	toUpload := make([]string, 0, len(paths))

	for _, res := range files {
		if !res.Created {
			toUpload = append(toUpload, res.Path)
		} else {
			log.Printf("%s exists - skipping", res.Path)
		}
	}

	if len(toUpload) == 0 {
		return files, nil
	}

	log.Printf("upload %d %v", len(toUpload), toUpload)
	err = multiUpload(toUpload)
	return files, err
}

func pathToFilename(path string) string {
	pc := strings.SplitN(path, "/", 2)

	if len(pc) == 2 {
		return pc[1]
	}

	return path
}

func multiStat(paths []string) ([]*resource, error) {
	results := make([]*resource, 0, len(paths))
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

	baseURL, err := findBaseURL()

	if err != nil {
		return nil, err
	}

	for _, si := range sr.Stat {
		filename := pathToFilename(si.Path)
		var cur *resource

		for j := 0; j < len(results); j++ {
			if results[j].Filename == filename {
				cur = results[j]
			}
		}

		if cur == nil {
			log.Errorf("unexpected result %v", si)
			continue
		}

		cur.Created = si.MD5 == cur.MD5
		cur.URL = baseURL + si.Path
	}

	return results, nil
}

func multiUpload(paths []string) error {
	req, err := multiMultipartRequest("/blob/upload/", paths)

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

	if len(ur.Received) != len(paths) {
		return fmt.Errorf("Expected %d received got %d", len(paths), len(ur.Received))
	}

	for _, rec := range ur.Received {
		log.Println("got:", rec.Path)
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
