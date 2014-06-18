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
	"sync"
	"time"

	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/log"
)

var (
	serverAddr = flag.String("addr", "http://localhost:6064/v1/api/blobserver", "server addr")
	workers    = flag.Int("workers", 2, "worker count")
	useMulti   = flag.Bool("multi", false, "use multi uploader")
	counter    int
	lastPrint  time.Time
)

func main() {
	flag.Parse()
	log.Println("start uploader")

	if flag.NArg() == 0 {
		log.Fatal("Expected file argument missing")
	}

	if *useMulti {
		multiUploader(flag.Args())
		return
	}

	queue := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		go uploader(queue, &wg)
	}

	lastPrint = time.Now()

	for _, filename := range flag.Args() {
		wg.Add(1)
		queue <- filename
	}
	wg.Wait()
}

func count() {
	counter++

	if time.Since(lastPrint) > time.Second {
		log.Printf("uploads per second %d\n", counter)
		lastPrint = time.Now()
		counter = 0
	}
}

func uploader(ch chan string, wg *sync.WaitGroup) {
	for {
		path := <-ch
		err := handleUpload(path)

		if err != nil {
			log.Error(err)
		}
		wg.Done()
		count()
	}
}

func multiUploader(paths []string) error {
	res, err := multiStat(paths)

	if err != nil {
		return err
	}

	toUpload := make([]string, 0, len(paths))

	for path, ok := range res {
		if !ok {
			toUpload = append(toUpload, path)
		} else {
			log.Printf("%s exists - skipping", path)
		}
	}

	if len(toUpload) == 0 {
		return nil
	}

	log.Printf("upload %d %v", len(toUpload), toUpload)
	return multiUpload(toUpload)
}

func multiStat(paths []string) (map[string]bool, error) {
	md5s := make(map[string]string, len(paths))
	results := make(map[string]bool, len(paths))
	values := url.Values{}

	for _, path := range paths {
		file, err := os.Open(path)

		if err != nil {
			return nil, err
		}

		hasher := md5.New()
		io.Copy(hasher, file)
		md5Exp := hex.EncodeToString(hasher.Sum(nil))
		err = file.Close()

		if err != nil {
			return nil, err
		}

		md5s[path] = md5Exp
		results[path] = false
		values.Add("blob", filepath.Base(path))
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
	parseResponse(res, sr)

	for _, si := range sr.Stat {
		results[si.Path] = md5s[si.Path] == si.MD5
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

func handleUpload(path string) error {
	ok, err := stat(path)

	if err != nil {
		return err
	}

	if ok {
		log.Printf("%s exists - skipping", path)
		return nil
	}

	log.Printf("%s upload", path)
	return upload(path)
}

func stat(path string) (bool, error) {
	file, err := os.Open(path)

	if err != nil {
		return false, err
	}

	defer file.Close()
	filename := filepath.Base(path)
	uri := absURL("/blob/stat/", url.Values{"blob": []string{filename}})
	req, err := http.NewRequest("GET", uri, nil)

	if err != nil {
		return false, err
	}

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		return false, err
	}

	sr := new(protocol.StatResponse)
	parseResponse(res, sr)

	if len(sr.Stat) != 1 {
		return false, nil
	}

	si := sr.Stat[0]
	hasher := md5.New()
	io.Copy(hasher, file)
	md5Exp := hex.EncodeToString(hasher.Sum(nil))
	return si.MD5 == md5Exp, nil
}

func upload(path string) error {
	file, err := os.Open(path)

	if err != nil {
		return err
	}
	defer file.Close()

	filename := filepath.Base(path)
	req, err := multipartRequest("/blob/upload/", filename, file)

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

	if len(ur.Received) != 1 {
		return fmt.Errorf("Expected 1 received got %d", len(ur.Received))
	}

	log.Println(ur.Received[0].Path)
	return nil
}

func multipartRequest(path, filename string, file io.Reader) (req *http.Request, err error) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	log.Print(filename)
	part, err := w.CreateFormFile("file", filename)

	if err != nil {
		w.Close()
		return
	}

	if _, err = io.Copy(part, file); err != nil {
		w.Close()
		return
	}

	w.Close()

	uri := absURL(path, url.Values{"use-filename": []string{"true"}})
	req, err = http.NewRequest("POST", uri, &b)

	if err != nil {
		return
	}

	// content type, contains the boundary.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return
}

func parseResponse(res *http.Response, v interface{}) {
	defer res.Body.Close()

	if c := res.Header.Get("Content-Type"); !strings.Contains(c, "application/json") {
		log.Error("Unexpected Content-Type")
	}

	reader := bufio.NewReader(res.Body)
	buf, _ := ioutil.ReadAll(reader)
	err := json.Unmarshal(buf, v)
	//fmt.Printf("%s\n", buf)
	//err := json.NewDecoder(res.Body).Decode(v)

	if err != nil {
		log.Error(err)
	}
}

func absURL(endpoint string, args url.Values) string {
	var params string

	if args != nil && len(args) > 0 {
		params = fmt.Sprintf("?%s", args.Encode())
	}

	return fmt.Sprintf("%s%s%s", *serverAddr, endpoint, params)
}
