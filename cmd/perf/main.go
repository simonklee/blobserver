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
	"strings"
	"time"

	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/log"
)

var (
	serverAddr = flag.String("addr", "http://localhost:6064/v1/api/blobserver", "server addr")
	workers    = flag.Int("workers", 10, "worker count")
	counter    int
	lastPrint  time.Time
)

func main() {
	flag.Parse()
	log.Println("start blobperf")

	queue := make(chan int)

	for i := 0; i < *workers; i++ {
		go uploader(queue)
	}

	lastPrint = time.Now()
	for {
		queue <- 1
	}
}

func count() {
	counter++

	if time.Since(lastPrint) > time.Second {
		fmt.Printf("uploads per second %d\n", counter)
		lastPrint = time.Now()
		counter = 0
	}
}

func uploader(ch chan int) {
	for {
		<-ch
		v := make([]byte, 33000)

		filename := md5Hash(v)
		req, err := multipartRequest("/blob/upload/", filename, v)

		if err != nil {
			log.Error("err creating request %v", err)
			return
		}

		res, err := doReq(req)

		if err != nil {
			log.Errorf("err sending request %v", err)
		}

		ur := new(protocol.UploadResponse)
		parseResponse(res, ur)

		if res.StatusCode != 201 {
			log.Errorf("Unexpected status code %d", res.StatusCode)
		}
		count()
	}
}

func multipartRequest(path, name string, contents []byte) (req *http.Request, err error) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	part, err := w.CreateFormFile("file", name)

	if err != nil {
		w.Close()
		return
	}

	f := bytes.NewBuffer(contents)

	if _, err = io.Copy(part, f); err != nil {
		w.Close()
		return
	}

	w.Close()

	url := absURL(path)
	req, err = http.NewRequest("POST", url, &b)

	if err != nil {
		return
	}

	// content type, contains the boundary.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return
}

func doReq(req *http.Request) (res *http.Response, err error) {
	client := &http.Client{}
	return client.Do(req)
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

func toURL(args map[string]string) (values url.Values) {
	values = url.Values{}

	if args != nil {
		for k, v := range args {
			values.Add(k, v)
		}
	}

	return
}

func absURL(endpoint string) string {
	return fmt.Sprintf("%s%s", *serverAddr, endpoint)
}

func md5Hash(data []byte) string {
	hasher := md5.New()
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}
