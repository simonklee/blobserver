package server

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/blobserver/storagetest"
	"github.com/simonz05/util/assert"
	"github.com/simonz05/util/log"
)

var (
	once       sync.Once
	serverAddr string
	server     *httptest.Server
)

func startServer() {
	sto := storagetest.NewFakeStorage()
	err := setupServer(sto)

	if err != nil {
		panic(err)
	}

	if testing.Verbose() {
		log.Severity = log.LevelInfo
	} else {
		log.Severity = log.LevelError
	}
	server = httptest.NewServer(nil)
	serverAddr = server.Listener.Addr().String()
}

func md5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func TestServer(t *testing.T) {
	once.Do(startServer)
	ast := assert.NewAssertWithName(t, "TestServer")

	contents := []string{"foo", "quux", "asdf", "qwerty", "0123456789"}
	var blobRefs []blob.Ref
	var blobSizedRefs []blob.SizedRef

	t.Logf("test upload")

	for i, v := range contents {
		filename := md5Hash(v)
		req, err := uploadRequest("/blob/upload/", filename, v)

		if err != nil {
			t.Fatalf("err creating request #%d - %v", i, err)
		}

		res, err := doReq(req)

		if err != nil {
			t.Fatalf("err sending request #%d - %v", i, err)
		}

		ur := new(protocol.UploadResponse)
		parseResponse(t, res, ur)
		ast.Equal(201, res.StatusCode)
		ast.Equal(1, len(ur.Received))
		br1 := ur.Received[0]

		ast.Equal(len(v), br1.Size)
		ast.True(len(br1.Ref.String()) > 0)

		blobRefs = append(blobRefs, br1.Ref)
		blobSizedRefs = append(blobSizedRefs, blob.SizedRef{Ref: br1.Ref, Size: br1.Size})
	}

	t.Logf("test stat")
	statArgs := url.Values{}

	for _, v := range blobRefs {
		statArgs.Add("blob", v.String())
	}

	uri := absURL("/blob/stat/", statArgs)
	req, err := http.NewRequest("GET", uri, nil)
	ast.Nil(err)

	res, err := doReq(req)

	if err != nil {
		t.Fatalf("err sending stat request %v", err)
	}

	ast.Equal(200, res.StatusCode)
	ur := new(protocol.StatResponse)
	parseResponse(t, res, ur)
	ast.Equal(len(blobRefs), len(ur.Stat))

	for i, v := range blobSizedRefs {
		url := absURL(fmt.Sprintf("/blob/stat/%s/", v.Path), nil)
		req, err := http.NewRequest("GET", url, nil)
		ast.Nil(err)
		res, err := doReq(req)

		if err != nil {
			t.Fatalf("err sending request #%d - %v", i, err)
		}

		ast.Equal(200, res.StatusCode)
		ur := new(protocol.StatResponse)
		parseResponse(t, res, ur)
		ast.Equal(1, len(ur.Stat))
		got := ur.Stat[0]
		ast.Equal(v.Path, got.Path)
		ast.Equal(v.Size, got.Size)
	}

	t.Logf("test remove")

	for i, v := range blobRefs {
		url := absURL(fmt.Sprintf("/blob/remove/%s/", v), nil)
		req, err := http.NewRequest("DELETE", url, nil)

		if err != nil {
			t.Fatal(err)
		}

		res, err := doReq(req)

		if err != nil {
			t.Fatalf("err sending request #%d - %v", i, err)
		}

		ur := new(protocol.RemoveResponse)
		parseResponse(t, res, ur)
		ast.Equal(200, res.StatusCode)
	}
}

func uploadRequest(path, name, contents string) (req *http.Request, err error) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	part, err := w.CreateFormFile("file", name)

	if err != nil {
		w.Close()
		return
	}

	f := bytes.NewBufferString(contents)

	if _, err = io.Copy(part, f); err != nil {
		w.Close()
		return
	}

	w.Close()
	// for key, val := range params {
	// 	_ = writer.WriteField(key, val)
	// }

	//if fw, err = w.CreateFormFile("key", name); err != nil {
	//    return
	//}
	//if _, err = fw.Write([]byte("KEY")); err != nil {
	//    return
	//}

	url := absURL(path, nil)
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
	res, err = client.Do(req)

	if err != nil {
		return
	}

	//if res.StatusCode != http.StatusOK {
	//    return fmt.Errorf("bad status: %s", res.Status)
	//}
	return
}

func parseResponse(t *testing.T, res *http.Response, v interface{}) {
	defer res.Body.Close()

	if c := res.Header.Get("Content-Type"); !strings.Contains(c, "application/json") {
		t.Fatalf("Unexpected Content-Type, got %s", c)
	}

	reader := bufio.NewReader(res.Body)
	buf, _ := ioutil.ReadAll(reader)
	err := json.Unmarshal(buf, v)
	//fmt.Printf("%s\n", buf)
	//err := json.NewDecoder(res.Body).Decode(v)

	if err != nil {
		t.Fatal(err)
	}
}

func absURL(endpoint string, args url.Values) string {
	var params string

	if args != nil && len(args) > 0 {
		params = fmt.Sprintf("?%s", args.Encode())
	}

	return fmt.Sprintf("http://%s/v1/api/blobserver%s%s", serverAddr, endpoint, params)
}
