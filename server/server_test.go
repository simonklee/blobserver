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

	log.Severity = log.LevelError
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
		req, err := multipartRequest("/blob/upload/", filename, v)

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
		blobSizedRefs = append(blobSizedRefs, blob.SizedRef{br1.Ref, br1.Size})
	}

	t.Logf("test remove")

	for i, v := range blobRefs {
		url := absURL(fmt.Sprintf("/blob/remove/%s/", v), nil)
		req, err := http.NewRequest("DELETE", url, nil)

		if err != nil {
			return
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

func multipartRequest(path, name, contents string) (req *http.Request, err error) {
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

func toURL(args map[string]string) (values url.Values) {
	values = url.Values{}

	if args != nil {
		for k, v := range args {
			values.Add(k, v)
		}
	}

	return
}

func absURL(endpoint string, args map[string]string) string {
	values := toURL(args)
	var params string

	if len(values) > 0 {
		params = fmt.Sprintf("?%s", values.Encode())
	}

	return fmt.Sprintf("http://%s/v1/api/blobserver%s%s", serverAddr, endpoint, params)
}
