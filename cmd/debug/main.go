package main

import (
	"os"
	"bufio"
	"runtime"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/simonz05/util/log"
)

var (
	serverAddr = flag.String("addr", "http://localhost:6064/v1/api/blobserver", "server addr")
	workers    = flag.Int("workers", 10, "worker count")
	counter *Counter
	errors *ErrorCollector
)

func init() {
	counter = NewCounter()
	errors = NewErrorCollector()
}

type Counter struct {
	lastPrint  time.Time
	cnt    int
	tot    int
	incrCh chan bool
}

func NewCounter() *Counter {
	c := &Counter{
		lastPrint: time.Now(),
		incrCh: make(chan bool, 1024),
	}

	go c.listen()
	return c
}

func (c *Counter) incr() {
	c.incrCh<-true
}

func (c *Counter) listen() {
	for _ = range c.incrCh {
		c.cnt++
		c.tot++

		if time.Since(c.lastPrint) > time.Second {
			fmt.Printf("tot %d reqs per second %d\n", c.tot, c.cnt)
			c.lastPrint = time.Now()
			c.cnt = 0
		}
	}
}

type Error struct {
	code    int
	url string
}

type ErrorCollector struct {
	tot    int
	ch chan *Error
	errors []*Error
}

func NewErrorCollector() *ErrorCollector {
	c := &ErrorCollector{
		ch: make(chan *Error),
	}

	go c.listen()
	return c
}

func (c *ErrorCollector) log(url string, code int) {
	c.ch <- &Error{url: url, code: code}
}

func (c *ErrorCollector) listen() {
	for err := range c.ch {
		c.tot++
		c.errors = append(c.errors, err)
	}
}

func (c *ErrorCollector) String() string{
	out := fmt.Sprintf(`
	total: %d
`, c.tot)

	for i, err := range c.errors {
		out += fmt.Sprintf("%d: %s %d\n", i, err.url, err.code)
	}

	return out
}

func getCDNURL() string {
	res, err := http.Get(absURL("/config/"))
	if err != nil {
		log.Fatal(err)
	}
	if res.StatusCode != 200 {
		log.Fatalf("Unexpected status code %d", res.StatusCode)
	}
	m := make(map[string]interface{})
	parseResponse(res, &m)
	return m["Data"].(map[string]interface{})["CDNUrl"].(string)
}

func fetcher(ch chan string, closed chan bool) {
	for {
		select {
			case url := <- ch: 
			res, err := http.Head(url)

			if err != nil {
				log.Errorf("err request url %s %v", url, err)
				continue
			}

			if res.StatusCode != 200 {
				log.Errorf("Unexpected status code %d", res.StatusCode)
				errors.log(url, res.StatusCode)
			}
			counter.incr()
		case <-time.After(time.Second):
			closed<-true
			return
		}
	}
	closed<-true
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
	// fmt.Printf("%s\n", buf)
	//err := json.NewDecoder(res.Body).Decode(v)

	if err != nil {
		log.Error(err)
	}
}

func absURL(endpoint string) string {
	return fmt.Sprintf("%s%s", *serverAddr, endpoint)
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Println("start blobdebug")

	cdnURL := getCDNURL()
	queue := make(chan string, 1024)
	quit := make(chan bool)

	for i := 0; i < *workers; i++ {
		go fetcher(queue, quit)
	}

	bio := bufio.NewReader(os.Stdin)

	for {
		line, _, err := bio.ReadLine(); 

		if err != nil {
			break
		}

		queue<-cdnURL + "/" + string(line)
	}

	for i := 0; i < *workers; i++ {
		<-quit
	}

	fmt.Fprint(os.Stderr, errors)
}
