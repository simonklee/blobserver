package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/simonz05/util/log"
)

var (
	cdnAddr   = flag.String("cdn", "", "cdn addr")
	sqlDSN    = flag.String("sql", "root:@tcp(localhost:3306)/testing?utf8&parseTime=True", "MySQL Data Source Name")
	workers   = flag.Int("workers", 32, "worker count")
	outfile   = flag.String("out", "", "out file")
	lastPrint time.Time
	counter   *Counter
	errors    *ErrorCollector
)

func init() {
	counter = NewCounter()
	errors = NewErrorCollector()
}

type Blob struct {
	BlobPath string
	Created  time.Time
}

func (b *Blob) URL() string {
	return absURL(b.BlobPath)
}

type Counter struct {
	lastPrint time.Time
	cnt       int
	tot       int
	incrCh    chan bool
	done      chan bool
}

func NewCounter() *Counter {
	c := &Counter{
		lastPrint: time.Now(),
		incrCh:    make(chan bool, 1024),
	}

	go c.listen()
	return c
}

func (c *Counter) incr() {
	c.incrCh <- true
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

func (c *Counter) total() int {
	return c.tot
}

type Error struct {
	code int
	blob *Blob
}

type ErrorCollector struct {
	tot    int
	ch     chan *Error
	errors []*Error
}

func NewErrorCollector() *ErrorCollector {
	c := &ErrorCollector{
		ch: make(chan *Error),
	}

	go c.listen()
	return c
}

func (c *ErrorCollector) log(blob *Blob, code int) {
	c.ch <- &Error{blob: blob, code: code}
}

func (c *ErrorCollector) listen() {
	for err := range c.ch {
		c.tot++
		c.errors = append(c.errors, err)
	}
}

func (c *ErrorCollector) String() string {
	out := fmt.Sprintf(`
	total: %d
`, c.tot)

	for _, err := range c.errors {
		out += fmt.Sprintf("%s %s\n", err.blob.BlobPath, err.blob.URL())
	}

	return out
}

func statter(queue chan *Blob, wg *sync.WaitGroup) {
	for blob := range queue {
		resp, err := http.Head(blob.URL())

		if err != nil {
			log.Errorf("err creating request %v", err)
		}

		if resp.StatusCode != 200 {
			//log.Errorf("Unexpected status code %d", resp.StatusCode)
			errors.log(blob, resp.StatusCode)
		}

		counter.incr()
		wg.Done()
	}
}

func fetcher(queue chan *Blob, wg *sync.WaitGroup) error {
	db, err := sql.Open("mysql", *sqlDSN)

	if err != nil {
		return err
	}

	sqlxDb := sqlx.NewDb(db, "mysql")
	defer db.Close()

	stmt := "SELECT BlobPath, Created FROM AvatarRevision"
	stmt += " WHERE BlobPath != ''"
	stmt += " LIMIT ?, ?"
	err = fetchBlobs(sqlxDb, queue, stmt, wg)

	if err != nil {
		return err
	}

	stmt = "SELECT BlobPath, Created FROM Avatar"
	stmt += " WHERE BlobPath != ''"
	stmt += " LIMIT ?, ?"
	err = fetchBlobs(sqlxDb, queue, stmt, wg)

	if err != nil {
		return err
	}

	stmt = "SELECT BlobPath, Created FROM Planet"
	stmt += " WHERE BlobPath != ''"
	stmt += " LIMIT ?, ?"
	err = fetchBlobs(sqlxDb, queue, stmt, wg)

	if err != nil {
		return err
	}

	stmt = "SELECT BlobPath, Created FROM Item"
	stmt += " WHERE BlobPath != ''"
	stmt += " LIMIT ?, ?"
	err = fetchBlobs(sqlxDb, queue, stmt, wg)

	if err != nil {
		return err
	}

	stmt = "SELECT BlobPath, PublishedTime FROM PublishedPlanet"
	stmt += " WHERE BlobPath != ''"
	stmt += " LIMIT ?, ?"
	err = fetchBlobs(sqlxDb, queue, stmt, wg)

	if err != nil {
		return err
	}

	return nil
}

func fetchBlobs(conn sqlx.Queryer, queue chan *Blob, query string, wg *sync.WaitGroup) error {
	iterSize := 1024
	offset := 0
	limit := iterSize
	tot := 0

	for {
		rows, err := conn.Queryx(query, offset, limit)

		if err != nil {
			return err
		}
		defer rows.Close()

		// read
		n := 0

		for rows.Next() {
			n++
			var blobPath string
			var created time.Time
			err = rows.Scan(&blobPath, &created)

			if err != nil {
				return err
			}

			queue <- &Blob{BlobPath: blobPath, Created: created}
			wg.Add(1)
		}

		if err := rows.Err(); err != nil {
			return err
		}

		rows.Close()
		tot += n

		if n == 0 || tot >= 6000 {
			break
		}

		offset = limit
		limit += iterSize
	}

	return nil
}

func absURL(endpoint string) string {
	return fmt.Sprintf("%s%s", *cdnAddr, endpoint)
}

func main() {
	flag.Parse()
	log.Println("start verifier")
	runtime.GOMAXPROCS(runtime.NumCPU())

	if *cdnAddr == "" {
		log.Fatal("expected cdn addr")
		os.Exit(1)
	}

	var out io.Writer

	if *outfile != "" {
		fp, err := os.OpenFile(*outfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			os.Exit(1)
		}
		defer fp.Close()
		out = fp
	} else {
		out = os.Stdout
	}

	wg := new(sync.WaitGroup)
	queue := make(chan *Blob, 4094)

	for i := 0; i < *workers; i++ {
		go statter(queue, wg)
	}

	lastPrint = time.Now()
	err := fetcher(queue, wg)

	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	wg.Wait()
	close(queue)
	fmt.Fprint(out, errors)
}
