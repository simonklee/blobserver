package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/simonz05/util/log"
)

var (
	cdnAddr   = flag.String("cdn", "", "cdn addr")
	sqlDSN    = flag.String("sql", "root:@tcp(localhost:3306)/testing?utf8&parseTime=True", "MySQL Data Source Name")
	workers   = flag.Int("workers", 32, "worker count")
	counter   int
	lastPrint time.Time
)

func main() {
	flag.Parse()
	log.Println("start verifier")

	inch := make(chan string)
	errch := make(chan error)

	for i := 0; i < *workers; i++ {
		go statter(errch, inch)
	}

	lastPrint = time.Now()
	err := fetcher(inch)

	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	errors := make([]error, 0)
	for {
		select {
		case <-time.After(time.Second * 10):
			goto done
		case err := <-errch:
			errors = append(errors, err)
		}
	}
done:
	log.Println(len(errors))
	for _, e := range errors {
		fmt.Printf("%s%s\n", *cdnAddr, e)
	}
	close(inch)
	close(errch)
}

func count() {
	counter++

	if time.Since(lastPrint) > time.Second {
		fmt.Printf("stat per second %d\n", counter)
		lastPrint = time.Now()
		counter = 0
	}
}

func statter(errch chan error, in chan string) {
	for blob := range in {
		resp, err := http.Head(absURL(blob))

		if err != nil {
			log.Errorf("err creating request %v", err)
		}

		if resp.StatusCode == 404 {
			errch <- fmt.Errorf(blob)
		}

		count()
	}
}

func absURL(endpoint string) string {
	return fmt.Sprintf("%s%s", *cdnAddr, endpoint)
}

func fetcher(queue chan string) error {
	db, err := sql.Open("mysql", *sqlDSN)

	if err != nil {
		return err
	}

	sqlxDb := sqlx.NewDb(db, "mysql")
	defer db.Close()

	stmt := "SELECT BlobPath FROM Avatar"
	stmt += " WHERE NOT BlobPath IS NULL"
	stmt += " LIMIT ?, ?"
	err = fetchBlobs(sqlxDb, queue, stmt)

	if err != nil {
		return err
	}

	return nil
}

func fetchBlobs(conn sqlx.Queryer, queue chan string, query string) error {
	iterSize := 1024
	offset := 0
	limit := iterSize

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
			err = rows.Scan(&blobPath)

			if err != nil {
				return err
			}
			queue <- blobPath
		}

		if err := rows.Err(); err != nil {
			return err
		}

		rows.Close()

		if n == 0 {
			break
		}

		offset = limit
		limit += iterSize
	}

	log.Println("done")
	return nil
}
