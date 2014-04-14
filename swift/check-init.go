package swift

import (
	"fmt"
	"runtime"

	"github.com/simonz05/util/log"
)

func createCont(ech chan error, in, out chan string, sto *swiftStorage) {
	for cont := range in {
		err := sto.createContainer(cont)
		if err != nil {
			ech <- err
			return
		}

		out <- cont
	}
}

func statCont(ech chan error, in, out chan string, sto *swiftStorage) {
	for cont := range in {
		_, headers, err := sto.conn.Container(cont)
		if err != nil {
			ech <- err
			return
		}
		r := headers["X-Container-Read"]
		exp := sto.containerReadACL

		if r != exp {
			ech <- fmt.Errorf("exp %s, got %s", exp, r)
			return
		}
		out <- cont
	}
}

// checkInit will create all containers for shards
func (s *swiftStorage) checkInit() error {
	if !s.shard {
		return false
	}

	w := runtime.NumCPU() << 3

	cch := make(chan string)
	sch := make(chan string)
	ech := make(chan error)
	qch := make(chan string, 32)

	defer close(ech)
	defer close(cch)
	defer close(sch)
	defer close(qch)

	log.Printf("swift: check init containers. worker cnt %d\n", w)

	for i := 0; i < w; i++ {
		go createCont(ech, cch, sch, s)
		go statCont(ech, sch, qch, s)
	}

	for _, shard := range shards {
		go func(ch chan string, shard string) {
			ch <- fmt.Sprintf("%s-%s", s.containerName, shard)
		}(cch, shard)
	}

	for i := len(shards); i > 0; i-- {
		select {
		case <-qch:
		case err := <-ech:
			return err
		}
	}

	return nil
}
