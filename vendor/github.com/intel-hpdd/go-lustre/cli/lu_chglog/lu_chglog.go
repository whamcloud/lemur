// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// View changelogs
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/intel-hpdd/go-lustre/changelog"
	"github.com/intel-hpdd/go-lustre/status"
)

var printTimestamp = false

func init() {
	flag.BoolVar(&printTimestamp, "ts", false, "display record time stamps")
}

type scannerFunc func(h changelog.Handle, next int64) int64

func getAsyncLogger(wg *sync.WaitGroup, logger scannerFunc) scannerFunc {
	wg.Add(1)
	if follow {
		return func(h changelog.Handle, nextIndex int64) int64 {
			defer wg.Done()
			for follow {
				nextIndex = logger(h, nextIndex)
				time.Sleep(1 * time.Second)
			}
			return nextIndex
		}
	}
	return func(h changelog.Handle, nextIndex int64) int64 {
		defer wg.Done()
		return logger(h, nextIndex)
	}
}

var (
	follow    bool
	target    string
	nextIndex int64
	consumer  string
)

func init() {
	flag.BoolVar(&follow, "f", false, "Continue wait for new records.")
	flag.StringVar(&target, "target", "", "Fetch logs from a specific metadata target.")
	flag.Int64Var(&nextIndex, "start", 0, "Record index to start watching log from.")
	flag.StringVar(&consumer, "id", "", "Consumer ID. Will cause logs to be flushed (assumes same consumer on each MDT!!).")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-f] [-id CONSUMER] --target MDT | /lustre/mount\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	var wg sync.WaitGroup
	flag.Parse()

	logger := func(h changelog.Handle, nextIndex int64) int64 {
		err := h.OpenAt(nextIndex, false)
		if err != nil {
			panic("mnt: Failed to open changelog")
		}
		r, err := h.NextRecord()
		for err == nil {
			fmt.Println(target, r.String())
			nextIndex = r.Index() + 1
			r, err = h.NextRecord()
		}
		if consumer != "" && nextIndex > 0 {
			h.Clear(consumer, nextIndex-1)
		}
		h.Close()
		return nextIndex
	}

	/* open changelogs and dump what is there */
	if len(target) > 0 {
		h := changelog.CreateHandle(target)
		go getAsyncLogger(&wg, logger)(h, nextIndex)
	} else {
		if len(flag.Args()) > 0 {
			mnt := flag.Args()[0]
			c, err := status.Client(mnt)
			if err != nil {
				log.Fatal(err)
			}

			for _, mdc := range c.LMVTargets() {
				h := changelog.CreateHandle(mdc)
				go getAsyncLogger(&wg, logger)(h, 0)
			}
		}
	}
	wg.Wait()
}
