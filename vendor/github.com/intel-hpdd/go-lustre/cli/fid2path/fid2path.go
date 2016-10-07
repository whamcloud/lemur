// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// fid2path displays the paths for one or more fids.
//  -mnt <mountpoint> Lustre mount point
//  -link <link nbr>: only print the file at the offset
package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/status"
)

var (
	link     int
	mnt      string
	verbose  bool
	absolute bool
)

func init() {
	flag.IntVar(&link, "link", 0, "Specific link to display, -1 for all available.")
	flag.StringVar(&mnt, "mnt", "", "Lustre mount point, defaults to current directory.")
	flag.BoolVar(&verbose, "v", false, "Display fid and path.")
	flag.BoolVar(&absolute, "a", false, "Print absolute paths.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [--link #] <fid>...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if mnt == "" {
		m, err := os.Getwd()
		if err != nil {
			fmt.Fprintln(os.Stderr, "! The -mnt <mntpoint> option was not specified.")
			flag.Usage()
			os.Exit(1)
		}
		mnt = m
	}

	root, err := fs.MountRoot(mnt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	for _, fidStr := range flag.Args() {
		fid, err := lustre.ParseFid(fidStr)
		if err != nil {
			fmt.Println(err)
			continue
		}
		var paths []string
		if link >= 0 {
			// Make sure to only fetch a single path if user requests it
			p, err := status.FidPathname(root, fid, link)
			if err != nil {
				fmt.Println(err)
				continue
			}
			paths = []string{p}
		} else {
			var err error
			paths, err = status.FidPathnames(root, fid)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}

		for _, p := range paths {
			if verbose {
				fmt.Printf("%s ", fid)
			}
			if absolute {
				p = path.Join(root.Path(), p)
			}
			fmt.Println(p)
		}
	}
}
