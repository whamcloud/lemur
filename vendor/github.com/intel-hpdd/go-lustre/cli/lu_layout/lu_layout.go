// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

/*
#cgo LDFLAGS: -llustreapi
#include <stdlib.h>
#include <lustre/lustreapi.h>
#include <lustre/lustreapi.h>
*/
import "C"

import (
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"

	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/llapi"
	"github.com/intel-hpdd/go-lustre/luser"
	"github.com/intel-hpdd/go-lustre/status"
	"golang.org/x/sys/unix"
)

var (
	fileinfo bool
	filename bool
)

func init() {
	flag.BoolVar(&fileinfo, "i", false, " print file info")
	flag.BoolVar(&filename, "f", false, "always print file name")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <path>...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

// This is currently a testbed for various methods of fetching
// metadata from lustre.

func printLayout(layout *llapi.DataLayout) {
	fmt.Printf("lmm_stripe_count:   %d\n", layout.StripeCount)
	fmt.Printf("lmm_stripe_size:    %d\n", layout.StripeSize)
	fmt.Printf("lmm_pattern:        0x%x\n", layout.StripePattern)
	fmt.Printf("lmm_layout_gen:     %d\n", layout.Generation)
	fmt.Printf("lmm_stripe_offset:  %d\n", layout.StripeOffset)
	if len(layout.Objects) > 0 {
		fmt.Printf("obdidx objid  objid group\n")
		for _, o := range layout.Objects {
			fmt.Printf("%12d %12d   0x%x    0x%x\n", o.Index, o.Object.Oid, o.Object.Oid, o.Object.Seq)
		}
	}
}

func printDirLayout(layout *llapi.DataLayout) {
	fmt.Printf("lmm_stripe_count:   %d\n", layout.StripeCount)
	fmt.Printf("lmm_stripe_size:    %d\n", layout.StripeSize)
	fmt.Printf("lmm_pattern:        0x%x\n", layout.StripePattern)
	fmt.Printf("lmm_stripe_offset:  %d\n", layout.StripeOffset)
}

func main() {
	flag.Parse()

	for _, name := range flag.Args() {
		fi, err := os.Stat(name)
		if err != nil {
			log.Println(err)
			continue
		}

		if fi.Mode().IsDir() {
			layout, err := llapi.DirDataLayout(name)
			if err != nil {
				errno, ok := err.(*os.SyscallError)
				if !ok || errno.Err != syscall.Errno(unix.ENODATA) {
					log.Printf("Unable to fetch directory layout: %v ", err)
				}

			} else {
				fmt.Println("\nDirectory layout:")
				printDirLayout(layout)
			}
		} else {
			// Fetch directly from EA
			layoutEA, err := llapi.FileDataLayoutEA(name)
			if err != nil {
				log.Printf("Unable to open EA: %v", err)
			} else {
				fmt.Println("\nDirectly from lustre.lov EA")
				printLayout(layoutEA)
			}

			// using IOC_MDC_GETSTRIPE (like lfs does)
			layout, err := llapi.FileDataLayout(name)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("\nUsing IOC_MDC_GETFILESTRIPE via llapi_file_get_stripe")
			printLayout(layout)
		}

		root, err := fs.MountRoot(name)
		if err != nil {
			log.Fatal(err)
		}
		fid, err := luser.GetFid(name)
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}

		// Get MDT index using llapi
		idx, err := status.GetMdt(root, fid)
		if err != nil {
			log.Fatal(err)
		}

		//		fmt.Println("\nMDT index using llapi")
		fmt.Printf("mdt index: %d\n", idx)

		/*		f, _ := root.Open()
				defer f.Close()

				idx2, err := llapi.GetMdtIndex(f, fid)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("\nMDT index using ioctl")
				fmt.Printf("mdt index: %d\n", idx2)
		*/
	}

}
