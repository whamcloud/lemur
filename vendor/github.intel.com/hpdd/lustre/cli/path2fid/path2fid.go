// path2fid displays the fids for provided files
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.intel.com/hpdd/lustre/luser"
)

var (
	fileinfo bool
	filename bool
)

func fiString(fi os.FileInfo) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Size: %d\n", fi.Size()))
	buffer.WriteString(fmt.Sprintf("Mode: %s\n", fi.Mode()))
	buffer.WriteString(fmt.Sprintf("Mtime: %s\n", fi.ModTime()))

	return buffer.String()
}

func init() {
	flag.BoolVar(&fileinfo, "i", false, " print file info")
	flag.BoolVar(&filename, "f", false, "always print file name")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <path>...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	for _, name := range flag.Args() {
		fid, err := luser.GetFid(name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			continue
		}

		if flag.NArg() > 1 || filename {
			fmt.Printf("%s: ", name)
		}

		fmt.Println(fid)

		if fileinfo {
			fi, err := os.Lstat(name)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Path: %s\n", name)
			fmt.Println(fiString(fi))
		}
	}
}
