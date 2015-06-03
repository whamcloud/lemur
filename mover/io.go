package main

import "io"

// CopyAt copies n bytes at offset of src file to same offset at destination.
//
// Like ReadAt, CopyAt returns non-nil error when writen < n, and at end of file
// that error is io.EOF.
// Implementation inspired by io.Copy()
func CopyAt(dst io.WriterAt, src io.ReaderAt, offset int64, n int) (written int, err error) {
	bufSize := 32 * 1024
	if bufSize > n {
		bufSize = n
	}
	buf := make([]byte, bufSize)
	for written < n {
		nr, er := src.ReadAt(buf, offset)
		if nr > 0 {
			nw, ew := dst.WriteAt(buf[0:nr], offset)
			if nw > 0 {
				written += nw
				offset += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
		}
		if er == io.EOF && written == n {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	return
}
