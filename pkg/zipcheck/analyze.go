package zipcheck

// Evaluate the "compressibilty" of a file by compressing
// a small sample.

import (
	"compress/zlib"
	"io"
	"math"
	"os"
	"time"

	"github.com/pkg/errors"
)

// CompressResult returns resuls of comressibility check.
type CompressResult struct {
	T        time.Duration
	Samples  int
	Size     int64
	Bytes    int64
	ZipBytes int64
}

// Null is a /dev/null Writer that counts how many bytes have been written to it.
type Null struct {
	Bytes int64
}

func (n *Null) Write(b []byte) (int, error) {
	n.Bytes += int64(len(b))
	return len(b), nil
}

// SampleFile reads count blocks of blockSize from fp, and copies them to w.
func SampleFile(w io.Writer, fp io.ReaderAt, count int, blockSize int64, step int64) (int64, error) {
	var offset int64
	var copied int64
	for i := 0; i < count; i++ {
		r := io.NewSectionReader(fp, offset, blockSize)
		nb, err := io.Copy(w, r)
		if err != nil {
			return copied, errors.Wrap(err, "copy failed")
		}
		copied += nb
		offset += step

	}
	return copied, nil
}

func analyze(fname string, count int, block int64, zipper zipFunc) (*CompressResult, error) {
	var cr CompressResult

	f, err := os.Open(fname)
	if err != nil {
		return nil, errors.Wrap(err, "open failed")
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat failed")
	}
	if count == 0 {
		// default is 2*log(size) smaples for a quick scan
		count = 2 * int(math.Log(float64(fi.Size())))
	}
	null := &Null{}
	w, err := zipper(null)
	if err != nil {
		return nil, errors.Wrap(err, "create compressor failed")
	}

	// Compress entire file it is smaller than the total sample size
	if fi.Size() < int64(count)*block {
		block = fi.Size()
		count = 1
	}

	step := fi.Size() / int64(count)
	started := time.Now()
	cr.Bytes, err = SampleFile(w, f, count, block, step)
	w.Close()
	if err != nil {
		return nil, errors.Wrap(err, "sample failed")
	}

	cr.Samples = count
	cr.Size = block
	cr.T = time.Since(started)
	cr.ZipBytes = null.Bytes
	return &cr, nil
}

type zipFunc func(io.Writer) (io.WriteCloser, error)

func gzip(level int) zipFunc {
	return func(w io.Writer) (io.WriteCloser, error) {
		zip, err := zlib.NewWriterLevel(w, level)
		if err != nil {
			return nil, errors.Wrap(err, "NewWriterLevel")
		}
		return zip, nil
	}
}

// AnalyzeFile will compress a sample of if the file and return estimated reduction percentage.
// 0 means no reduction, 50% means file might be resuduced to half.
func AnalyzeFile(fname string) (float64, error) {
	cr, err := analyze(fname, 0, 4096, gzip(1))
	if err != nil {
		return 0, errors.Wrap(err, "analayze failed")
	}
	reduced := (1 - float64(cr.ZipBytes)/float64(cr.Bytes)) * 100
	return reduced, nil
}
