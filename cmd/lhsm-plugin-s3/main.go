package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"
)

type (
	archiveConfig struct {
		id     uint32
		bucket string
		prefix string
	}

	archiveSet map[uint32]*archiveConfig

	s3Config struct {
		agentAddress string
		enableDebug  bool
		clientRoot   string
		archives     archiveSet
	}
)

var config *s3Config

func (set archiveSet) String() string {
	var buf bytes.Buffer

	for _, a := range set {
		fmt.Fprintf(&buf, "%d:%s/%s\n", a.id, a.bucket, a.prefix)
	}

	return buf.String()
}

func (set archiveSet) Set(value string) error {
	// id,archiveRoot
	fields := strings.Split(value, ",")
	if len(fields) != 2 {
		return fmt.Errorf("Unable to parse %q", value)
	}

	id, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return fmt.Errorf("Unable to parse %q: %s", fields[0], err)
	}

	u, err := url.Parse(fields[1])
	if err != nil {
		return fmt.Errorf("Unable to parse %q: %s", fields[0], err)
	}
	if u.Scheme != "s3" {
		return fmt.Errorf("Scheme %q not supported", u.Scheme)
	}

	set[uint32(id)] = &archiveConfig{
		id:     uint32(id),
		bucket: u.Host,
		prefix: u.Path,
	}
	return nil
}

func init() {
	config = &s3Config{
		archives: make(archiveSet),
	}

	flag.BoolVar(&config.enableDebug, "debug", false, "Enable debug logging")
	flag.StringVar(&config.agentAddress, "agent", ":4242", "Lustre client mountpoint")
	flag.StringVar(&config.clientRoot, "client", "", "Lustre client mountpoint")
	flag.Var(config.archives, "archive", "Archive definition(s) (id,archiveRoot)")

}

func s3(config *s3Config) {
	c, err := client.New(config.clientRoot)
	if err != nil {
		alert.Fatal(err)
	}

	done := make(chan struct{})
	interruptHandler(func() {
		close(done)
	})

	plugin, err := dmplugin.New(config.agentAddress)
	if err != nil {
		alert.Fatalf("failed to dial: %s", err)
	}
	defer plugin.Close()

	for _, a := range config.archives {
		plugin.AddMover(S3Mover(c, a.id, a.bucket, a.prefix))
	}

	<-done
	plugin.Stop()
}

func main() {
	flag.Parse()

	if config.enableDebug {
		debug.Enable()
	}

	s3(config)
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			debug.Printf("signal received: %s", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()
}
