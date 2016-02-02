package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/rcrowley/go-metrics"

	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"
	"github.intel.com/hpdd/svclog"
)

type (
	archiveConfig struct {
		id          uint32
		archiveRoot string
	}

	archiveSet map[uint32]*archiveConfig

	posixConfig struct {
		agentAddress string
		enableDebug  bool
		clientRoot   string
		archives     archiveSet
	}
)

var (
	rate   metrics.Meter
	config *posixConfig
)

func init() {
	rate = metrics.NewMeter()

	config = &posixConfig{
		agentAddress: "localhost:4242",
		archives:     make(archiveSet),
	}

	flag.BoolVar(&config.enableDebug, "debug", false, "Enable debug logging")
	flag.StringVar(&config.clientRoot, "client", "", "Lustre client mountpoint")
	flag.Var(config.archives, "archive", "Archive definition(s) (id,archiveRoot)")
	/*
		go func() {
			for {
				fmt.Fprintf(os.Stderr, "total %s msg/sec (1 min/5 min/15 min/inst): %s/%s/%s/%s\r",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				time.Sleep(1 * time.Second)
			}
		}()
	*/
}

func (set archiveSet) String() string {
	var buf bytes.Buffer

	for _, a := range set {
		fmt.Fprintf(&buf, "%d:%s\n", a.id, a.archiveRoot)
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

	set[uint32(id)] = &archiveConfig{
		id:          uint32(id),
		archiveRoot: fields[1],
	}

	return nil
}

func posix(config *posixConfig) {
	c, err := client.New(config.clientRoot)
	if err != nil {
		svclog.Fail(err)
	}

	done := make(chan struct{})
	interruptHandler(func() {
		close(done)
	})

	plugin, err := dmplugin.New(config.agentAddress)
	if err != nil {
		svclog.Fail("failed to dial: %s", err)
	}
	defer plugin.Close()

	for _, a := range config.archives {
		plugin.AddMover(PosixMover(c, a.archiveRoot, a.id))
	}

	<-done
	plugin.Stop()
}

func main() {
	flag.Parse()

	if config.enableDebug {
		svclog.EnableDebug()
	}

	posix(config)
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			svclog.Debug("signal received: %s", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()
}
