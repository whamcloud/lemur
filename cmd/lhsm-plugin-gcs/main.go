// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"

	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/pkg/fsroot"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/audit"
	"github.com/intel-hpdd/logging/debug"
)

type (
	archiveConfig struct {
		Name   string `hcl:",key"`
		ID     int
		Bucket string
		Prefix string

		Credentials *google.Credentials
	}

	// ArchiveSet is a list of mover configs.
	archiveSet []*archiveConfig

	gcsConfig struct {
		NumThreads        int        `hcl:"num_threads"`
		ServiceAccountKey string     `hcl:"service_account_key"`
		Archives          archiveSet `hcl:"archive"`
	}
)

// Should this be configurable?
const updateInterval = 10 * time.Second

var rate metrics.Meter

func (c *gcsConfig) String() string {
	return dmplugin.DisplayConfig(c)
}

func (a *archiveConfig) String() string {
	return fmt.Sprintf("%d:%s:%s", a.ID, a.Bucket, a.Prefix)
}

func init() {
	rate = metrics.NewMeter()

	// if debug.Enabled() {
	go func() {
		var lastCount int64
		for {
			if lastCount != rate.Count() {
				audit.Logf("total %s (1 min/5 min/15 min/inst): %s/%s/%s/%s msg/sec\n",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				lastCount = rate.Count()
			}
			time.Sleep(10 * time.Second)
		}
	}()
	// }
}

// CheckValid determines if the archive configuration is a valid one.
func (a *archiveConfig) checkValid() error {
	var errs []string

	if a.Bucket == "" {
		errs = append(errs, fmt.Sprintf("Archive %s: archive bucket not set", a.Name))
	}

	if a.ID < 1 {
		errs = append(errs, fmt.Sprintf("Archive %s: archive id not set", a.Name))
	}

	if len(errs) > 0 {
		return errors.Errorf("Errors: %s", strings.Join(errs, ", "))
	}

	return nil
}

func (a *archiveConfig) checkGCSAccess() error {

	ctx := context.Background()
	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithCredentials(a.Credentials))
	if err != nil {
		return errors.Wrap(err, "Failed to create client")
	}

	b := client.Bucket(a.Bucket)

	it := b.Objects(ctx, nil)
	if _, err := it.Next(); err != iterator.Done && err != nil {
		return errors.Wrap(err, "Unable to list GCS bucket objects")
	}

	return nil
}

func (a *archiveConfig) createCredentials(ctx context.Context, cfg *gcsConfig) error {

	var errs error

	if cfg.ServiceAccountKey != "" {
		data, err := ioutil.ReadFile(cfg.ServiceAccountKey)
		if err != nil {
			return errors.Wrap(err, "Unable to read the service account key file")
		}

		a.Credentials, errs = google.CredentialsFromJSON(ctx, data, "https://www.googleapis.com/auth/devstorage.read_write")
		if errs != nil {
			return errors.Wrap(errs, "Failed to get credentials from service account key file")
		}

	} else {
		a.Credentials, errs = google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
		if errs != nil {
			return errors.Wrap(errs, "No default credentials found")
		}

	}

	return nil
}

func getMergedConfig(plugin *dmplugin.Plugin) (*gcsConfig, error) {
	var cfg *gcsConfig
	cfg = new(gcsConfig)

	err := dmplugin.LoadConfig(plugin.ConfigFile(), &cfg)

	if err != nil {
		return nil, fmt.Errorf("Failed to load config: %s", err)
	}

	return cfg, nil
}

func noop() {

	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.New(path)
	})
	if err != nil {
		alert.Abort(errors.Wrap(err, "create plugin failed"))
	}

	cfg, err := getMergedConfig(plugin)
	if err != nil {
		alert.Abort(errors.Wrap(err, "Unable to determine plugin configuration"))
	}

	debug.Printf("GCS Mover configuration:\n%v", cfg)

	if len(cfg.Archives) == 0 {
		alert.Abort(errors.New("Invalid configuration: No archives defined"))
	}

	//Create context
	ctx := context.Background()

	for _, archive := range cfg.Archives {
		debug.Print(archive)
		if err := archive.createCredentials(ctx, cfg); err != nil {
			alert.Abort(errors.Wrap(err, "Unable create credentials"))
		}
		if err := archive.checkValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
		if err := archive.checkGCSAccess(); err != nil {
			alert.Abort(errors.Wrap(err, "GCS access check failed"))
		}
	}

	debug.Printf("GCS configuration:\n%v", cfg)

	// All base filesystem operations will be relative to current directory
	err = os.Chdir(plugin.Base())
	if err != nil {
		alert.Abort(errors.Wrap(err, "chdir failed"))
	}

	interruptHandler(func() {
		plugin.Stop()
	})

	for _, archive := range cfg.Archives {
		// Creates a client.
		client, err := storage.NewClient(ctx, option.WithCredentials(archive.Credentials))
		if err != nil {
			log.Fatalf("Failed to create client: %v", err)
		}

		plugin.AddMover(&dmplugin.Config{
			Mover:      GcsMover(archive, ctx, client),
			NumThreads: cfg.NumThreads,
			ArchiveID:  uint32(archive.ID),
		})
	}

	plugin.Run()
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

func main() {
	noop()
}
