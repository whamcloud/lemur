// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
		Name               string `hcl:",key"`
		ID                 int
		AWSAccessKeyID     string `hcl:"aws_access_key_id"`
		AWSSecretAccessKey string `hcl:"aws_secret_access_key"`
		Endpoint           string
		Region             string
		Bucket             string
		Prefix             string
		UploadPartSize     int64 `hcl:"upload_part_size"`

		s3Creds *credentials.Credentials
	}

	archiveSet []*archiveConfig

	s3Config struct {
		NumThreads         int        `hcl:"num_threads"`
		AWSAccessKeyID     string     `hcl:"aws_access_key_id"`
		AWSSecretAccessKey string     `hcl:"aws_secret_access_key"`
		Endpoint           string     `hcl:"endpoint"`
		Region             string     `hcl:"region"`
		UploadPartSize     int64      `hcl:"upload_part_size"`
		Archives           archiveSet `hcl:"archive"`
	}
)

// Should this be configurable?
const updateInterval = 10 * time.Second

var rate metrics.Meter

func (c *s3Config) String() string {
	return dmplugin.DisplayConfig(c)
}

func (a *archiveConfig) String() string {
	return fmt.Sprintf("%d:%s:%s:%s/%s", a.ID, a.Endpoint, a.Region, a.Bucket, a.Prefix)
}

func (a *archiveConfig) checkValid() error {
	var errors []string

	if a.Bucket == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: bucket not set", a.Name))
	}

	if a.ID < 1 {
		errors = append(errors, fmt.Sprintf("Archive %s: archive id not set", a.Name))

	}

	if a.UploadPartSize < s3manager.MinUploadPartSize {
		errors = append(errors, fmt.Sprintf("Archive %s: upload_part_size %d is less than minimum (%d)", a.Name, a.UploadPartSize, s3manager.MinUploadPartSize))
	}

	if len(errors) > 0 {
		return fmt.Errorf("Errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

func (a *archiveConfig) checkS3Access() error {
	if _, err := a.s3Creds.Get(); err != nil {
		return errors.Wrap(err, "No S3 credentials found; cannot initialize data mover")
	}

	if _, err := s3Svc(a).ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(a.Bucket),
	}); err != nil {
		return errors.Wrap(err, "Unable to list S3 bucket objects")
	}

	return nil
}

// this does an in-place merge, replacing any unset archive-level value
// with the global value for that setting
func (a *archiveConfig) mergeGlobals(g *s3Config) {
	if a.AWSAccessKeyID == "" {
		a.AWSAccessKeyID = g.AWSAccessKeyID
	}

	if a.AWSSecretAccessKey == "" {
		a.AWSSecretAccessKey = g.AWSSecretAccessKey
	}

	if a.Endpoint == "" {
		a.Endpoint = g.Endpoint
	}

	if a.Region == "" {
		a.Region = g.Region
	}

	if a.UploadPartSize == 0 {
		a.UploadPartSize = g.UploadPartSize
	} else {
		// Allow this to be configured in MiB
		a.UploadPartSize *= 1024 * 1024
	}

	// Set the default credentials provider
	a.s3Creds = defaults.CredChain(
		aws.NewConfig().WithRegion(a.Region), defaults.Handlers(),
	)
	// If these were set on a per-archive basis, override the defaults.
	if a.AWSAccessKeyID != "" && a.AWSSecretAccessKey != "" {
		a.s3Creds = credentials.NewStaticCredentials(
			a.AWSAccessKeyID, a.AWSSecretAccessKey, "",
		)
	}
}

func (c *s3Config) Merge(other *s3Config) *s3Config {
	result := new(s3Config)

	result.UploadPartSize = c.UploadPartSize
	if other.UploadPartSize > 0 {
		result.UploadPartSize = other.UploadPartSize
	}

	result.NumThreads = c.NumThreads
	if other.NumThreads > 0 {
		result.NumThreads = other.NumThreads
	}

	result.Region = c.Region
	if other.Region != "" {
		result.Region = other.Region
	}

	result.Endpoint = c.Endpoint
	if other.Endpoint != "" {
		result.Endpoint = other.Endpoint
	}

	result.AWSAccessKeyID = c.AWSAccessKeyID
	if other.AWSAccessKeyID != "" {
		result.AWSAccessKeyID = other.AWSAccessKeyID
	}

	result.AWSSecretAccessKey = c.AWSSecretAccessKey
	if other.AWSSecretAccessKey != "" {
		result.AWSSecretAccessKey = other.AWSSecretAccessKey
	}

	result.Archives = c.Archives
	if len(other.Archives) > 0 {
		result.Archives = other.Archives
	}

	return result
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

func s3Svc(ac *archiveConfig) *s3.S3 {
	cfg := aws.NewConfig().WithRegion(ac.Region).WithCredentials(ac.s3Creds)
	if debug.Enabled() {
		cfg.WithLogger(debug.Writer())
		cfg.WithLogLevel(aws.LogDebug)
	}
	if ac.Endpoint != "" {
		cfg.WithEndpoint(ac.Endpoint)
		cfg.WithS3ForcePathStyle(true)
	}
	return s3.New(session.New(cfg))
}

func getMergedConfig(plugin *dmplugin.Plugin) (*s3Config, error) {
	baseCfg := &s3Config{
		Region:         "us-east-1",
		UploadPartSize: s3manager.DefaultUploadPartSize,
	}

	var cfg s3Config
	err := dmplugin.LoadConfig(plugin.ConfigFile(), &cfg)

	if err != nil {
		return nil, fmt.Errorf("Failed to load config: %s", err)
	}

	// Allow this to be configured in MiB
	if cfg.UploadPartSize != 0 {
		cfg.UploadPartSize *= 1024 * 1024
	}

	return baseCfg.Merge(&cfg), nil
}

func main() {
	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.New(path)
	})
	if err != nil {
		alert.Abort(errors.Wrap(err, "failed to initialize plugin"))
	}
	defer plugin.Close()

	cfg, err := getMergedConfig(plugin)
	if err != nil {
		alert.Abort(errors.Wrap(err, "Unable to determine plugin configuration"))
	}

	if len(cfg.Archives) == 0 {
		alert.Abort(errors.New("Invalid configuration: No archives defined"))
	}

	for _, ac := range cfg.Archives {
		ac.mergeGlobals(cfg)
		if err = ac.checkValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
		if err = ac.checkS3Access(); err != nil {
			alert.Abort(errors.Wrap(err, "S3 access check failed"))
		}
	}

	debug.Printf("S3Mover configuration:\n%v", cfg)

	// All base filesystem operations will be relative to current directory
	err = os.Chdir(plugin.Base())
	if err != nil {
		alert.Abort(errors.Wrap(err, "chdir failed"))
	}

	interruptHandler(func() {
		plugin.Stop()
	})

	for _, ac := range cfg.Archives {
		plugin.AddMover(&dmplugin.Config{
			Mover:      S3Mover(ac, s3Svc(ac), uint32(ac.ID)),
			NumThreads: cfg.NumThreads,
			ArchiveID:  uint32(ac.ID),
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
