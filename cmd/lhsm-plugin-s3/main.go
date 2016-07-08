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
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"

	"github.intel.com/hpdd/lemur/dmplugin"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
)

type (
	archiveConfig struct {
		Name   string `hcl:",key"`
		ID     int
		Region string
		Bucket string
		Prefix string
	}

	archiveSet []*archiveConfig

	s3Config struct {
		NumThreads         int        `hcl:"num_threads"`
		Region             string     `hcl:"region"`
		Endpoint           string     `hcl:"endpoint"`
		AWSAccessKeyID     string     `hcl:"aws_access_key_id"`
		AWSSecretAccessKey string     `hcl:"aws_secret_access_key"`
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
	return fmt.Sprintf("%d:%s:%s/%s", a.ID, a.Region, a.Bucket, a.Prefix)
}

func (a *archiveConfig) checkValid() error {
	var errors []string

	if a.Bucket == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: bucket not set", a.Name))
	}

	if a.ID < 1 {
		errors = append(errors, fmt.Sprintf("Archive %s: archive id not set", a.Name))

	}

	if len(errors) > 0 {
		return fmt.Errorf("Errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

func (c *s3Config) Merge(other *s3Config) *s3Config {
	result := new(s3Config)

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

func s3Svc(region string, endpoint string) *s3.S3 {
	// TODO: Allow more per-archive configuration options?
	cfg := aws.NewConfig().WithRegion(region)
	if endpoint != "" {
		cfg.WithEndpoint(endpoint)
		cfg.WithS3ForcePathStyle(true)
	}
	return s3.New(session.New(cfg))
}

func getMergedConfig(plugin dmplugin.Plugin) (*s3Config, error) {
	baseCfg := &s3Config{
		Region: "us-east-1",
	}

	var cfg s3Config
	err := dmplugin.LoadConfig(plugin.ConfigFile(), &cfg)

	if err != nil {
		return nil, fmt.Errorf("Failed to load config: %s", err)
	}

	return baseCfg.Merge(&cfg), nil
}

func checkS3Configuration(cfg *s3Config) error {
	// Check to make sure that the SDK can find some credentials
	// before continuing; otherwise there will be long timeouts and
	// mysterious failures on HSM actions.
	// Hopefully this will work for non-AWS S3 implementations as well.
	s3Creds := defaults.CredChain(aws.NewConfig().WithRegion(cfg.Region), defaults.Handlers())
	if _, err := s3Creds.Get(); err != nil {
		return errors.Wrap(err, "No S3 credentials found; cannot initialize data mover")
	}

	svc := s3Svc(cfg.Region, cfg.Endpoint)
	if _, err := svc.ListBuckets(&s3.ListBucketsInput{}); err != nil {
		return errors.Wrap(err, "Unable to list S3 buckets")
	}

	return nil
}

func main() {
	plugin, err := dmplugin.New(path.Base(os.Args[0]))
	if err != nil {
		alert.Abort(errors.Wrap(err, "failed to initialize plugin"))
	}
	defer plugin.Close()

	cfg, err := getMergedConfig(plugin)
	if err != nil {
		alert.Abort(errors.Wrap(err, "Unable to determine plugin configuration"))
	}

	debug.Printf("S3Mover configuration:\n%v", cfg)

	if len(cfg.Archives) == 0 {
		alert.Abort(errors.New("Invalid configuration: No archives defined"))
	}

	for _, archive := range cfg.Archives {
		debug.Print(archive)
		if err := archive.checkValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
	}

	// Set the configured AWS credentials in the environment for use
	// by the SDK. If there are no explicitly configured credentials,
	// then the SDK will look for them in other ways
	// (e.g. ~/.aws/credentials, ec2 role, etc.).
	if cfg.AWSAccessKeyID != "" {
		os.Setenv("AWS_ACCESS_KEY_ID", cfg.AWSAccessKeyID)
	}
	if cfg.AWSSecretAccessKey != "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.AWSSecretAccessKey)
	}

	if err := checkS3Configuration(cfg); err != nil {
		alert.Abort(errors.Wrap(err, "S3 config check failed"))
	}

	// All base filesystem operations will be relative to current directory
	err = os.Chdir(plugin.Base())
	if err != nil {
		alert.Abort(errors.Wrap(err, "chdir failed"))
	}

	interruptHandler(func() {
		plugin.Stop()
	})

	for _, a := range cfg.Archives {
		// Allow each archive to set its own region, but default
		// to the region set for the plugin.
		region := cfg.Region
		if a.Region != "" {
			region = a.Region
		}
		s3Svc := s3Svc(region, cfg.Endpoint)
		plugin.AddMover(&dmplugin.Config{
			Mover:      S3Mover(s3Svc, uint32(a.ID), a.Bucket, a.Prefix),
			NumThreads: cfg.NumThreads,
			ArchiveID:  uint32(a.ID),
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
