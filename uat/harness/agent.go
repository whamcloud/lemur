package harness

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent"
	defaults "github.intel.com/hpdd/lemur/cmd/lhsmd/config"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs/spec"
	"github.intel.com/hpdd/lustre/pkg/mntent"
)

const (
	// HsmAgentCfgKey refers to this context's agent config file
	HsmAgentCfgKey = "agent_config_key"

	// HsmAgentBinary is the name of the HSM Agent
	HsmAgentBinary = "lhsmd.race"

	// HsmPluginPrefix is the base name of data mover plugins
	HsmPluginPrefix = "lhsm-plugin-"
)

// AgentDriver allows the harness to drive an HSM agent
type AgentDriver struct {
	ac      *agent.Config
	cmd     *exec.Cmd
	started bool
}

// AgentPid returns the pid of the running agent, if available
func (ad *AgentDriver) AgentPid() (int, error) {
	if ad.cmd == nil {
		return -1, fmt.Errorf("AgentPid() called with nil cmd")
	}

	return ad.cmd.Process.Pid, nil
}

type harnessConfigProvider struct {
	ctx *ScenarioContext
}

func (p *harnessConfigProvider) Retrieve() (credentials.Value, error) {
	if p.ctx.Config.AWSAccessKeyID == "" && p.ctx.Config.AWSSecretAccessKey == "" {
		return credentials.Value{}, fmt.Errorf("No AWS credentials set in harness config")
	}
	return credentials.Value{
		AccessKeyID:     p.ctx.Config.AWSAccessKeyID,
		SecretAccessKey: p.ctx.Config.AWSSecretAccessKey,
		ProviderName:    "HarnessConfig",
	}, nil
}

func (p *harnessConfigProvider) IsExpired() bool {
	return false
}

// ConfigureAgent creates or updates the Context's agent config
func ConfigureAgent(ctx *ScenarioContext) error {
	cd, err := getClientDeviceForMount(ctx.Config.LustrePath)
	if err != nil {
		return errors.Wrapf(err, "Failed to get client device for %s", ctx.Config.LustrePath)
	}

	agentPath, err := exec.LookPath(HsmAgentBinary)
	if err != nil {
		return errors.Wrap(err, "Unable to determine plugin dir based on lhsmd location")
	}

	agentConfig := agent.NewConfig()
	agentConfig.PluginDir = path.Dir(agentPath)
	agentConfig.MountRoot = ctx.Workdir() + "/mnt"
	agentConfig.ClientDevice = cd

	// Maybe this should be an error?
	if ctx.AgentDriver != nil {
		alert.Warn("Updating existing agent driver in context")
	}

	cfgFile := ctx.Workdir() + defaults.DefaultConfigPath
	ctx.SetKey(HsmAgentCfgKey, cfgFile)

	ctx.AgentDriver, err = newAgentDriver(ctx, agentConfig)
	if err != nil {
		return errors.Wrap(err, "Unable to create agent driver")
	}

	return WriteAgentConfig(ctx)
}

// WriteAgentConfig writes the agent configuration into the workdir
func WriteAgentConfig(ctx *ScenarioContext) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.ac == nil {
		return fmt.Errorf("WriteAgentConfig() may only be called after ConfigureAgent()")
	}

	cfgFile, err := ctx.GetKey(HsmAgentCfgKey)
	if err != nil {
		return errors.Wrap(err, "No config file path found")
	}

	cfgDir := path.Dir(cfgFile)
	if err := os.MkdirAll(cfgDir, 0700); err != nil {
		return errors.Wrap(err, "Failed to create agent config dir")
	}
	return ioutil.WriteFile(cfgFile, []byte(ctx.AgentDriver.ac.String()), 0600)
}

// StartAgent starts the configured agent
func StartAgent(ctx *ScenarioContext) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.cmd == nil {
		return fmt.Errorf("StartAgent() may only be called after ConfigureAgent()")
	}

	ctx.AgentDriver.started = true
	return ctx.AgentDriver.cmd.Start()
}

func newAgentCmd(ctx *ScenarioContext) (*exec.Cmd, error) {
	cfgFile, err := ctx.GetKey(HsmAgentCfgKey)
	if err != nil {
		return nil, errors.Wrap(err, "No config file path found")
	}

	stdout, err := os.OpenFile(ctx.Workdir()+"/agent.stdout", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create agent stdout file")
	}
	stderr, err := os.OpenFile(ctx.Workdir()+"/agent.stderr", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create agent stderr file")
	}

	agentArgs := []string{"-config=" + cfgFile}
	if ctx.Config.EnableAgentDebug {
		agentArgs = append(agentArgs, "-debug")
	}
	cmd := exec.Command(HsmAgentBinary, agentArgs...) // #nosec
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	return cmd, nil
}

func newAgentDriver(ctx *ScenarioContext, cfg *agent.Config) (*AgentDriver, error) {
	cmd, err := newAgentCmd(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to create agent cmd")
	}

	driver := &AgentDriver{
		ac:  cfg,
		cmd: cmd,
	}

	return driver, nil
}

// StopAgent stops the running agent
func StopAgent(ctx *ScenarioContext) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.cmd == nil {
		return fmt.Errorf("StopAgent() may only be called after StartAgent()")
	}
	if !ctx.AgentDriver.started {
		return nil
	}

	ad := ctx.AgentDriver
	if ad.cmd.ProcessState != nil && ad.cmd.ProcessState.Exited() {
		return fmt.Errorf("StopAgent() called on stopped agent")
	}

	// Send SIGTERM to allow the agent to clean up
	if err := ad.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrap(err, "sending SIGTERM to agent failed")
	}

	if err := ad.cmd.Wait(); err != nil {
		return errors.Wrapf(err, "agent did not exit cleanly")
	}

	return nil
}

func writePosixMoverConfig(ctx *ScenarioContext, name string) error {
	cfg := fmt.Sprintf(`archive "one" {
	id = 1
	root = "%s"
}`, ctx.Workdir()+"/archives/1")

	cfgFile := ctx.Workdir() + "/etc/lhsmd/" + name
	cfgDir := path.Dir(cfgFile)
	if err := os.MkdirAll(cfgDir, 0700); err != nil {
		return errors.Wrap(err, "Failed to create plugin config dir")
	}

	return ioutil.WriteFile(cfgFile, []byte(cfg), 0600)
}

func writeS3MoverConfig(ctx *ScenarioContext, name string) error {
	var awsKey string
	var awsSecret string

	// Get AWS credentials from the environment or via explicit
	// harness configuration.
	chainProvider := &credentials.ChainProvider{
		Providers: []credentials.Provider{
			&credentials.EnvProvider{},
			&credentials.SharedCredentialsProvider{},
			&harnessConfigProvider{
				ctx: ctx,
			},
		},
	}
	if val, err := chainProvider.Retrieve(); err == nil {
		awsKey = val.AccessKeyID
		awsSecret = val.SecretAccessKey
	}

	if awsKey == "" || awsSecret == "" {
		return fmt.Errorf("Unable to get AWS credentials from environment or harness config")
	}

	// Start with configured defaults
	ctx.S3Bucket = ctx.Config.S3Bucket
	ctx.S3Prefix = ctx.Config.S3Prefix

	if ctx.Config.S3Bucket == "" {
		var err error
		ctx.S3Bucket, err = createS3Bucket(ctx)
		if err != nil {
			return errors.Wrap(err, "Unable to create test bucket")
		}
		debug.Printf("Created S3 bucket: %s", ctx.S3Bucket)
		ctx.AddCleanup(cleanupS3Bucket(ctx))
	} else if ctx.Config.S3Prefix == "" {
		ctx.S3Prefix = path.Base(ctx.Workdir())
		debug.Printf("Using %s for S3 prefix", ctx.S3Prefix)
		ctx.AddCleanup(cleanupS3Prefix(ctx))
	}

	// TODO: Make configuration of credentials optional.
	cfg := fmt.Sprintf(`region = "%s"
endpoint = "%s"
aws_access_key_id = "%s"
aws_secret_access_key = "%s"

archive "one" {
	id = 1
	bucket = "%s"
	prefix = "%s"
}`, ctx.Config.S3Region, ctx.Config.S3Endpoint, awsKey, awsSecret, ctx.S3Bucket, ctx.S3Prefix)

	cfgFile := ctx.Workdir() + "/etc/lhsmd/" + name
	cfgDir := path.Dir(cfgFile)
	if err := os.MkdirAll(cfgDir, 0700); err != nil {
		return errors.Wrap(err, "Failed to create plugin config dir")
	}

	return ioutil.WriteFile(cfgFile, []byte(cfg), 0600)
}

func s3Svc(region string, endpoint string) *s3.S3 {
	// TODO: Allow more per-archive configuration options?
	cfg := aws.NewConfig().WithRegion(region)
	if endpoint != "" {
		cfg.WithEndpoint(endpoint)
		cfg.WithS3ForcePathStyle(true)
	}
	// cfg.WithLogLevel(aws.LogDebug)
	return s3.New(session.New(cfg))
}

func createS3Bucket(ctx *ScenarioContext) (string, error) {
	svc := s3Svc(ctx.Config.S3Region, ctx.Config.S3Endpoint)
	bucket, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(path.Base(ctx.Workdir())),
	})
	if err != nil {
		return "", errors.Wrap(err, "Failed to create test bucket")
	}
	return path.Base(*bucket.Location), nil
}

func cleanupS3Prefix(ctx *ScenarioContext) cleanupFn {
	return func() error {
		debug.Printf("Cleaning up %s/%s...", ctx.S3Bucket, ctx.S3Prefix)
		if err := deleteObjects(ctx); err != nil {
			return errors.Wrap(err, "Failed to delete test prefix objects in cleanup")
		}

		err := deleteObject(ctx, ctx.S3Prefix)
		return errors.Wrap(err, "Failed to delete test prefix in cleanup")
	}

}

func deleteObject(ctx *ScenarioContext, key string) error {
	svc := s3Svc(ctx.Config.S3Region, ctx.Config.S3Endpoint)
	doi := &s3.DeleteObjectInput{
		Bucket: aws.String(ctx.S3Bucket),
		Key:    aws.String(key),
	}
	_, err := svc.DeleteObject(doi)
	return errors.Wrap(err, key)
}

func deleteObjects(ctx *ScenarioContext) error {
	svc := s3Svc(ctx.Config.S3Region, ctx.Config.S3Endpoint)
	loi := &s3.ListObjectsInput{
		Bucket: aws.String(ctx.S3Bucket),
		Prefix: aws.String(ctx.S3Prefix),
	}

	out, err := svc.ListObjects(loi)
	if err != nil {
		return errors.Wrap(err, "Failed to list bucket objects")
	}

	if len(out.Contents) == 0 {
		return nil
	}

	// Not all S3 backends support DeleteObjects, so do this the hard way.
	for _, obj := range out.Contents {
		err = deleteObject(ctx, *obj.Key)
		if err != nil {
			return errors.Wrap(err, "Failed to delete")
		}
	}
	return nil
}

func cleanupS3Bucket(ctx *ScenarioContext) cleanupFn {
	svc := s3Svc(ctx.Config.S3Region, ctx.Config.S3Endpoint)
	dbi := &s3.DeleteBucketInput{
		Bucket: aws.String(ctx.S3Bucket),
	}
	return func() error {
		debug.Printf("Cleaning up %s...", ctx.S3Bucket)

		if err := deleteObjects(ctx); err != nil {
			return errors.Wrap(err, "Failed to delete test bucket objects in cleanup")
		}

		_, err := svc.DeleteBucket(dbi)
		return errors.Wrap(err, "Failed to delete test bucket in cleanup")
	}
}

func writeMoverConfig(ctx *ScenarioContext, name string) error {
	n := strings.Split(name, ".")
	switch n[0] {
	case "lhsm-plugin-posix":
		return writePosixMoverConfig(ctx, name)
	case "lhsm-plugin-s3":
		return writeS3MoverConfig(ctx, name)
	default:
		return fmt.Errorf("Unknown data mover in test: %s", name)
	}
}

// AddConfiguredMover adds a data mover to the agent configuration
func AddConfiguredMover(ctx *ScenarioContext, name string) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.ac == nil {
		return fmt.Errorf("AddConfiguredMover() may only be called after ConfigureAgent()")
	}

	ac := ctx.AgentDriver.ac
	ac.EnabledPlugins = append(ac.EnabledPlugins, name)

	if err := writeMoverConfig(ctx, name); err != nil {
		return errors.Wrap(err, "Failed to write data mover config")
	}

	// Write updated agent config
	return WriteAgentConfig(ctx)
}

func getClientDeviceForMount(mnt string) (*spec.ClientDevice, error) {
	entry, err := mntent.GetEntryByDir(mnt)
	if err != nil {
		return nil, err
	}

	return spec.ClientDeviceFromString(entry.Fsname)
}
