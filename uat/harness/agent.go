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
	"github.com/intel-hpdd/lemur/cmd/lhsmd/agent"
	defaults "github.com/intel-hpdd/lemur/cmd/lhsmd/config"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/debug"
	"github.com/intel-hpdd/go-lustre/fs/spec"
	"github.com/intel-hpdd/go-lustre/pkg/mntent"
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
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return errors.Wrap(err, "Failed to create agent config dir")
	}
	return ioutil.WriteFile(cfgFile, []byte(ctx.AgentDriver.ac.String()), 0644)
}

// StartAgent starts the configured agent
func StartAgent(ctx *ScenarioContext) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.cmd == nil {
		return fmt.Errorf("StartAgent() may only be called after ConfigureAgent()")
	}

	ctx.AgentDriver.started = true

	//after client booting, /var/run/lhsmd folder will disapper, so need to check it

	if !isDirExists("/var/run/lhsmd") {
		err := os.MkdirAll("/var/run/lhsmd", 0700)
		if err != nil {
				debug.Printf("Failed to create /var/run/lhsmd")
		} else {
				//it should be OK
				debug.Printf("Succeeded to create/var/run/lhsmd")
		}
	} else {
		debug.Printf("/var/run/lhsmd exists")
	}

	return ctx.AgentDriver.cmd.Start()
}


func isDirExists(path string) bool {
    fi, err := os.Stat(path)
 
    if err != nil {
        return os.IsExist(err)
    } else {
        return fi.IsDir()
    }
 
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
	cmd := exec.Command(HsmAgentBinary, agentArgs...)
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
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return errors.Wrap(err, "Failed to create plugin config dir")
	}

	return ioutil.WriteFile(cfgFile, []byte(cfg), 0644)
}

func writeS3MoverConfig(ctx *ScenarioContext, name string) error {
	var awsKey string
	var awsSecret string
	debug.Printf("writeS3MoverConfig")

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
	ctx.S3Region = ctx.Config.S3Region
	ctx.MyArchiveID = ctx.Config.MyArchiveID
	ctx.MyTimeout = ctx.Config.MyTimeout

	if ctx.Config.S3Bucket == "" {
		var err error
		ctx.S3Bucket, err = createS3Bucket(ctx)
		if err != nil {
			return errors.Wrap(err, "Unable to create test bucket")
		}
		debug.Printf("Created S3 bucket: %s", ctx.S3Bucket)
		ctx.AddCleanup(cleanupS3Bucket(ctx))
	} else if ctx.Config.S3Prefix == "" {
//		ctx.S3Prefix = path.Base(ctx.Workdir())
		ctx.S3Prefix = "testprefix"
		debug.Printf("Using %s for S3 prefix", ctx.S3Prefix)
//		ctx.AddCleanup(cleanupS3Prefix(ctx))
	}

	// TODO: Make configuration of credentials optional.
	cfg := fmt.Sprintf(`region = "%s"
aws_access_key_id = "%s"
aws_secret_access_key = "%s"
timeout = "%s"
archive "one" {
	id = "%s"
	bucket = "%s"
	prefix = "%s"
}`, ctx.S3Region, awsKey, awsSecret,ctx.MyTimeout,ctx.MyArchiveID, ctx.S3Bucket, ctx.S3Prefix)

	cfgFile := ctx.Workdir() + "/etc/lhsmd/" + name
	cfgDir := path.Dir(cfgFile)
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return errors.Wrap(err, "Failed to create plugin config dir")
	}

	return ioutil.WriteFile(cfgFile, []byte(cfg), 0644)
}

func writeAliMoverConfig(ctx *ScenarioContext, name string) error {
	var aliKey string
	var aliSecret string
	debug.Printf("writeAliMoverConfig")
	
	aliKey = ctx.Config.AliAccessKeyID
	aliSecret = ctx.Config.AliAccessKeySecret
	if aliKey == "" || aliSecret == "" {
		return fmt.Errorf("Unable to get Ali credentials from environment or harness config")
	}

	// Start with configured defaults
	ctx.AliBucket = ctx.Config.AliBucket
	ctx.AliPrefix = ctx.Config.AliPrefix
	ctx.AliRegion = ctx.Config.AliRegion
	ctx.AliEndpoint = ctx.Config.AliEndpoint	
	ctx.MyArchiveID = ctx.Config.MyArchiveID
	ctx.MyTimeout = ctx.Config.MyTimeout
	ctx.Myproxy = ctx.Config.Myproxy
	ctx.Partsize = ctx.Config.Partsize
	ctx.Routines = ctx.Config.Routines

	if ctx.Config.AliBucket == "" {
		return fmt.Errorf("please set ali_bucket in config")
	} else if ctx.Config.AliPrefix == "" {
		return fmt.Errorf("please set ali_prefix in config")
		
	}


	// TODO: Make configuration of credentials optional.
	cfg := fmt.Sprintf(`
ali_endpoint = "%s"
ali_access_key_id = "%s"
ali_access_key_secret = "%s"
timeout = "%s"
myproxy = "%s"
partsize = "%s"
routines = "%s"
archive "one" {
	id = "%s"
	bucket = "%s"
	prefix = "%s"
}`, ctx.AliEndpoint, aliKey, aliSecret, ctx.MyTimeout, ctx.Myproxy, ctx.Partsize, ctx.Routines, ctx.MyArchiveID, ctx.AliBucket, ctx.AliPrefix)

	cfgFile := ctx.Workdir() + "/etc/lhsmd/" + name
	cfgDir := path.Dir(cfgFile)
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return errors.Wrap(err, "Failed to create plugin config dir")
	}

	return ioutil.WriteFile(cfgFile, []byte(cfg), 0644)
}


func createS3Bucket(ctx *ScenarioContext) (string, error) {
	svc := s3.New(session.New(aws.NewConfig().WithRegion(ctx.Config.S3Region)))
	bucket, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(path.Base(ctx.Workdir())),
	})
	if err != nil {
		return "", errors.Wrap(err, "Failed to create test bucket")
	}
	return path.Base(*bucket.Location), nil
}

func cleanupS3Prefix(ctx *ScenarioContext) cleanupFn {
	svc := s3.New(session.New(aws.NewConfig().WithRegion(ctx.Config.S3Region)))
	doi := &s3.DeleteObjectInput{
		Bucket: aws.String(ctx.S3Bucket),
		Key:    aws.String(ctx.S3Prefix),
	}
	return func() error {
		debug.Printf("Cleaning up %s/%s...", ctx.S3Bucket, ctx.S3Prefix)
		if err := deleteObjects(ctx); err != nil {
			return errors.Wrap(err, "Failed to delete test prefix objects in cleanup")
		}

		_, err := svc.DeleteObject(doi)
		return errors.Wrap(err, "Failed to delete test prefix in cleanup")
	}

}

func deleteObjects(ctx *ScenarioContext) error {
	svc := s3.New(session.New(aws.NewConfig().WithRegion(ctx.Config.S3Region)))
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

	deleteInput := &s3.Delete{}
	for _, obj := range out.Contents {
		deleteInput.Objects = append(deleteInput.Objects, &s3.ObjectIdentifier{Key: obj.Key})
	}
	doi := &s3.DeleteObjectsInput{
		Bucket: aws.String(ctx.S3Bucket),
		Delete: deleteInput,
	}
	_, err = svc.DeleteObjects(doi)

	return errors.Wrap(err, "Failed to delete objects")
}

func cleanupS3Bucket(ctx *ScenarioContext) cleanupFn {
	svc := s3.New(session.New(aws.NewConfig().WithRegion(ctx.Config.S3Region)))

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
	case "lhsm-plugin-ali":
                return writeAliMoverConfig(ctx, name)
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
