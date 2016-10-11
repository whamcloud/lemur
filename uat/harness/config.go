package harness

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"strconv"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/debug"

	"github.com/hashicorp/hcl"
	"github.com/pkg/errors"
)

const (
	// UATConfigFile is the name of the harness configuration file
	UATConfigFile = ".lhsmd-config"

	// UATConfigEnvVar is the name of the optional environment variable that
	// may be set to specify config location
	UATConfigEnvVar = "LHSMD_UAT_CONFIG_FILE"
)

// Config holds configuration for the test harness
type Config struct {
	HsmDriver        string `hcl:"hsm_driver" json:"hsm_driver"`
	LustrePath       string `hcl:"lustre_path" json:"lustre_path"`
	CleanupOnFailure bool   `hcl:"cleanup_on_failure" json:"cleanup_on_failure"`
	EnableAgentDebug bool   `hcl:"enable_agent_debug" json:"enable_agent_debug"`

	AWSAccessKeyID     string `hcl:"aws_access_key_id" json:"aws_access_key_id"`
	AWSSecretAccessKey string `hcl:"aws_secret_access_key" json:"aws_secret_access_key"`
	S3Region           string `hcl:"s3_region" json:"s3_region"`
	S3Bucket           string `hcl:"s3_bucket" json:"s3_bucket"`
	S3Prefix           string `hcl:"s3_prefix" json:"s3_prefix"`
	AliAccessKeyID     string `hcl:"ali_access_key_id" json:"ali_access_key_id"`
        AliAccessKeySecret string `hcl:"ali_access_key_secret" json:"ali_access_key_secret"`
	AliBucket           string `hcl:"ali_bucket" json:"ali_bucket"`
        AliPrefix           string `hcl:"ali_prefix" json:"ali_prefix"`	
	AliRegion           string `hcl:"ali_region" json:"ali_region"`	
	AliEndpoint           string `hcl:"ali_endpoint" json:"ali_endpoint"`	
	MyArchiveID           string `hcl:"archiveid" json:"archiveid"`
	MyTimeout           string `hcl:"timeout" json:"timeout"`	
	Myproxy           string `hcl:"myproxy" json:"myproxy"`	
	Partsize           string `hcl:"partsize" json:"partsize"`
        Routines           string `hcl:"routines" json:"routines"`	
}

// Merge combines this config's values with the other config's values
func (c *Config) Merge(other *Config) *Config {
	result := new(Config)

	result.HsmDriver = c.HsmDriver
	if other.HsmDriver != "" {
		result.HsmDriver = other.HsmDriver
	}

	result.LustrePath = c.LustrePath
	if other.LustrePath != "" {
		result.LustrePath = other.LustrePath
	}

	result.CleanupOnFailure = other.CleanupOnFailure
	result.EnableAgentDebug = other.EnableAgentDebug

	result.S3Region = c.S3Region
	if other.S3Region != "" {
		result.S3Region = other.S3Region
	}

	result.S3Bucket = c.S3Bucket
	if other.S3Bucket != "" {
		result.S3Bucket = other.S3Bucket
	}
	
	result.S3Prefix = c.S3Prefix
	if other.S3Prefix != "" {
		result.S3Prefix = other.S3Prefix
	}
	
	result.AliBucket = c.AliBucket
        if other.AliBucket != "" {
                result.AliBucket = other.AliBucket
        }

        result.AliPrefix = c.AliPrefix
        if other.AliPrefix != "" {
                result.AliPrefix = other.AliPrefix
        }

	result.AliRegion = c.AliRegion
        if other.AliRegion != "" {
                result.AliRegion = other.AliRegion
        }
		
	result.AliEndpoint = c.AliEndpoint
        if other.AliEndpoint != "" {
                result.AliEndpoint = other.AliEndpoint
        }
	
	result.Myproxy = c.Myproxy
        if other.Myproxy != "" {
                result.Myproxy = other.Myproxy
        }
	

	result.MyArchiveID = c.MyArchiveID
        if other.MyArchiveID != "" {
                result.MyArchiveID = other.MyArchiveID
        }
	//error handling 
	iTemp, err := strconv.ParseInt(result.MyArchiveID,10,64)
	if err != nil {
		result.MyArchiveID = "1"
	}
	
	if (iTemp < 1 || iTemp > 32) {
		result.MyArchiveID = "1"
	}

	result.MyTimeout = c.MyTimeout
        if other.MyTimeout != "" {
                result.MyTimeout = other.MyTimeout
        }
	
	iTemp, err = strconv.ParseInt(result.MyTimeout,10,64)
        if err != nil {
                result.MyTimeout = "-1"
		iTemp = -1
        }
	
	if (iTemp == -1 || iTemp == 0)	{
		result.MyTimeout = "1000000000"
	}
	

	debug.Printf("result.S3Prefix-%s,c.S3Prefix-%s",result.S3Prefix,c.S3Prefix)
	result.AWSAccessKeyID = c.AWSAccessKeyID
	if other.AWSAccessKeyID != "" {
		result.AWSAccessKeyID = other.AWSAccessKeyID
	}

	result.AWSSecretAccessKey = c.AWSSecretAccessKey
	if other.AWSSecretAccessKey != "" {
		result.AWSSecretAccessKey = other.AWSSecretAccessKey
	}

	result.AliAccessKeyID = c.AliAccessKeyID
        if other.AliAccessKeyID != "" {
                result.AliAccessKeyID = other.AliAccessKeyID
        }

        result.AliAccessKeySecret = c.AliAccessKeySecret
        if other.AliAccessKeySecret != "" {
                result.AliAccessKeySecret = other.AliAccessKeySecret
        }	

	result.Partsize = c.Partsize
        if other.Partsize != "" {
                result.Partsize = other.Partsize
        }
	
	iTemp, err = strconv.ParseInt(result.Partsize,10,64)
        if err != nil {
                result.Partsize = "1"
        }

        if (iTemp <= 0) {
                result.Partsize = "1"
        }


        result.Routines = c.Routines
        if other.Routines != "" {
                result.Routines = other.Routines
        }

	iTemp, err = strconv.ParseInt(result.Routines,10,64)
        if err != nil {
                result.Routines = "1"
        }

        if (iTemp <= 0) {
                result.Routines = "1"
        }


	return result
}

func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		alert.Abort(errors.Wrap(err, "couldn't marshal test config to json"))
	}

	var out bytes.Buffer
	json.Indent(&out, data, "", "\t")
	return out.String()
}

// NewConfig initializes a new Config instance with default values
func NewConfig() *Config {
	return &Config{
		S3Region: "us-east-1",
	}
}

// LoadConfig attempts to load a config from one of the default locations
func LoadConfig() (*Config, error) {
	cfg := NewConfig()

	user, err := user.Current()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to get current user")
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to get current directory")
	}
	// list of locations to try, in decreasing precedence
	locations := []string{
		os.Getenv(UATConfigEnvVar),
		path.Join(cwd, UATConfigFile),
		path.Join(user.HomeDir, UATConfigFile),
	}

	for _, location := range locations {
		debug.Printf("trying to load config from %s", location)
		if loaded, err := loadConfigFile(location); err == nil {
			cfg = cfg.Merge(loaded)
			break
		}
	}

	debug.Printf("Harness config: %s", cfg)
	return cfg, nil
}

func loadConfigFile(cfgPath string) (*Config, error) {
	cfg := NewConfig()

	data, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}

	if err := hcl.Decode(cfg, string(data)); err != nil {
		alert.Warnf("config file error %s:%s", cfgPath, err)
		return nil, err
	}

	return cfg, nil
}
