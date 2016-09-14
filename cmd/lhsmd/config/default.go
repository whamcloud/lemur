package config

const (
	// DefaultConfigDir is the default agent config directory
	DefaultConfigDir = "/etc/lhsmd"
	// AgentConfigFile is the agent config file in config dir
	AgentConfigFile = "agent"
	// DefaultConfigPath is the default path to the agent config file
	DefaultConfigPath = DefaultConfigDir + "/" + AgentConfigFile

	// ConfigDirEnvVar is the name of an environment variable which
	// can be set to change the location of config files
	// (e.g. for development)
	ConfigDirEnvVar = "LHSMD_CONFIG_DIR"

	// AgentConnEnvVar is the environment variable containing a connect
	// string for plugins to use when registering with the agent
	AgentConnEnvVar = "LHSMD_AGENT_CONNECTION"

	// PluginMountpointEnvVar is the environment variable containing
	// a Lustre client mountpoint to be used by the plugin
	PluginMountpointEnvVar = "LHSMD_CLIENT_MOUNTPOINT"

	// DefaultTransport is the default agent<->plugin transport
	DefaultTransport = "grpc"

	// DefaultTransportSocketDir is default directory to store the unix socket
	DefaultTransportSocketDir = "/var/run/lhsmd"

	// DefaultAgentMountRoot is the root directory for agent client mounts
	DefaultAgentMountRoot = "/mnt/lhsmd"

	// DefaultPluginDir is the default location for plugin binaries
	DefaultPluginDir = "/usr/libexec/lhsmd"
)

// DefaultClientMountOptions is the default set of Lustre client
// mount options
var DefaultClientMountOptions = []string{"user_xattr"}
