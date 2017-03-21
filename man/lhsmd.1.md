% LHSMD (1) User Manual
% Intel Corporation
% REPLACE_DATE

# NAME

lhsmd - Lustre HSM Agent

# SYNOPSIS

lhsmd [-config *FILE*] [-debug]

# DESCRIPTION

Lhsmd is a Lustre HSM Agent. It handles HSM requests from the Lustre
coordinator, and forwards the requests to the configured data mover
plugins based on the archive id of the request. The configuration of
the plugins specifies which Lustre Archive ID is associated with an a
each archive endpoint.  More than one plugin can be used at the same
time, and each data mover can support multiple archive IDs and
endpoints.

The agent configuration file specifies which Lustre filesystem is being managed,
which plugins to start, and options  storing
metrics in an InfluxDB database. By default, example config files are
provided in `/etc/lhsmd`. These can be copied to the correct, non
".example" name and customized accordingly.

Although the agent can be run directly on the command for debugging
purposes, for production use we recommend using systemd (or equivalent) to
manage and run the lhsmd service to ensure only one agent runs per
host.

	# systemctl enable lhsmd
	# systemctl start lhsmd

# OPTIONS

-config *FILE*
:    Specify configuration file instead of using default
     `/etc/lhsmd/agent`.

-debug
:    Enable debug logging.

# GENERAL USAGE

The default location for the agent configuration file is `/etc/lhsmd/agent`. These are the configuration options available.

`client_device`
:     Required option, the `client_device` the mount target for the Lustre filesystem the agent will be using. The
      agent will create mount points of the filesystem for itself and for each of the configured plugins.

`mount_root`
:     The `mount_root` is the location for the Lustre mount points created by the agent.

`enabled_plugins`
:     A list of plugins to start. If the plugin name is not an absolute path, the agent will search for a binary
      matching the plugin name provided here.

`plugin_dir`
:     An additional directory to search for plugins.

`handler_count`
:     Number of threads that will be used to process HSM requests in the agent. (The number of threads in the
      plugins is configured separately)

`snapshots`
:     Optional section to enable the HSM Snapshot feature. When this is enabled,
      then each time a file is archived, the agent will create a released copy of file in
      `.hsmsnapshot` which corresponds to archived version of the file. If the original file
      is changed or deleted, then the snapshot can be used to retrieve the archived version.

      `enabled`
      :     If true, then the experimental HSM snapshot feature is enabled.

`influxdb`
:     Optional section for storing `lhsmd` metrics in an InfluxDB database.

     `url`
     :     Optional URL used for sending metrics to an InfluxDB. If not set, the metrics will not be saved.

     `db`
     :      Name for the database for metrics.

     `user`
     :      InfluxDB user name.

     `password`
     :     InfluxDB password.

# EXAMPLES

A sample agent configuration that enables the snapshot feature:

        mount_root= "/var/lib/lhsmd/roots"
        client_device=  "10.0.2.15@tcp:/lustre"
        enabled_plugins = ["lhsm-plugin-posix", "lhsm-plugin-s3"]
        handler_count = 4
        snapshots {
                enabled = true
        }

        influxdb {
                url = "http://10.0.1.123:8086"
                db = "lhsmd"
                user = "*user*"
                password = "*password*"
        }

# SEE ALSO      

`lhsm-plugin-s3` (1), `lhsm-plugin-posix` (1), `lfs-hsm` (1)
