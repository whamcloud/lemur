% LHSMD (1) User Manual
% Google LLC
% REPLACE_DATE

# NAME

lhsm-plugin-gcs - Lhsmd plugin for Google Cloud Storage (GCS)

# DESCRIPTION

`lhsm-plugin-gcs` is a data mover that supports archiving data in Google Cloud Storage. It is not intended
to be run directly, and should only be run by `lhsmd`. 

# GENERAL USAGE

The default location for the mover configuration file is `/etc/lhsmd/lhsm-plugin-gcs`.
These are the configuration options available.

`credentials`
:      The service account key with permissions to create, get and delete objects to the provided GCS
       bucket. This option is provided for convenience. Typically permissions to a bucket are
       provided through a service account applied to the VM instance.
       If this option is set, then it will take priority over the service account of 
       the VM instance.

`num_threads`
:     The maximum number of concurrent copy requests the plugin will allow.

`archive`
:    Each `archive` section configures an archive endpoint that will be registered with the agent
     and corresponds with a Lustre Archive ID. It is important that each Archive ID be used with the
     same endpoint on each data mover.

     `id`
     :     The ID associated with this archive.

     `bucket`
     :     The GCS bucket that will be used.

     `prefix`
     :     An optional prefix key for the archive objects.

# EXAMPLES

A sample GCS plugin configuration with one archive:

        num_threads = 8

        archive  "gcs-test" {
          id = 3
          bucket = "*bucket*"
          prefix = "gcs-test-archive"
        }

# SEE ALSO

`lhsmd` (1)
