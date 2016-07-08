% LHSMD (1) User Manaual
% Intel Corporation
% REPLACE_DATE

# NAME

lhsm-plugin-s3 - Lhsmd plugin for AWS S3

# DESCRIPTION

`lhsm-plugin-s3` is a data mover that supports archiving data in AWS S3. It is not intended
to be run directly, and should only be run by `lhsmd`.  It is configured using the
configuration file.

# GENERAL USAGE

The default location for the mover configuration file is `/etc/lhsm/lhsm-plugin-s3`.
These are the configuration options available.

`region`
:     The AWS region to use. The default is `us-east-1`.

`endpoint`
:     The full URL of the S3 service. The service must support auth V4 signed
      authentication. The default value will be the AWS S3 endpoint for the
      current region.

`num_threads`
:     The maximum number of concurrent copy requests the plugin will allow.

`archive`
:    Each `archive` section configures an archive endpoint that will be registered with the agent
     and corresponds with a Lustre Archive ID. It is important that each Archive ID be used with the
     same endpoint on each data mover newUploader

     `id`
     :     The ID associated with this archive.

     `bucket`
     :     The AWS S3 bucket that will be used.

     `prefix`
     :     An optional prefix key for the archive objects.

# EXAMPLES

A sample S3 plugin configuration with one archive:

        num_threads = 8

        archive  "s3-test" {
          id = 2
          bucket = "*bucket*"
          prefix = "s3-test-archive"
        }

# SEE ALSO

`lhsmd` (1)
