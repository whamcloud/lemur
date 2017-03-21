% LHSMD (1) User Manual
% Intel Corporation
% REPLACE_DATE

# NAME

lhsm-plugin-s3 - Lhsmd plugin for AWS S3

# DESCRIPTION

`lhsm-plugin-s3` is a data mover that supports archiving data in AWS S3. It is not intended
to be run directly, and should only be run by `lhsmd`. 

# GENERAL USAGE

The default location for the mover configuration file is `/etc/lhsmd/lhsm-plugin-s3`.
These are the configuration options available.

The four S3 service connection parameters can be set globally or customized for each
archive.

`region`
:     The AWS region to use. The default is `us-east-1`.

`endpoint`
:     The full URL of the S3 service. The service must support auth V4 signed
      authentication. The default value will be the AWS S3 endpoint for the
      current region so this is only needed when using non-AWS S3 services.

`aws_access_key_id`
:      The access key with permissions to write to the provide S3
       bucket. This option is provided for convenience. Typically keys are
       provided though a standard mechanism for AWS tools, such as
       ~/.aws/credentials, AWS_ACCESS_KEY_ID environment variable, or
       an IAM Role. If this is set, then this will take priority over
       other keys found in the environment.

`aws_secret_access_key`
:      The AWS secret key. This option is provided for convenience. Typically keys are
       provided though a standard mechanism for AWS  tools, such as ~/.aws/credentials,
       AWS_SECRET_ACCESS_KEY environment variable, or an IAM Role. If this is set, then
       this will take priority over other keys found in the environment.



`num_threads`
:     The maximum number of concurrent copy requests the plugin will allow.

`archive`
:    Each `archive` section configures an archive endpoint that will be registered with the agent
     and corresponds with a Lustre Archive ID. It is important that each Archive ID be used with the
     same endpoint on each data mover.

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
