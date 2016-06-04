# lemur
HPDD HSM Agent/Data Movers

## Testing Quickstart
NB: The product test harness does not manage Lustre filesystems -- you will need to create one and mount a client somewhere.

NB: The harness must run as root, because the agent must run as root in order to fiddle with secure xattrs and do other root-y stuff.

NB: The s3 data mover tests will fail unless it finds valid AWS credentials in the environment.

1. Clone this repo to an el7 build host (other platforms coming later)
1. `make rpm`
1. Copy built RPMs to Lustre client host and install them
1. `sudo /usr/libexec/lemur-testing/lemur-uat-runner`
