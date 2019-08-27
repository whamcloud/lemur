[![GoDoc](https://godoc.org/github.com/intel-hpdd/lemur/dmplugin?status.svg)](https://godoc.org/github.com/intel-hpdd/lemur/dmplugin)

# HPDD HSM Agent and Data Movers for Lustre

Lemur is new, open source implementation of HSM tools for the [Lustre Filesystem](http://lustre.org) based on this
[design](https://wiki.hpdd.intel.com/display/PUB/HSM+Agent+Design). This project
is developed by the Intel High Performance Data Division, one of the main
contributors to the Lustre Filesystem. We welcome community involvement and
contributions. If you would like to raise an issue or file a bug, please use the
LMR project on our community [Jira
server](https://jira.whamcloud.com/projects/LMR) instead of Github issues.


## Build

We use Docker for building packages, so as long as you have Docker running,
`make rpm` will take care of everything needed to create a clean build
environment and build a set of Lemur RPMs. The results will be stored in
`./output/RPMS`.

For development and testing, it is generally more convenient to use a Linux host
(or virtual machine) with Lustre installed. We recommend RHEL or CentOS 7.x,
Lustre 2.7 or above, and at least Go 1.6.


## Testing Quickstart

The unit tests are run with `make test` and do not require a Lustre environment
to be configured.

The user acceptance test (UAT) automates basic testing with the agent and
datamovers. The harness does not manage Lustre filesystems -- you will need to
create one and mount a client somewhere, and ensue the Coordinator is enabled on
the MDT. The harness must run as root, because the agent must also run as root in
order to fiddle with secure xattrs and do other root-y stuff.


1. `make rpm`
1. Copy built RPMs to Lustre client host and install them
1. `sudo /usr/libexec/lemur-testing/lemur-uat-runner`

Set [uat/README.md](uat/README.md) for more details on running and confgiuring the user acceptance tests.
