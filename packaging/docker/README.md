# Docker Images

In this directory are several recipes for creating Docker images. The go-el7 image is generally useful as a base EL7 image with the latest go packages installed (at time of writing, go 1.7.1-1 is latest). After running `make -C go-el7`, an image with the tags go-el7:latest and go-el7:(version) will be created.

### go-el7
 * Creates an EL7 image with the most recent version of go packaged by the Fedora project
 * Image tags: go-el7:latest, go-el7:(version)

### host-kernel
  * Delegates to linux-host-kernel or mac-host-kernel, depending on host platform

### mac-host-kernel
  * Builds on the latest go-el7 image to create a kernel-devel image suitable for building and installing a Lustre client (on Mac, the targeted Linux kernel is the Docker-provided Moby kernel)
  * Prerequisite image(s): go-el7:latest
  * Image tags: mac-host-kernel:latest, mac-host-kernel:(moby version), host-kernel:latest

### linux-host-kernel
  * Builds on the latest go-el7 image to create a kernel-devel image suitable for building and installing a Lustre client
  * Prerequisite image(s): go-el7:latest
  * Image tags: linux-host-kernel:latest, linux-host-kernel:(uname -r), host-kernel:latest

### native-lustre-client
  * Builds on the host-kernel image to create a lustre-client image suitable for building against liblustreapi and/or mounting a Lustre client from within a container (pulls from latest successful master build on jenkins)
  * Prerequisite image(s): go-el7:latest, host-kernel:latest
  * Image tags: native-lustre-client:latest, native-lustre-client:(lustre version), lustre-client:latest

### buildonly-lustre-client
  * Simple image which just installs a downloaded lustre-client RPM without attempting to match it with the host kernel -- only useful for builds against liblustreapi
  * Prerequisite image(s): go-el7:latest
  * Image tags: buildonly-lustre-client:latest, buildonly-lustre-client:(lustre version), lustre-client:latest

### lemur-rpm-build
  * Image which can be used to produce lemur RPMs from the source tree
  * Prerequisite image(s): go-el7:latest, lustre-client:latest
  * Image tags: lemur-rpm-build:latest, lemur-rpm-build:(version)