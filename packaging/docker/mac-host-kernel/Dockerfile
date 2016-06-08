FROM go-el7:latest
MAINTAINER Michael MacDonald <michael.macdonald@intel.com>

# Grab host kernel source and prepare symlinks.
RUN export KERNEL_VERSION=$(uname -r | cut -d '-' -f 1) \
	&& mkdir -p /usr/src/kernels \
	&& curl -L https://www.kernel.org/pub/linux/kernel/v${KERNEL_VERSION%%.*}.x/linux-$KERNEL_VERSION.tar.xz | tar -C /usr/src/kernels -xJ \
	&& mv /usr/src/kernels/linux-$KERNEL_VERSION /usr/src/kernels/$KERNEL_VERSION \
	&& mkdir -p /lib/modules/$(uname -r) \
	&& ln -sf /usr/src/kernels/$KERNEL_VERSION /lib/modules/$(uname -r)/build \
	&& ln -sf build /lib/modules/$(uname -r)/source

RUN yum install -y bc

# Set up host kernel source for building DKMS client.
# Notes:
# 1) We have to pretend that it's a RHEL kernel in order to make DKMS happy
RUN export KERNEL_VERSION=$(uname -r | cut -d '-' -f 1) \
        && yum install -y bc \
	&& RHEL_RELEASE=($(awk '{gsub(/\./, " ", $4); print $4}' /etc/redhat-release)) \
	&& cd /usr/src/kernels/$KERNEL_VERSION \
	&& zcat /proc/1/root/proc/config.gz > .config \
	&& make modules_prepare \
	&& echo -e "#define RHEL_MAJOR ${RHEL_RELEASE[0]}\n#define RHEL_MINOR ${RHEL_RELEASE[1]}\n#define RHEL_RELEASE \"${RHEL_RELEASE[0]}.${RHEL_RELEASE[1]}.${RHEL_RELEASE[2]}\"\n" >> include/generated/uapi/linux/version.h
