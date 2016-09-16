FROM host-kernel:latest
MAINTAINER Michael MacDonald <michael.macdonald@intel.com>

env REPO_NAME lustre-client

ARG package_url

RUN echo -e "[${REPO_NAME}]\nname=${REPO_NAME}\ngpgcheck=0\nbaseurl=${package_url}\n" | sed -e 's/,/%2C/g' > /etc/yum.repos.d/${REPO_NAME}.repo \
	&& unset no_proxy NO_PROXY \
	&& yum install -y lustre-client
