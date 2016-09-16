FROM lustre-client:latest
MAINTAINER Michael MacDonald <michael.macdonald@intel.com>

RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

RUN yum install -y sudo yum-utils

RUN sed -i -e "s/^\(Defaults\s\+requiretty.*\)/#\1/" /etc/sudoers

ADD ./lemur.spec /tmp/lemur.spec

# prep the image with some build deps, but this will be run again
# for the actual build to catch any changes since the image was built
RUN yum-builddep -y /tmp/lemur.spec && rm /tmp/lemur.spec

VOLUME ["/source", "/root/rpmbuild"]
CMD make -C /source local-rpm
