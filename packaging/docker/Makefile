PROXY_VARS := HTTP_PROXY HTTPS_PROXY FTP_PROXY NO_PROXY http_proxy https_proxy ftp_proxy no_proxy
BUILD_VARS := $(shell bv=; for pvar in $(PROXY_VARS); do if [ x"$${!pvar}" == "x" ]; then continue; fi; bv="$$bv --build-arg=$$pvar=$${!pvar}"; done; echo $$bv)
SUBDIRS := go-el7 host-kernel lustre-client

subdirs: $(SUBDIRS)

$(SUBDIRS):
	@BUILD_VARS="$(BUILD_VARS)" make -C $@

host-kernel: go-el7
lustre-client: host-kernel

.PHONY: $(SUBDIRS) subdirs
