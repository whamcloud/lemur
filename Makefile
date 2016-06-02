ROOTDIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
GODOG_BIN := $(shell if which godog >/dev/null 2>&1; then true; else echo godog; fi)
PLUGINS := $(notdir $(shell ls -d $(ROOTDIR)/cmd/lhsm-plugin-*))
BINARIES := lhsmd $(PLUGINS)

.PHONY: default $(BINARIES)

default: test install
install: $(BINARIES)


$(BINARIES):
	@echo -n "Installing $@... "
	@cd $(ROOTDIR)/cmd/$@ && \
	go install && \
	which $@

test:
	go test ./...
	make -C uat test
