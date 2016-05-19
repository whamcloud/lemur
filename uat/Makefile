ROOTDIR := $(dir $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
GODOG_BIN := $(shell if which godog >/dev/null 2>&1; then true; else echo godog; fi)
PLUGINS := $(notdir $(shell ls -d $(ROOTDIR)/lhsm-plugin-*))
TEST_BINARIES := lhsmd $(PLUGINS)

default: test
binaries: $(TEST_BINARIES)

$(TEST_BINARIES):
	@echo -n "Building $@... "
	@cd $(ROOTDIR)/$@ && \
	go install -race && \
	which $@

$(GODOG_BIN):
	@echo "You need to go get github.com/DATA-DOG/godog/cmd/godog"
	exit 1

test: $(GODOG_BIN) $(TEST_BINARIES)
	@echo Running User Acceptance Tests...
	@sudo -E sh -c "export PATH=$(PATH); $(shell which godog) ."
