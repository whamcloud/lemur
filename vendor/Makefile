export PATH := $(GOPATH)/bin:$(PATH)
GVT_BIN = $(shell PATH=$(PATH); if which gvt 2>/dev/null; then true; else echo get-gvt; fi)
GO_SOURCES := $(shell find $(dir $(CURDIR)) -name '*.go' -and -not -path '*vendor*' -and -not -path '*output*')
GO_SRC_DIRS := $(addprefix .,$(sort $(subst $(dir $(CURDIR)),,$(dir $(GO_SOURCES)))))
IMPORT_PATH := $(subst $(GOPATH)/src/,,$(dir $(CURDIR)))

# Explicit repo imports which need to be added because they can't be inferred
# from imports of repo subdirs. These are needed for slurp.
EXPLICIT_IMPORTS :=

# These patterns match imports which we don't need to ship, and therefore
# don't want to include in the vendor tree.
define EXCLUDES
camlistore.org # golang.org/x/net/http2/h2demo/h2demo.go
go4.org # golang.org/x/net/http2/h2demo/h2demo.go
cloud.google.com
google.golang.org/appengine
google.golang.org/api
github.com/BurntSushi # gopkg.in/urfave/cli.v1/altsrc
github.com/urfave # gopkg.in/urfave/cli.v1/altsrc
github.com/stathat # github.com/rcrowley/go-metrics/stathat/stathat.go
github.com/intel-hpdd/logging/applog
github.com/fatih # github.com/intel-hpdd/logging/applog
github.com/mattn # github.com/intel-hpdd/logging/applog
github.com/briandowns # github.com/intel-hpdd/logging/applog
endef
export EXCLUDES

define IMPORT_LIST_FN
$$(deps=; for dep in $$(go list -f '{{ join .Deps "\n" }}' $(GO_SRC_DIRS) | sort | uniq); do if localdep=$$(go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' $$dep 2>/dev/null); then deps="$$deps\n$$localdep"; else deps="$$deps\n$$dep"; fi; done; echo $$deps | egrep -v "($$EXCLUDE_LIST$(IMPORT_PATH)|vendor)" | sort | uniq)
endef
export IMPORT_LIST_FN

manifest: $(GVT_BIN) $(GO_SOURCES)
	@pushd .. >/dev/null; \
	echo "Building list of dependencies in $(GO_SRC_DIRS) and checking for missing vendors..."; \
	EXCLUDE_LIST=$$(echo $$"$$EXCLUDES" | sed -e 's/[[:space:]]*#.*$$//' | tr '\n' '|'); \
	IMPORT_LIST="$$(if [ '$(EXPLICIT_IMPORTS)' != "" ]; then echo '$(EXPLICIT_IMPORTS) '; fi)$(IMPORT_LIST_FN)"; \
	while [ "$$IMPORT_LIST" != "" ]; do \
		echo "Vendoring $$(echo $$IMPORT_LIST | wc -w) dependencies."; \
		for import in $$IMPORT_LIST; do \
			$(GVT_BIN) fetch -no-recurse $$import; \
		done; \
		IMPORT_LIST=$(IMPORT_LIST_FN); \
	done; \
	echo "Done."; \
	popd >/dev/null; \
	touch $@

get-gvt:
	go get -u github.com/FiloSottile/gvt
