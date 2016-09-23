GVT_BIN := $(shell if which gvt >/dev/null 2>&1; then true; else echo get-gvt; fi)
GO_SOURCES := $(shell find .. -name *.go -and -not -path *vendor* -and -not -path *output*)
GO_SRC_DIRS := $(shell echo $(GO_SOURCES) | tr ' ' '\n' | sed -e 's/^..\///' -e 's/\/.*$$//' | sort | uniq | tr '\n' ' ')

# Explicit repo imports which need to be added because they can't be inferred
# from imports of repo subdirs or are in the UAT dir.
EXPLICIT_IMPORTS := github.intel.com/hpdd/logging github.com/DATA-DOG/godog

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
github.intel.com/hpdd/logging/applog
github.com/fatih # github.intel.com/hpdd/logging/applog
github.com/mattn # github.intel.com/hpdd/logging/applog
github.com/briandowns # github.intel.com/hpdd/logging/applog
endef
export EXCLUDES


manifest: $(GVT_BIN) $(GO_SOURCES)
	# First, build the naive import set recursively.
	pushd .. >/dev/null; \
	imports="$(EXPLICIT_IMPORTS) $$(go list -f '{{ join .Deps "\n" }}' $(addsuffix /...,$(addprefix ./,$(GO_SRC_DIRS))) | grep -v vendor | xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' | sort | uniq)"; \
	for import in $$imports; do \
		$(GVT_BIN) fetch $$import; \
	done; \
	popd >/dev/null; \
	touch $@

	# Next, run through the vendored imports and remove those we
	# know our production code doesn't need.
	# It would be better if we could prevent these from being imported
	# in the first place, but then we'd need to build the recursive
	# imports list ourselves instead of letting gvt do it.
	pushd .. >/dev/null; \
	EXCLUDE_LIST=$$(echo $$"$$EXCLUDES" | sed -e 's/\s*#.*//'); \
	for vendored in $$($(GVT_BIN) list -f "{{.Importpath}}"); do \
		for exclude in $$EXCLUDE_LIST; do \
			if [[ $$vendored == *$$exclude* ]]; then \
				$(GVT_BIN) delete $$vendored; \
				continue 2; \
			fi; \
		done; \
	done; \
	popd >/dev/null

	# Finally, run through the excludes and nuke any remaining cruft.
	EXCLUDE_LIST=$$(echo $$"$$EXCLUDES" | sed -e 's/\s*#.*//'); \
	for exclude in $$EXCLUDE_LIST; do \
		if [ -n "$$exclude" ] && [[ $$exclude != /* ]]; then \
			rm -fr $$exclude; \
		fi; \
	done

get-gvt:
	go get -u github.com/FiloSottile/gvt
GVT_BIN = $(shell which gvt)
