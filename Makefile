# variable definitions
NAME := lemur
DESC := Lustre HSM Agent and Movers
PREFIX ?= $(PWD)/usr/local
BUILDROOT ?=
VERSION := $(shell git describe --tags --always --dirty | tr '-' '_')
BUILDDATE := $(shell date -u +"%B %d, %Y")
GOVERSION := $(shell go version)
PKG_RELEASE ?= 1
PROJECT_URL := "https://github.intel.comcom/hpdd/$(NAME)"
LDFLAGS := -X 'main.version=$(VERSION)'
FEATURE_TESTS := uat/features

CMD_SOURCES := $(shell find cmd -name main.go)
FEATURE_FILES := $(shell find $(FEATURE_TESTS) -type f -name *.feature -exec basename {} \; | tr '\n' ' ')

TARGETS := $(patsubst cmd/%/main.go,%,$(CMD_SOURCES))
RACE_TARGETS := $(patsubst cmd/%/main.go,%.race,$(CMD_SOURCES))
PANDOC_BIN := $(shell if which pandoc >/dev/null 2>&1; then echo pandoc; else echo true; fi)

$(TARGETS):
	go build -v -i -ldflags "$(LDFLAGS)" -o $@ ./cmd/$@

$(RACE_TARGETS):
	go build -v -i -ldflags "$(LDFLAGS)" --race -o $@ ./cmd/$(basename $@)

# build tasks
rpm: docker-rpm
docker-rpm: docker
	rm -fr $(CURDIR)/output
	mkdir -p $(CURDIR)/output/{BUILD,BUILDROOT,RPMS/{noarch,x86_64},SPECS,SRPMS}
	docker run --rm -v$(CURDIR):/source -v$(CURDIR)/output:/root/rpmbuild lemur-rpm-build

local-rpm:
	$(MAKE) -C packaging/rpm NAME=$(NAME) VERSION=$(VERSION) RELEASE=$(PKG_RELEASE) URL=$(PROJECT_URL)

docker:
	$(MAKE) -C packaging/docker

vendor:
	$(MAKE) -C vendor

# development tasks
check: test uat

test:
	go test $$(go list ./... | grep -v /vendor/ | grep -v /uat/ )

uat: $(RACE_TARGETS)
	@make -C uat test PATH=$(PWD):$(PATH)

coverage:
	@-go test -v -coverprofile=cover.out $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	@-go tool cover -html=cover.out -o cover.html

benchmark:
	@echo "Running tests..."
	@go test -bench=. $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)

%.1: man/%.1.md
	sed "s/REPLACE_DATE/$(BUILDDATE)/" $< | $(PANDOC_BIN) -s -t man -o $@

# Man pages
MAN_SOURCES := $(shell find man -name "*.md")
MAN_TARGETS := $(patsubst man/%.md,%,$(MAN_SOURCES))

docs: $(MAN_TARGETS)

all: lint $(TARGETS) $(MAN_TARGETS)
.DEFAULT_GOAL:=all

# Installation
INSTALLED_TARGETS = $(addprefix $(PREFIX)/bin/, $(TARGETS))
INSTALLED_MAN_TARGETS = $(addprefix $(PREFIX)/share/man/man1/, $(MAN_TARGETS))
# test targets
UAT_RACE_TARGETS_DEST := libexec/$(NAME)-testing
INSTALLED_RACE_TARGETS = $(addprefix $(PREFIX)/$(UAT_RACE_TARGETS_DEST)/, $(RACE_TARGETS))
UAT_FEATURES_DEST := share/$(NAME)/test/features
INSTALLED_FEATURES = $(addprefix $(PREFIX)/$(UAT_FEATURES_DEST)/, $(FEATURE_FILES))

# Sample config files
#
EXAMPLES = $(shell find doc -name "*.example")
EXAMPLE_TARGETS = $(patsubst doc/%,%,$(EXAMPLES))
INSTALLED_EXAMPLES = $(addprefix $(PREFIX)/etc/lhsmd/, $(EXAMPLE_TARGETS))

# Cleanliness...
lint:
	gometalinter -j2 --vendor -D errcheck -D dupl -D gocyclo --deadline 60s ./... --exclude pdm/

# install tasks
$(PREFIX)/bin/%: %
	install -d $$(dirname $@)
	install -m 755 $< $@

$(PREFIX)/share/man/man1/%: %
	install -d $$(dirname $@)
	install -m 644 $< $@

$(PREFIX)/$(UAT_FEATURES_DEST)/%: $(FEATURE_TESTS)/%
	install -d $$(dirname $@)
	install -m 644 $< $@

$(PREFIX)/$(UAT_RACE_TARGETS_DEST)/%: %
	install -d $$(dirname $@)
	install -m 755 $< $@

$(PREFIX)/etc/lhsmd/%:
	install -d $$(dirname $@)
	install -m 644 doc/$$(basename $@) $@

install-example: $(INSTALLED_EXAMPLES)

install: $(INSTALLED_TARGETS) $(INSTALLED_MAN_TARGETS)

local-install:
	$(MAKE) install PREFIX=usr/local

$(NAME)-uat-runner: uat/*.go
	cd uat && \
	go test -c -o $(PWD)/$@ -ldflags="-X '_$(PWD)/uat.runDir=$(subst $(BUILDROOT),,$(dir $(PREFIX)/$(UAT_FEATURES_DEST)))' -X '_$(PWD)/uat.raceBinPath=$(subst $(BUILDROOT),,$(PREFIX)/$(UAT_RACE_TARGETS_DEST))'"

uat-install: $(NAME)-uat-runner $(INSTALLED_RACE_TARGETS) $(INSTALLED_FEATURES)
	install -m 755 $(NAME)-uat-runner $(PREFIX)/$(UAT_RACE_TARGETS_DEST)

# clean up tasks
clean-docs:
	rm -rf ./docs

clean-deps:
	rm -rf $(DEPDIR)

clean: clean-docs clean-deps
	rm -rf ./usr
	rm -f $(TARGETS)
	rm -f $(RACE_TARGETS)
	rm -f $(MAN_TARGETS)
	rm -f $(NAME)-uat-runner

.PHONY: $(TARGETS) $(RACE_TARGETS)
.PHONY: all check test uat rpm deb install local-install packages  coverage docs jekyll deploy-docs clean-docs clean-deps clean uat-install vendor
