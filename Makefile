# variable definitions
NAME := lemur
DESC := Lustre HSM Agent and Movers
PREFIX ?= usr/local
VERSION := $(shell git describe --tags --always --dirty)
BUILDDATE := $(shell date -u +"%B %d, %Y")
GOVERSION := $(shell go version)
PKG_RELEASE ?= 1
PROJECT_URL := "https://github.intel.comcom/hpdd/$(NAME)"
LDFLAGS := -X 'main.version=$(VERSION)'

CMD_SOURCES := $(shell find cmd -name main.go)

TARGETS := $(patsubst cmd/%/main.go,%,$(CMD_SOURCES))
GODOG_BIN := $(shell if which godog >/dev/null 2>&1; then echo godog; else echo true; fi)
PANDOC_BIN := $(shell if which pandoc >/dev/null 2>&1; then echo pandoc; else echo true; fi)

$(TARGETS):
	go build -i -ldflags "$(LDFLAGS)" -o $@ ./cmd/$@

# development tasks
check: test all

test:
	go test $$(go list ./... | grep -v /vendor/ ) # | grep -v /cmd/)

uat:
	make -C uat test

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

# Installation
INSTALLED_TARGETS = $(addprefix $(PREFIX)/bin/, $(TARGETS))
INSTALLED_MAN_TARGETS = $(addprefix $(PREFIX)/share/man/man1/, $(MAN_TARGETS))

# install tasks
$(PREFIX)/bin/%: %
	install -d $$(dirname $@)
	install -m 755 $< $@

$(PREFIX)/share/man/man1/%: %
	install -d $$(dirname $@)
	install -m 644 $< $@

install: $(INSTALLED_TARGETS) $(INSTALLED_MAN_TARGETS)

local-install:
	$(MAKE) install PREFIX=usr/local

# clean up tasks
clean-docs:
	rm -rf ./docs

clean-deps:
	rm -rf $(DEPDIR)

clean: clean-docs clean-deps
	rm -rf ./usr
	rm -f $(TARGETS)
	rm -f $(MAN_TARGETS)

all: $(TARGETS) $(MAN_TARGETS)
.DEFAULT_GOAL:=all

.PHONY: $(TARGETS)
.PHONY: all check test uat rpm deb install local-install packages  coverage docs jekyll deploy-docs clean-docs clean-deps clean
