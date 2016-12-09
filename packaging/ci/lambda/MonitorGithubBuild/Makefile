PYTHON ?= python2.7
FUNCTION_NAME = $(notdir $(CURDIR))
OUTDIR ?= $(dir $(CURDIR))
ZIPFILE ?= $(OUTDIR)/$(FUNCTION_NAME).zip

$(ZIPFILE): dist
	rm -f $@
	pushd dist >/dev/null && \
	zip -r $@ * && \
	popd >/dev/null

dist: *.py $(dir $(CURDIR))/lemur_ci/*.py
	rm -fr dist
	mkdir dist
	cp -a *.py $(dir $(CURDIR))/lemur_ci dist

clean:
	rm -fr dist $(ZIPFILE)

.PHONY: clean
