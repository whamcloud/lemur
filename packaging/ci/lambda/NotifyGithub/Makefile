PYTHON ?= python2.7
PIP_PACKAGES ?= PyGithub
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
	for pkg in $(PIP_PACKAGES); do \
		pip install --no-compile -t dist $$pkg; \
		rm -fr dist/*/tests; \
	done

clean:
	rm -fr dist $(ZIPFILE)

.PHONY: clean
