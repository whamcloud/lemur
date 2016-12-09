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

dist: *.py ../pipeline.py
	rm -fr dist
	mkdir dist
	cp *.py ../pipeline.py dist
	for pkg in $(PIP_PACKAGES); do \
		pip install --no-compile -t dist $$pkg; \
		rm -fr dist/*/tests; \
	done

clean:
	rm -fr dist $(ZIPFILE)

.PHONY: clean
