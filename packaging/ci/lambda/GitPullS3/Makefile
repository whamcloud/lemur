PYTHON ?= python2.7
FUNCTION_NAME = $(notdir $(CURDIR))
OUTDIR ?= $(dir $(CURDIR))
ZIPFILE ?= $(OUTDIR)/$(FUNCTION_NAME).zip

$(ZIPFILE): *.py
	rm -f $@
	zip -r $@ *

clean:
	rm -f $(ZIPFILE)

.PHONY: clean
