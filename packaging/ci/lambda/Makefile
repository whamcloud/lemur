SUBDIRS = $(dir $(wildcard */Makefile))

.PHONY: subdirs $(SUBDIRS)

subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ OUTDIR=$(CURDIR)

clean:
	for subdir in $(SUBDIRS); do \
	       $(MAKE) -C $$subdir clean; \
       	done	       
