SOURCE    := $(NAME)-$(VERSION).tar.gz
WORKDIR   := $(shell mktemp -d)
SPECFILE  := $(NAME).spec
INITFILES := $(patsubst %,$(WORKDIR)/%,lhsmd.service lhsmd.conf)
USER      ?= $(shell whoami)
YBLDDEP   := $(shell if which yum-builddep 2>/dev/null; then true; else echo yum-builddep; fi)

rpm: $(WORKDIR)/$(SOURCE) $(INITFILES) $(WORKDIR)/$(SPECFILE) $(WORKDIR)/.deps
	cd $(WORKDIR) && \
	rpmbuild --define '%_sourcedir $(WORKDIR)' \
	         --define '%_gitver $(VERSION)' \
		 --define '%PACKAGE_PREFIX $(NAME)' \
		 --define '%dist $(RELEASE)' \
		 -ba $(SPECFILE) && \
	rm -fr $(WORKDIR)
	# clean up any cruft left by the go compiler
	rm -fr $$(rpm --eval '%{_builddir}')/$(NAME)-*

$(WORKDIR)/.deps: $(YBLDDEP) $(WORKDIR)/$(SPECFILE)
	sudo yum-builddep -y $(WORKDIR)/$(SPECFILE) && touch $(WORKDIR)/.deps

$(YBLDDEP):
	sudo yum install -y yum-utils

$(WORKDIR)/$(SPECFILE):
	cp $(SPECFILE) $(WORKDIR) && chown $(USER).$(USER) $(WORKDIR)/$(SPECFILE)

$(WORKDIR)/$(SOURCE):
	cd ../../ && \
	tar --owner=$(USER) --group=$(USER) \
	    --exclude=.git --exclude=*.swp \
	    --transform 's|./|$(NAME)-$(VERSION)/|' -czf $@ ./ && \
	chown $(USER).$(USER) $@

$(INITFILES):
	cp $(notdir $@) $@
