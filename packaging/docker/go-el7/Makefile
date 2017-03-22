FEDORA_GOLANG ?= $(shell curl -s http://mirrors.kernel.org/fedora/development/rawhide/Everything/source/tree/Packages/g/ | awk '/>golang-1.*\.src\.rpm</ {print $2}' | sed -E 's/.*golang-(1.*)\.src\.rpm.*/\1/')
FEDORA_GOLANG_MACROS ?= $(shell curl -s http://mirrors.kernel.org/fedora/development/rawhide/Everything/x86_64/os/Packages/g/ | awk '/>go-srpm-macros.*\.noarch\.rpm</ {print $2}' | sed -E 's/.*go-srpm-macros-(.*)\.noarch\.rpm.*/\1/')
REPO ?= $(notdir $(CURDIR))

IMAGE := $(shell latest=$$(docker images | awk "/$(REPO).*$(FEDORA_GOLANG)/ {print \$$2}"); if [ "$$latest" == $(FEDORA_GOLANG) ]; then true; else echo $(REPO)/$(FEDORA_GOLANG); fi)

RPM_OUT := $(dir $(dir $(CURDIR)))output-go-$(FEDORA_GOLANG)

$(FEDORA_GOLANG): $(IMAGE)

rpms: $(RPM_OUT)
	@echo "Copying built RPMs to $(RPM_OUT)..."
	@docker run --rm -v $(RPM_OUT):/out:z $(REPO):latest sh -c 'cp -a /root/rpmbuild/RPMS/* /out'
	@echo "Copied go-$(FEDORA_GOLANG) RPMs to $(RPM_OUT)"

$(RPM_OUT): $(IMAGE)
	@rm -fr $@ && mkdir -p $@

$(IMAGE): Dockerfile
	@echo "Building $(IMAGE) for $(FEDORA_GOLANG)"
	docker build -t $(subst /,:,$(IMAGE)) -t $(REPO):latest --build-arg=go_version=$(FEDORA_GOLANG) --build-arg=go_macros_version=$(FEDORA_GOLANG_MACROS) $(BUILD_VARS) .

clean:
	docker rmi $(subst /,:,$(IMAGE)) $(REPO):latest

.PHONY: $(FEDORA_GOLANG) $(IMAGE)
