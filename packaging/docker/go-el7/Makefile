FEDORA_GOLANG ?= $(shell curl -s http://mirrors.kernel.org/fedora/development/rawhide/Everything/source/tree/Packages/g/ | awk '/>golang-1.*\.src\.rpm</ {print $2}' | sed -E 's/.*golang-(1.*)\.src\.rpm.*/\1/')
REPO ?= $(notdir $(CURDIR))

IMAGE := $(shell latest=$$(docker images | awk "/$(REPO).*$(FEDORA_GOLANG)/ {print \$$2}"); if [ "$$latest" == $(FEDORA_GOLANG) ]; then true; else echo $(REPO)/$(FEDORA_GOLANG); fi)

$(FEDORA_GOLANG): $(IMAGE)

$(IMAGE): Dockerfile
	@echo "Building $(IMAGE) for $(FEDORA_GOLANG)"
	docker build -t $(subst /,:,$(IMAGE)) -t $(REPO):latest --build-arg=go_version=$(FEDORA_GOLANG) $(BUILD_VARS) .

clean:
	docker rmi $(subst /,:,$(IMAGE)) $(REPO):latest

.PHONY: $(FEDORA_GOLANG) $(IMAGE)
