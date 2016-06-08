REPO ?= $(notdir $(CURDIR))

HOST_KERNEL_VERSION = $(shell docker run --rm go-el7 uname -r)
IMAGE = $(shell latest=$$(docker images | awk "/$(REPO).*$(HOST_KERNEL_VERSION)/ {print \$$2}"); if [ "$$latest" == $(HOST_KERNEL_VERSION) ]; then true; else echo $(REPO)/$(HOST_KERNEL_VERSION); fi)

$(HOST_KERNEL_VERSION): $(IMAGE)

$(IMAGE): Dockerfile
	@echo "Building $(IMAGE) for $(HOST_KERNEL_VERSION)"
	docker build -t $(subst /,:,$(IMAGE)) -t $(REPO):latest -t host-kernel:latest $(BUILD_VARS) .
