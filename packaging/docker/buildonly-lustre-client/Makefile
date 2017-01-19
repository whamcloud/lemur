
REPO ?= $(notdir $(CURDIR))

BUILDER_URL ?= https://build.hpdd.intel.com
LUSTRE_JOB ?= lustre-b2_8
LUSTRE_BUILD ?= lastSuccessfulBuild
CLIENT_PACKAGE ?= lustre-client

PACKAGE_URL := $(BUILDER_URL)/job/$(LUSTRE_JOB)/arch=x86_64,build_type=client,distro=el7,ib_stack=inkernel/$(LUSTRE_BUILD)
CLIENT_VERSION := $(shell curl -sf $(PACKAGE_URL)/api/json | python -c 'import sys, json, re; pkg=[a for a in json.load(sys.stdin)["artifacts"] if re.search(r"$(CLIENT_PACKAGE)-\d+.*\.rpm", a["fileName"])][0]["fileName"]; print(re.sub(r"$(CLIENT_PACKAGE)-(.*)\.x86_64\.x86_64\.rpm",r"\1",pkg))')
IMAGE := $(shell latest=$$(docker images | awk "/$(REPO).*$(CLIENT_VERSION)/ {print \$$2}"); if [ "$$latest" == $(CLIENT_VERSION) ]; then true; else echo $(REPO)/$(CLIENT_VERSION); fi)

$(CLIENT_VERSION): $(IMAGE)

$(IMAGE): Dockerfile
	@echo "Building $(IMAGE) for $(CLIENT_VERSION)"
	docker build -t $(subst /,:,$(IMAGE)) -t $(REPO):latest -t lustre-client:latest --build-arg=package_url=$(PACKAGE_URL)/artifact/artifacts/ $(BUILD_VARS) .

clean:
	docker rmi $(subst /,:,$(IMAGE)) $(REPO):latest lustre-client:latest

.PHONY: $(CLIENT_VERSION) $(IMAGE)
