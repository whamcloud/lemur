REPO ?= $(notdir $(CURDIR))

VERSION := 1
IMAGE := $(shell latest=$$(docker images | awk "/$(REPO).*$(VERSION)/ {print \$$2}"); if [ "$$latest" == $(VERSION) ]; then true; else echo $(REPO)/$(VERSION); fi)

$(VERSION): $(IMAGE)

$(IMAGE): Dockerfile lemur.spec
	@echo "Building $(IMAGE)"
	docker build -t $(subst /,:,$(IMAGE)) -t $(REPO):latest $(BUILD_VARS) .

lemur.spec: ../../rpm/lemur.spec
	cp -a ../../rpm/lemur.spec .

clean:
	rm -f lemur.spec
	docker rmi $(REPO):latest $(subst /,:,$(IMAGE))

.PHONY: $(VERSION) $(IMAGE)
