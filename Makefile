NAME = aiproductive.azurecr.io/raw-log-forwarder
VERSION = 0.1.0

GOFMT ?= gofmt "-s"
GO ?= go
GO_FLAGS := -ldflags "-X main.Version=$(VERSION)"
GOCOVER=$(GO) tool cover
TEST_OPTS := -covermode=count -coverprofile=coverage.out

DOCKER_FLAGS ?=

PACKAGES ?= $(shell $(GO) list ./...)
SOURCES ?= $(shell find . -name "*.go" -type f)


all: build


fmt:
	$(GOFMT) -w $(SOURCES)


vet:
	$(GO) vet $(PACKAGES)


GOOS ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')
GOARCH ?= amd64
.PHONY: build
build:
	$(GO) build $(GO_FLAGS) -buildmode=c-shared -o out_azblob_$(GOOS)_$(GOARCH).so $(SOURCES)


test:
	@$(GO) test $(TEST_OPTS) $(PACKAGES)
	@$(GOCOVER) -func=coverage.out
	@$(GOCOVER) -html=coverage.out


clean:
	rm -rf *.so *.h *~ coverage.out


image:
	docker build -f build/image/Dockerfile -t $(NAME):$(VERSION) --rm $(DOCKER_FLAGS) .
	docker push $(NAME):$(VERSION)


runtest:
	docker run -it --rm \
	  -v ${PWD}/configs:/fluent-bit/etc/ \
	  -p 2020:2020 \
	  $(NAME):$(VERSION)
