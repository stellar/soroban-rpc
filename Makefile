all: check build test

export RUSTFLAGS=-Dwarnings -Dclippy::all -Dclippy::pedantic

REPOSITORY_COMMIT_HASH := "$(shell git rev-parse HEAD)"
ifeq (${REPOSITORY_COMMIT_HASH},"")
	$(error failed to retrieve git head commit hash)
endif
# Want to treat empty assignment, `REPOSITORY_VERSION=` the same as absence or unset.
# By default make `?=` operator will treat empty assignment as a set value and will not use the default value.
# Both cases should fallback to default of getting the version from git tag.
ifeq ($(strip $(REPOSITORY_VERSION)),)
	override REPOSITORY_VERSION = "$(shell git describe --tags --always --abbrev=0 --match='v[0-9]*.[0-9]*.[0-9]*' 2> /dev/null | sed 's/^.//')"
endif
REPOSITORY_BRANCH := "$(shell git rev-parse --abbrev-ref HEAD)"
BUILD_TIMESTAMP ?= $(shell date '+%Y-%m-%dT%H:%M:%S')
GOLDFLAGS :=	-X 'github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config.Version=${REPOSITORY_VERSION}' \
				-X 'github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config.CommitHash=${REPOSITORY_COMMIT_HASH}' \
				-X 'github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config.BuildTimestamp=${BUILD_TIMESTAMP}' \
				-X 'github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config.Branch=${REPOSITORY_BRANCH}'


# The following works around incompatibility between the rust and the go linkers -
# the rust would generate an object file with min-version of 13.0 where-as the go
# compiler would generate a binary compatible with 12.3 and up. To align these
# we instruct the go compiler to produce binaries comparible with version 13.0.
# this is a mac-only limitation.
ifeq ($(shell uname -s),Darwin)
	MACOS_MIN_VER = -ldflags='-extldflags -mmacosx-version-min=13.0'
endif

# Always specify the build target so that libpreflight.a is always put into
# an architecture subdirectory (i.e. target/$(CARGO_BUILD_TARGET)/release-with-panic-unwind )
# Otherwise it will be much harder for Golang to find the library since
# it would need to distinguish when we are crosscompiling and when we are not
# (libpreflight.a is put at target/release-with-panic-unwind/ when not cross compiling)
CARGO_BUILD_TARGET ?= $(shell rustc -vV | sed -n 's|host: ||p')

SOROBAN_RPC_BINARY := soroban-rpc
STELLAR_RPC_BINARY := stellar-rpc


# update the Cargo.lock every time the Cargo.toml changes.
Cargo.lock: Cargo.toml
	cargo update --workspace

install: build-libs
	go install -ldflags="${GOLDFLAGS}" ${MACOS_MIN_VER} ./...

build: build-libs
	go build -ldflags="${GOLDFLAGS}" ${MACOS_MIN_VER} ./...

build-libs: Cargo.lock
	cd cmd/soroban-rpc/lib/preflight && \
	cargo build --target $(CARGO_BUILD_TARGET) --profile release-with-panic-unwind && \
	cd ../xdr2json && \
	cargo build --target $(CARGO_BUILD_TARGET) --profile release-with-panic-unwind

check: rust-check go-check

rust-check: Cargo.lock
	cargo fmt --all --check
	cargo clippy

watch:
	cargo watch --clear --watch-when-idle --shell '$(MAKE)'

fmt:
	go fmt ./...
	cargo fmt --all

rust-test:
	cargo test

go-test: build-libs
	go test ./...

test: go-test rust-test

clean:
	cargo clean
	go clean ./...

# DEPRECATED - please use build-stellar-rpc instead
# the build-soroban-rpc build target is an optimized build target used by
# https://github.com/stellar/pipelines/blob/master/soroban-rpc/Jenkinsfile-soroban-rpc-package-builder
# as part of the package building.
build-soroban-rpc: build-libs
	go build -ldflags="${GOLDFLAGS}" ${MACOS_MIN_VER} -o ${SOROBAN_RPC_BINARY} -trimpath -v ./cmd/soroban-rpc

# the build-stellar-rpc build target is an optimized build target used by
# https://github.com/stellar/pipelines/blob/master/soroban-rpc/Jenkinsfile-soroban-rpc-package-builder
# as part of the package building.
build-stellar-rpc: build-libs
	go build -ldflags="${GOLDFLAGS}" ${MACOS_MIN_VER} -o ${STELLAR_RPC_BINARY} -trimpath -v ./cmd/soroban-rpc


go-check-branch:
	golangci-lint run ./... --new-from-rev $$(git rev-parse origin/main)

go-check:
	golangci-lint run ./...


# PHONY lists all the targets that aren't file names, so that make would skip the timestamp based check.
.PHONY: clean fmt watch test rust-test go-test check rust-check go-check install build build-soroban-rpc build-libs lint lint-changes
