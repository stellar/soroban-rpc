#!/bin/bash

set -e

SED=sed
if [ -z "$(sed --version 2>&1 | grep GNU)" ]; then
    SED=gsed
fi

CURL="curl -sL --fail-with-body"

# PROTOS is in ascending order so the last iteration of the PROTOS-based loops
# will end up with the highest protocol value used for recording any state
# variables.
PROTOS=$($SED -n ':pkg; /"soroban-env-host"/ {n; /version/ { s/[^0-9]*\([0-9]\+\).*/\1/ p; b pkg;}}' Cargo.toml | sort -n | tr '\n' ' ')
if [ -z "$PROTOS" ]; then
  echo "Cannot find soroban-env-host dependencies in Cargo.toml"
  exit 1
fi

for PROTO in $PROTOS
do
  if ! CARGO_OUTPUT=$(cargo tree -p soroban-env-host@$PROTO 2>&1); then
    echo "The project depends on multiple versions of the soroban-env-host@$PROTO Rust library, please unify them."
    echo
    echo
    echo "Full error:"
    echo $CARGO_OUTPUT
    exit 1
  fi
done

# revision of the https://github.com/stellar/rs-stellar-xdr library used by the Rust code
RS_STELLAR_XDR_REVISION=""

# revision of https://github.com/stellar/stellar-xdr/ used by the Rust code
STELLAR_XDR_REVISION_FROM_RUST=""

function stellar_xdr_version_from_rust_dep_tree {
  LINE=$(grep stellar-xdr | head -n 1)
  # try to obtain a commit
  COMMIT=$(echo $LINE | $SED -n 's/.*rev=\(.*\)#.*/\1/p')
  if [ -n "$COMMIT" ]; then
    echo "$COMMIT"
    return
  fi
  # obtain a crate version
  echo $LINE | $SED -n  's/.*stellar-xdr \(v\)\{0,1\}\([^ ]*\).*/\2/p'
}

for PROTO in $PROTOS
do
  if CARGO_OUTPUT=$(cargo tree --depth 0 -p stellar-xdr@$PROTO 2>&1); then
    RS_STELLAR_XDR_REVISION=$(echo "$CARGO_OUTPUT" | stellar_xdr_version_from_rust_dep_tree)
    if [ ${#RS_STELLAR_XDR_REVISION} -eq 40 ]; then
      # revision is a git hash
      STELLAR_XDR_REVISION_FROM_RUST=$($CURL https://raw.githubusercontent.com/stellar/rs-stellar-xdr/${RS_STELLAR_XDR_REVISION}/xdr/curr-version)
    else
      # revision is a crate version
      CARGO_SRC_BASE_DIR=$(realpath ${CARGO_HOME:-$HOME/.cargo}/registry/src/index*)
      STELLAR_XDR_REVISION_FROM_RUST=$(cat "${CARGO_SRC_BASE_DIR}/stellar-xdr-${RS_STELLAR_XDR_REVISION}/xdr/curr-version")
    fi
  else
    echo "The project depends on multiple versions of the Rust rs-stellar-xdr@$PROTO library"
    echo "Make sure a single version of stellar-xdr@$PROTO is used"
    echo
    echo
    echo
    echo "Full error:"
    echo $CARGO_OUTPUT
  fi
done

# Now, lets compare the Rust and Go XDR revisions
# TODO: The sed extraction below won't work for version tags
GO_XDR_REVISION=$(go list -m -f '{{.Version}}' github.com/stellar/go | $SED 's/.*-\(.*\)/\1/')

# revision of https://github.com/stellar/stellar-xdr/ used by the Go code
STELLAR_XDR_REVISION_FROM_GO=$($CURL https://raw.githubusercontent.com/stellar/go/${GO_XDR_REVISION}/xdr/xdr_commit_generated.txt)

if [ "$STELLAR_XDR_REVISION_FROM_GO" != "$STELLAR_XDR_REVISION_FROM_RUST" ]; then
  echo "Go and Rust dependencies are using different revisions of https://github.com/stellar/stellar-xdr"
  echo
  echo "Rust dependencies are using commit $STELLAR_XDR_REVISION_FROM_RUST"
  echo "Go dependencies are using commit $STELLAR_XDR_REVISION_FROM_GO"
  exit 1
fi

# Now, lets make sure that the core and captive core version used in the tests use the same version and that they depend
# on the same XDR revision

PROTOCOL_VERSIONS=$(grep 'protocol-version: ' .github/workflows/stellar-rpc.yml | grep -o -E '[0-9]+')

for P in $PROTOCOL_VERSIONS; do
    CORE_CONTAINER_REVISION=$($SED -n 's/.*PROTOCOL_'$P'_CORE_DOCKER_IMG:.*\/\(stellar-core\|unsafe-stellar-core\(-next\)\{0,1\}\)\:.*\.\([a-zA-Z0-9]*\)\..*/\3/p' < .github/workflows/stellar-rpc.yml)
    CAPTIVE_CORE_PKG_REVISION=$($SED -n 's/.*PROTOCOL_'$P'_CORE_DEBIAN_PKG_VERSION:.*\.\([a-zA-Z0-9]*\)\..*/\1/p' < .github/workflows/stellar-rpc.yml)

    if [ "$CORE_CONTAINER_REVISION" != "$CAPTIVE_CORE_PKG_REVISION" ]; then
	    echo "Stellar RPC protocol $P integration tests are using different versions of the Core container and Captive Core Debian package."
	    echo
	    echo "Core container image commit $CORE_CONTAINER_REVISION"
	    echo "Captive core debian package commit $CAPTIVE_CORE_PKG_REVISION"
	    exit 1
    fi

    # Revision of https://github.com/stellar/rs-stellar-xdr by Core.
    # We obtain it from src/rust/src/host-dep-tree-curr.txt but Alternatively/in addition we could:
    #  * Check the rs-stellar-xdr revision of host-dep-tree-prev.txt
    #  * Check the stellar-xdr revision

    # FIXME: we shouldn't hardcode the protocol number in the file being checked
    CORE_HOST_DEP_TREE_CURR=$($CURL https://raw.githubusercontent.com/stellar/stellar-core/${CORE_CONTAINER_REVISION}/src/rust/src/dep-trees/p22-expect.txt)


    RS_STELLAR_XDR_REVISION_FROM_CORE=$(echo "$CORE_HOST_DEP_TREE_CURR" | stellar_xdr_version_from_rust_dep_tree)
    if [ "$RS_STELLAR_XDR_REVISION" != "$RS_STELLAR_XDR_REVISION_FROM_CORE" ]; then
	    echo "The Core revision used in protocol $P integration tests (${CORE_CONTAINER_REVISION}) uses a different revision of https://github.com/stellar/rs-stellar-xdr"
	    echo
	    echo "Current repository's revision $RS_STELLAR_XDR_REVISION"
	    echo "Core's revision $RS_STELLAR_XDR_REVISION_FROM_CORE"
    fi
done

