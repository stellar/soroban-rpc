#!/usr/bin/bash

version=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name=="soroban-rpc") | .version')
cargo package \
  --no-verify \
  --package soroban-rpc \
  --config "source.crates-io.replace-with = 'vendored-sources'" \
  --config "source.vendored-sources.directory = 'vendor'"
path="target/package/soroban-rpc-${version}.crate"
tar xvfz "$path" -C vendor/
# Crates in the vendor directory require a checksum file, but it doesn't
# matter if it is empty.
echo '{"files":{}}' > vendor/$name-$version/.cargo-checksum.json