#!/bin/bash
set -e
dirpath="${1:?Usage: $0 <dirpath>}"
cargo build -r
hyperfine \
    -L impl breadth-first,depth-first,fastasync,fastio,recursive,walkdir \
    -w3 \
    -n '{impl}' "target/release/zarr-checksum-gallery {impl} $dirpath"
