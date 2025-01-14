#!/bin/bash
commit1="${1:?Usage $0 <commit1> <commit2> <zarr>}"
commit2="${2:?Usage $0 <commit1> <commit2> <zarr>}"
zarr="${3:?Usage: $0 <commit1> <commit2> <zarr>}"
hyperfine \
    -L commit "$commit1","$commit2" \
    -s 'git checkout {commit} && cargo build -r' \
    -w3 \
    -n '{commit}' "target/release/zarr-checksum-gallery fastio $zarrpath"
