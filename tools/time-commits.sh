#!/bin/bash
commit1="${1:?Usage $0 <commit1> <commit2> <zarr> [<implementation>]}"
commit2="${2:?Usage $0 <commit1> <commit2> <zarr> [<implementation>]}"
zarrpath="${3:?Usage: $0 <commit1> <commit2> <zarr> [<implementation>]}"
implementation="${4:-fastio}"
hyperfine \
    -L commit "$commit1","$commit2" \
    -s 'git checkout {commit} && cargo build -r' \
    -w3 \
    -n '{commit}' "target/release/zarr-checksum-gallery $implementation $zarrpath"
