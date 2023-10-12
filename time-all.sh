#!/bin/bash
set -e

cmd=target/release/zarr-checksum-gallery
zarr="${1:?Usage: $0 <zarr>}"

cargo build -r

hyperfine \
    -w3 \
    -n breadth-first "$cmd breadth-first $zarr" \
    -n collapsio-arc "$cmd collapsio-arc ${ZARR_THREADS:+--threads $ZARR_THREADS} $zarr" \
    -n collapsio-mpsc "$cmd collapsio-mpsc ${ZARR_THREADS:+--threads $ZARR_THREADS} $zarr" \
    -n depth-first "$cmd depth-first $zarr" \
    -n fastasync "$cmd fastasync ${ZARR_ASYNC_THREADS:+--threads $ZARR_ASYNC_THREADS} ${ZARR_WORKERS:+--workers $ZARR_WORKERS} $zarr" \
    -n fastio "$cmd fastio ${ZARR_THREADS:+--threads $ZARR_THREADS} $zarr" \
    -n recursive "$cmd recursive $zarr"
