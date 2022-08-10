[![Project Status: Concept – Minimal or no implementation has been done yet, or the repository is only intended to be a limited example, demo, or proof-of-concept.](https://www.repostatus.org/badges/latest/concept.svg)](https://www.repostatus.org/#concept)
[![CI Status](https://github.com/jwodder/zarr-checksum-gallery/actions/workflows/test.yml/badge.svg)](https://github.com/jwodder/zarr-checksum-gallery/actions/workflows/test.yml)
[![codecov.io](https://codecov.io/gh/jwodder/zarr-checksum-gallery/branch/master/graph/badge.svg)](https://codecov.io/gh/jwodder/zarr-checksum-gallery)
[![MIT License](https://img.shields.io/github/license/jwodder/zarr-checksum-gallery.svg)](https://opensource.org/licenses/MIT)

This is a Rust library & binary featuring a collection of various different
ways to implement a Merkle tree hash for a directory tree in the [format][1]
used by [the DANDI project](https://github.com/dandi) for Zarr assets.  It was
written partly in search of the most efficient implementation but mostly as
just an exercise in Rust.

[1]: https://github.com/dandi/dandi-archive/blob/master/doc/design/zarr-support-3.md#zarr-entry-checksum-format

Installation
============

Regardless of which installation method you choose, you need to first [install
Rust and Cargo](https://www.rust-lang.org/tools/install).

To install the `zarr-checksum-gallery` binary in `~/.cargo/bin`, run:

    cargo install --git https://github.com/jwodder/zarr-checksum-gallery

Alternatively, a binary localized to a clone of this repository can be built
with:

    git clone https://github.com/jwodder/zarr-checksum-gallery
    cd zarr-checksum-gallery
    cargo build  # or `cargo build --release` to enable optimizations
    # You can now run the binary with `cargo run -- <args>` while in this
    # repository.


Usage
=====

    zarr-checksum-gallery [<global options>] <implementation> [<options>] <dirpath>

`zarr-checksum-gallery` computes the Zarr checksum for the directory at
`<dirpath>` using the given `<implementation>` (See list below).  Regardless of
the implementation chosen, the checksum should always be the same for the same
directory contents & layout; if it is not, it is a bug.

Global Options
--------------

- `--debug` — Show DEBUG log messages listing the checksum for each file &
  directory as it's computed.

- `--trace` — Show TRACE log messages in addition to DEBUG messages.  Not all
  implementations emit TRACE logs.

Implementations
---------------

- `breadth-first` — Walk the directory tree iteratively & breadth-first,
  building a tree of file checksums in memory

- `depth-first` — Walk the directory tree iteratively & depth-first, computing
  the checksum for each directory as soon as possible

- `fastasync` — Walk the directory tree using multiple asynchronous worker
  tasks, building a tree of file checksums in memory

  **Options:**

    - `-w <NUM>`/`--workers <NUM>` — Set the number of worker tasks to use.
      The default value is the number of logical CPU cores on the machine.

- `fastio` — Walk the directory tree using multiple threads, building a tree of
  file checksums in memory

  **Options:**

    - `-t <NUM>`/`--threads <NUM>` — Set the number of threads to use.  The
      default value is the number of logical CPU cores on the machine.

- `recursive` — Walk the directory tree recursively and depth-first, computing
  the checksum for each directory as soon as possible

- `walkdir` — Walk the directory tree using [the `walkdir`
  crate](https://crates.io/crates/walkdir), building a tree of file checksums
  in memory
