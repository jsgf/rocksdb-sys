RocksDB bindings
================

[![Build Status](https://travis-ci.org/jsgf/rocksdb-sys.svg?branch=master)](https://travis-ci.org/jsgf/rocksdb-sys)

This is low-level bindings to RocksDB's C API, as of 3.14.

By default RocksDB uses tcmalloc, which can interact badly with Rust's use of jemalloc/system
malloc, since tcmalloc won't get loaded until rocksdb is itself loaded. To avoid this conflict, this
crate builds rocksdb for itself, and uses static linking to avoid pulling in tcmalloc. However,
since the code will end up being part of a .so file, it needs to be compiled with -fPIC, which is an
additional build option.

If you want to use rocksdb's various compression options, then you must pre-install the appropriate
libraries (snappy, lz4, bzip2).
