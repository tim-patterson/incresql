# Incresql
![Build Status](https://github.com/incresql/incresql/workflows/Test/badge.svg)

A database supporting incremental updates of materialized views

### Building/Running from source
To build and/or run incresql from source you will first need the rust toolchain installed, the install
instructions can be found at https://rustup.rs/

Once this is done you can use the standard cargo commands to build/test/run.

```sh
  # Run Incresql(dev build)
  cargo run

  # Run Incresql(release build)
  cargo run --release

  # Test Incresql
  cargo test --all

  # Build Incresql
  cargo build --release
```