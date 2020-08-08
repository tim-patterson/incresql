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

### Developing
Before checking in all tests need to pass,
the code needs to be formatted and lints need to pass.
```sh
  cargo test --workspace --lib --bins
  cargo fmt --all
  cargo clippy --all
```

### Connecting
To connect to incresql first start it by running
```sh
  # Running from source
  cargo run --release

  # OR

  # Running from release
  ./incresql
```

And then in another terminal (tab) run(assuming you have the mysql client installed):
```sh
  mysql -h 127.0.0.1

  mysql> select 1+2;
```