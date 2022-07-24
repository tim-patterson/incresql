# Incresql
![Build Status](https://github.com/incresql/incresql/workflows/Test/badge.svg)

A database supporting incremental updates of materialized views

### Goals
The goal of the project is to explore and experiment with the incremental materialized views space.

Incremental materialized views can be thought of as fulfilling the same use cases as ksqlDB but with more standard sql in
something that feels more like a standard relational database.

The use case's in mind are BI dashboarding/reporting as well as being able to support serving per-user data to user facing apps/services.

### Project Status
Basic functionality is working with groupbys, joins etc.
Project is abandoned, lessons have been learned...

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
  cargo test --workspace --tests

  # Build Incresql
  cargo build --release
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
  mysql -h 127.0.0.1 -P3307

  mysql> select 1+2;
```

### Developing
Before checking in all tests need to pass,
the code needs to be formatted and lints need to pass.
```sh
  cargo test --workspace --tests
  cargo fmt --all
  cargo clippy --all
```

### Benchmarking
To benchmark run the following command
```sh
  cargo run --release --example tpch

  # Or with a larger scale factor
  cargo run --release --example tpch -- -s 10

  # Or to skip the reset/loading phase and rerun with
  # the tables loaded from the previous run
  cargo run --release --example tpch -- --skipload
```

To manually run queries against the loaded benchmarking tables, run the following

```sh
cargo run  --release -- --directory target/benchmark_db
```

### Related Work
Other similar projects are
* https://github.com/mit-pdos/noria - More academic in nature, focusing more on traditional transactional apps used prepared statements.
* https://github.com/MaterializeInc/materialize - Similar goals but is an in-memory system rather than leaning on rocksdb.

