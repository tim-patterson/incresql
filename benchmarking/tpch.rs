use runtime::Runtime;
use std::error::Error;
use std::process::Command;

#[cfg(not(windows))]
use jemallocator::Jemalloc;
use mysql::prelude::Queryable;
use mysql::Conn;
use server::Server;
use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn Error>> {
    let listen_address = "0.0.0.0:3308";
    let client_url = "mysql://root:password@localhost:3308";
    let path = "target/benchmark_db";
    build_test_data()?;
    reset_database(path)?;
    let server_thread = start_server(listen_address, path);
    std::thread::sleep(Duration::from_secs(1));
    eprintln!("Creating connection");
    let mut mysql_connection = mysql::Conn::new(client_url)?;
    create_tables(&mut mysql_connection)?;
    load_tables(&mut mysql_connection)?;
    eprintln!("Done");
    std::mem::drop(server_thread);
    Ok(())
}

fn build_test_data() -> Result<(), Box<dyn Error>> {
    let current_dir = std::env::current_dir().unwrap();
    let dbgendata_dir = current_dir.join("target").join("dbgen_s1");

    if dbgendata_dir.is_dir() {
        eprintln!("dbgen files already exist :)");
        return Ok(());
    }

    eprintln!("Building dbgen");
    Command::new("docker")
        .args(&["build", "--tag", "dbgen", "benchmarking/docker/dbgen"])
        .spawn()?
        .wait()?;

    eprintln!("Running dbgen");
    std::fs::create_dir_all(&dbgendata_dir).unwrap();
    let datadir_str = dbgendata_dir.as_os_str().to_str().unwrap();
    Command::new("docker")
        .args(&[
            "run",
            "-w",
            "/data",
            "-v",
            &format!("{}:/data", datadir_str),
            "dbgen",
            "/tpch-dbgen/dbgen",
            "-s",
            "1",
            "-f",
            "-b",
            "/tpch-dbgen/dists.dss",
        ])
        .spawn()?
        .wait()
        .expect("Error running dbgen");
    Ok(())
}

fn reset_database(path: &str) -> Result<(), Box<dyn Error>> {
    eprintln!("Resetting database");
    if PathBuf::from(path).exists() {
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn start_server(listen_address: &str, path: &str) -> Result<JoinHandle<()>, Box<dyn Error>> {
    eprintln!("Initializing Runtime");
    let runtime = Runtime::new(path)?;
    eprintln!("Initializing Server");
    let mut server = Server::new(runtime);
    let address = listen_address.to_string();
    Ok(std::thread::spawn(move || {
        eprintln!("Server Running");
        server.listen(&address).unwrap();
    }))
}

fn create_tables(connection: &mut Conn) -> Result<(), Box<dyn Error>> {
    eprintln!("Creating schema/tables");
    connection.query_drop("create database tpch_1")?;
    connection.query_drop(
        "\
CREATE TABLE tpch_1.part
(
    p_partkey       BIGINT,
    p_name          TEXT,
    p_mfgr          TEXT,
    p_brand         TEXT,
    p_type          TEXT,
    p_size          INTEGER,
    p_container     TEXT,
    p_retailprice   DECIMAL(12,4),
    p_comment       TEXT
)
    ",
    )?;

    connection.query_drop(
        "\
CREATE TABLE tpch_1.supplier
(
    s_suppkey     BIGINT,
    s_name        TEXT,
    s_address     TEXT,
    s_nationkey   INTEGER,
    s_phone       TEXT,
    s_acctbal     DECIMAL(12,4),
    s_comment     TEXT
)
    ",
    )?;

    connection.query_drop(
        "\
CREATE TABLE tpch_1.partsupp
(
    ps_partkey     BIGINT,
    ps_suppkey     BIGINT,
    ps_availqty    INTEGER,
    ps_supplycost  DECIMAL(12,4),
    ps_comment     TEXT
)
    ",
    )?;

    connection.query_drop(
        "\
CREATE TABLE tpch_1.customer
(
    c_custkey    BIGINT,
    c_name       TEXT,
    c_address    TEXT,
    c_nationkey  INTEGER,
    c_phone      TEXT,
    c_acctbal    DECIMAL(12,4),
    c_mkcsegment TEXT,
    c_comment    TEXT
)
    ",
    )?;

    connection.query_drop(
        "\
CREATE TABLE tpch_1.orders
(
    o_orderkey       BIGINT,
    o_custkey        BIGINT,
    o_orderstatus    TEXT,
    o_totalprice     DECIMAL(12,4),
    o_orderpriority  TEXT,
    o_clerk          TEXT,
    o_shippriority   INTEGER,
    o_comment        TEXT
)
    ",
    )?;

    connection.query_drop(
        "\
CREATE TABLE tpch_1.lineitem
(
    l_orderkey       BIGINT,
    l_partkey        BIGINT,
    l_suppkey        BIGINT,
    l_linenumber     INTEGER,
    l_quantity       DECIMAL(12,4),
    l_extendedprice  DECIMAL(12,4),
    l_discount       DECIMAL(12,4),
    l_tax            DECIMAL(12,4),
    l_returnflag     BOOLEAN,
    l_linestatus     BOOLEAN,
    l_shipdate       DATE,
    l_commitdate     DATE,
    l_receiptdate    DATE,
    l_shipinstruct   TEXT,
    l_shipmode       TEXT,
    l_comment        TEXT
)
    ",
    )?;

    connection.query_drop(
        "\
CREATE TABLE tpch_1.nation
(
    n_nationkey   INTEGER,
    n_name        TEXT,
    n_regionkey   INTEGER,
    n_comment     TEXT
)
    ",
    )?;

    connection.query_drop(
        "\
CREATE TABLE tpch_1.region
(
    r_regionkey   INTEGER,
    r_name        TEXT,
    r_comment     TEXT
)
    ",
    )?;

    Ok(())
}

fn load_tables(connection: &mut Conn) -> Result<(), Box<dyn Error>> {
    eprintln!("Loading lineitem");
    let start = Instant::now();
    connection.query_drop(
        r#"
INSERT INTO tpch_1.lineitem
SELECT
  CAST(data->>"$[0]" AS BIGINT) as l_orderkey,
  CAST(data->>"$[1]" AS BIGINT) as l_partkey,
  CAST(data->>"$[2]" AS BIGINT) as l_suppkey,
  CAST(data->>"$[3]" AS INTEGER) as l_linenumber,
  CAST(data->>"$[4]" AS DECIMAL(12,4)) as l_quantity,
  CAST(data->>"$[5]" AS DECIMAL(12,4)) as l_extendedprice,
  CAST(data->>"$[6]" AS DECIMAL(12,4)) as l_discount,
  CAST(data->>"$[7]" AS DECIMAL(12,4)) as l_tax,
  CAST(data->>"$[8]" AS BOOLEAN) as l_returnflag,
  CAST(data->>"$[9]" AS BOOLEAN) as l_linestatus,
  CAST(data->>"$[10]" AS DATE) as l_shipdate,
  CAST(data->>"$[11]" AS DATE) as l_commitdate,
  CAST(data->>"$[12]" AS DATE) as l_receiptdate,
  data->>"$[13]" as l_shipinstruct,
  data->>"$[14]" as l_shipmode,
  data->>"$[15]" as l_comment
FROM directory "target/dbgen_s1/lineitem.tbl" with(delimiter="|")
    "#,
    )?;
    println!("lineitems load in {:?}", start.elapsed());
    Ok(())
}
