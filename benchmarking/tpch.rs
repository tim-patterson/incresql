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
    p_retailprice   DECIMAL(12,2),
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
    s_acctbal     DECIMAL(12,2),
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
    ps_supplycost  DECIMAL(12,2),
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
    c_acctbal    DECIMAL(12,2),
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
    o_totalprice     DECIMAL(12,2),
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
    l_quantity       DECIMAL(12,2),
    l_extendedprice  DECIMAL(12,2),
    l_discount       DECIMAL(12,2),
    l_tax            DECIMAL(12,2),
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
    load_table(
        connection,
        r#"
INSERT INTO tpch_1.part
SELECT
  CAST(data->>"$[0]" AS BIGINT) as p_partkey,
  data->>"$[1]" as p_name,
  data->>"$[2]" as p_mfgr,
  data->>"$[3]" as p_brand,
  data->>"$[4]" as p_type,
  CAST(data->>"$[5]" AS INTEGER) as p_size,
  data->>"$[6]" as p_container,
  CAST(data->>"$[7]" AS DECIMAL(12,2)) as p_retailprice,
  data->>"$[8]" as p_comment
FROM directory "target/dbgen_s1/part.tbl" with(delimiter="|")
    "#,
        "part",
    )?;

    load_table(
        connection,
        r#"
INSERT INTO tpch_1.supplier
SELECT
  CAST(data->>"$[0]" AS BIGINT) as s_suppkey,
  data->>"$[1]" as s_name,
  data->>"$[2]" as s_address,
  CAST(data->>"$[3]" as INTEGER) as s_nationkey,
  data->>"$[4]" as s_phone,
  CAST(data->>"$[5]" AS DECIMAL(12,2)) as s_acctbal,
  data->>"$[6]" as s_comment
FROM directory "target/dbgen_s1/supplier.tbl" with(delimiter="|")
    "#,
        "supplier",
    )?;

    load_table(
        connection,
        r#"
INSERT INTO tpch_1.partsupp
SELECT
  CAST(data->>"$[0]" AS BIGINT) as ps_partkey,
  CAST(data->>"$[1]" AS BIGINT) as ps_suppkey,
  CAST(data->>"$[2]" as INTEGER) as ps_availqty,
  CAST(data->>"$[3]" as DECIMAL(12,2)) as ps_supplycost,
  data->>"$[4]" as ps_comment
FROM directory "target/dbgen_s1/partsupp.tbl" with(delimiter="|")
    "#,
        "partsupp",
    )?;

    load_table(
        connection,
        r#"
INSERT INTO tpch_1.customer
SELECT
  CAST(data->>"$[0]" AS BIGINT) as c_custkey,
  data->>"$[1]" as c_name,
  data->>"$[2]" as c_address,
  CAST(data->>"$[3]" AS INTEGER) as c_nationkey,
  data->>"$[4]" as c_phone,
  CAST(data->>"$[5]" AS DECIMAL(12,2)) as c_acctbal,
  data->>"$[6]" as c_mkcsegment,
  data->>"$[7]" as c_comment
FROM directory "target/dbgen_s1/customer.tbl" with(delimiter="|")
    "#,
        "customer",
    )?;

    load_table(
        connection,
        r#"
INSERT INTO tpch_1.orders
SELECT
  CAST(data->>"$[0]" AS BIGINT) as o_orderkey,
  CAST(data->>"$[1]" AS BIGINT) as o_custkey,
  data->>"$[2]" as o_orderstatus,
  CAST(data->>"$[3]" AS DECIMAL(12,2)) as o_totalprice,
  data->>"$[4]" as o_orderpriority,
  data->>"$[5]" as o_clerk,
  CAST(data->>"$[6]" AS INTEGER) as o_shippriority,
  data->>"$[7]" as o_comment
FROM directory "target/dbgen_s1/orders.tbl" with(delimiter="|")
    "#,
        "orders",
    )?;

    load_table(
        connection,
        r#"
INSERT INTO tpch_1.lineitem
SELECT
  CAST(data->>"$[0]" AS BIGINT) as l_orderkey,
  CAST(data->>"$[1]" AS BIGINT) as l_partkey,
  CAST(data->>"$[2]" AS BIGINT) as l_suppkey,
  CAST(data->>"$[3]" AS INTEGER) as l_linenumber,
  CAST(data->>"$[4]" AS DECIMAL(12,2)) as l_quantity,
  CAST(data->>"$[5]" AS DECIMAL(12,2)) as l_extendedprice,
  CAST(data->>"$[6]" AS DECIMAL(12,2)) as l_discount,
  CAST(data->>"$[7]" AS DECIMAL(12,2)) as l_tax,
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
        "lineitem",
    )?;

    load_table(
        connection,
        r#"
INSERT INTO tpch_1.nation
SELECT
  CAST(data->>"$[0]" AS INTEGER) as n_nationkey,
  data->>"$[1]" as n_name,
  CAST(data->>"$[2]" AS INTEGER) as n_regionkey,
  data->>"$[3]" as n_comment
FROM directory "target/dbgen_s1/nation.tbl" with(delimiter="|")
    "#,
        "nation",
    )?;

    load_table(
        connection,
        r#"
INSERT INTO tpch_1.region
SELECT
  CAST(data->>"$[0]" AS INTEGER) as r_regionkey,
  data->>"$[1]" as r_name,
  data->>"$[2]" as r_comment
FROM directory "target/dbgen_s1/region.tbl" with(delimiter="|")
    "#,
        "region",
    )?;
    Ok(())
}

fn load_table(
    connection: &mut Conn,
    load_script: &str,
    table_name: &str,
) -> Result<(), Box<dyn Error>> {
    eprintln!("Loading {}", table_name);
    let start = Instant::now();
    connection.query_drop(load_script)?;
    println!("  {} load in {:?}", table_name, start.elapsed());

    let compaction_start = Instant::now();
    connection.query_drop(&format!("COMPACT TABLE tpch_1.{}", table_name))?;
    println!("  compaction in {:?}", compaction_start.elapsed());
    println!("  {} total_load in  {:?}", table_name, start.elapsed());
    Ok(())
}
