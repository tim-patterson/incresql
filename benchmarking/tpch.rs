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
CREATE TABLE tpch_1.lineitem
(
    l_orderkey    BIGINT,
    l_partkey     BIGINT,
    l_suppkey     BIGINT,
    l_linenumber  BIGINT,
    l_quantity    DECIMAL(10,4),
    l_extendedprice  DECIMAL(10,4),
    l_discount    DECIMAL(10,4),
    l_tax         DECIMAL(10,4),
    l_returnflag  BOOLEAN,
    l_linestatus  BOOLEAN,
    l_shipdate    TEXT,
    l_commitdate  TEXT,
    l_receiptdate TEXT,
    l_shipinstruct TEXT,
    l_shipmode     TEXT,
    l_comment      TEXT
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
  CAST(data->>"$[3]" AS BIGINT) as l_linenumber,
  CAST(data->>"$[4]" AS DECIMAL(10,4)) as l_quantity,
  CAST(data->>"$[5]" AS DECIMAL(10,4)) as l_extendedprice,
  CAST(data->>"$[6]" AS DECIMAL(10,4)) as l_discount,
  CAST(data->>"$[7]" AS DECIMAL(10,4)) as l_tax,
  CAST(data->>"$[8]" AS BOOLEAN) as l_returnflag,
  CAST(data->>"$[9]" AS BOOLEAN) as l_linestatus,
  data->>"$[10]" as l_shipdate,
  data->>"$[11]" as l_commitdate,
  data->>"$[12]" as l_receiptdate,
  data->>"$[13]" as l_shipinstruct,
  data->>"$[14]" as l_shipmode,
  data->>"$[15]" as l_comment
FROM directory "target/dbgen_s1/lineitem.tbl" with(delimiter="|")
    "#,
    )?;
    println!("lineitems load in {:?}", start.elapsed());
    Ok(())
}
