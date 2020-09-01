use super::BenchmarkRunner;
use mysql::prelude::Queryable;
use mysql::Conn;
use runtime::Runtime;
use server::Server;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

pub struct IncresqlRunner {
    s: u8,
    mysql_connection: Conn,
}

impl IncresqlRunner {
    pub fn new(s: u8, reset: bool) -> Result<Self, Box<dyn Error>> {
        let listen_address = "0.0.0.0:3308";
        let client_url = "mysql://root:password@localhost:3308";
        let path = "target/benchmark_db";

        if reset {
            eprintln!("Resetting database");
            if PathBuf::from(path).exists() {
                std::fs::remove_dir_all(path)?;
            }
        }

        eprintln!("Initializing Runtime");
        let runtime = Runtime::new(path)?;
        eprintln!("Initializing Server");
        let mut server = Server::new(runtime);
        let address = listen_address.to_string();
        std::thread::spawn(move || {
            eprintln!("Server Running");
            server.listen(&address).unwrap();
        });

        let mysql_connection = mysql::Conn::new(client_url)?;

        Ok(IncresqlRunner {
            s,
            mysql_connection,
        })
    }
}

impl BenchmarkRunner for IncresqlRunner {
    fn create_tables(&mut self) -> Result<(), Box<dyn Error>> {
        eprintln!("Creating schema/tables");
        self.mysql_connection
            .query_drop(format!("create database tpch_{}", self.s))?;
        self.mysql_connection
            .query_drop(format!("use tpch_{}", self.s))?;
        self.mysql_connection.query_drop(
            "\
CREATE TABLE part
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

        self.mysql_connection.query_drop(
            "\
CREATE TABLE supplier
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

        self.mysql_connection.query_drop(
            "\
CREATE TABLE partsupp
(
    ps_partkey     BIGINT,
    ps_suppkey     BIGINT,
    ps_availqty    INTEGER,
    ps_supplycost  DECIMAL(12,2),
    ps_comment     TEXT
)
    ",
        )?;

        self.mysql_connection.query_drop(
            "\
CREATE TABLE customer
(
    c_custkey    BIGINT,
    c_name       TEXT,
    c_address    TEXT,
    c_nationkey  INTEGER,
    c_phone      TEXT,
    c_acctbal    DECIMAL(12,2),
    c_mktsegment TEXT,
    c_comment    TEXT
)
    ",
        )?;

        self.mysql_connection.query_drop(
            "\
CREATE TABLE orders
(
    o_orderkey       BIGINT,
    o_custkey        BIGINT,
    o_orderstatus    TEXT,
    o_totalprice     DECIMAL(12,2),
    o_orderdate      DATE,
    o_orderpriority  TEXT,
    o_clerk          TEXT,
    o_shippriority   INTEGER,
    o_comment        TEXT
)
    ",
        )?;

        self.mysql_connection.query_drop(
            "\
CREATE TABLE lineitem
(
    l_orderkey       BIGINT,
    l_partkey        BIGINT,
    l_suppkey        BIGINT,
    l_linenumber     INTEGER,
    l_quantity       DECIMAL(12,2),
    l_extendedprice  DECIMAL(12,2),
    l_discount       DECIMAL(12,2),
    l_tax            DECIMAL(12,2),
    l_returnflag     TEXT,
    l_linestatus     TEXT,
    l_shipdate       DATE,
    l_commitdate     DATE,
    l_receiptdate    DATE,
    l_shipinstruct   TEXT,
    l_shipmode       TEXT,
    l_comment        TEXT
)
    ",
        )?;

        self.mysql_connection.query_drop(
            "\
CREATE TABLE nation
(
    n_nationkey   INTEGER,
    n_name        TEXT,
    n_regionkey   INTEGER,
    n_comment     TEXT
)
    ",
        )?;

        self.mysql_connection.query_drop(
            "\
CREATE TABLE region
(
    r_regionkey   INTEGER,
    r_name        TEXT,
    r_comment     TEXT
)
    ",
        )?;

        Ok(())
    }

    fn load_tables(&mut self, data_dir: &str) -> Result<(), Box<dyn Error>> {
        self.mysql_connection
            .query_drop(format!("use tpch_{}", self.s))?;
        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO part
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
FROM directory "{}/part.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "part",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO supplier
SELECT
  CAST(data->>"$[0]" AS BIGINT) as s_suppkey,
  data->>"$[1]" as s_name,
  data->>"$[2]" as s_address,
  CAST(data->>"$[3]" as INTEGER) as s_nationkey,
  data->>"$[4]" as s_phone,
  CAST(data->>"$[5]" AS DECIMAL(12,2)) as s_acctbal,
  data->>"$[6]" as s_comment
FROM directory "{}/supplier.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "supplier",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO partsupp
SELECT
  CAST(data->>"$[0]" AS BIGINT) as ps_partkey,
  CAST(data->>"$[1]" AS BIGINT) as ps_suppkey,
  CAST(data->>"$[2]" as INTEGER) as ps_availqty,
  CAST(data->>"$[3]" as DECIMAL(12,2)) as ps_supplycost,
  data->>"$[4]" as ps_comment
FROM directory "{}/partsupp.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "partsupp",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO customer
SELECT
  CAST(data->>"$[0]" AS BIGINT) as c_custkey,
  data->>"$[1]" as c_name,
  data->>"$[2]" as c_address,
  CAST(data->>"$[3]" AS INTEGER) as c_nationkey,
  data->>"$[4]" as c_phone,
  CAST(data->>"$[5]" AS DECIMAL(12,2)) as c_acctbal,
  data->>"$[6]" as c_mktsegment,
  data->>"$[7]" as c_comment
FROM directory "{}/customer.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "customer",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO orders
SELECT
  CAST(data->>"$[0]" AS BIGINT) as o_orderkey,
  CAST(data->>"$[1]" AS BIGINT) as o_custkey,
  data->>"$[2]" as o_orderstatus,
  CAST(data->>"$[3]" AS DECIMAL(12,2)) as o_totalprice,
  CAST(data->>"$[4]" AS DATE) as o_orderdate,
  data->>"$[5]" as o_orderpriority,
  data->>"$[6]" as o_clerk,
  CAST(data->>"$[7]" AS INTEGER) as o_shippriority,
  data->>"$[8]" as o_comment
FROM directory "{}/orders.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "orders",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO lineitem
SELECT
  CAST(data->>"$[0]" AS BIGINT) as l_orderkey,
  CAST(data->>"$[1]" AS BIGINT) as l_partkey,
  CAST(data->>"$[2]" AS BIGINT) as l_suppkey,
  CAST(data->>"$[3]" AS INTEGER) as l_linenumber,
  CAST(data->>"$[4]" AS DECIMAL(12,2)) as l_quantity,
  CAST(data->>"$[5]" AS DECIMAL(12,2)) as l_extendedprice,
  CAST(data->>"$[6]" AS DECIMAL(12,2)) as l_discount,
  CAST(data->>"$[7]" AS DECIMAL(12,2)) as l_tax,
  data->>"$[8]" as l_returnflag,
  data->>"$[9]" as l_linestatus,
  CAST(data->>"$[10]" AS DATE) as l_shipdate,
  CAST(data->>"$[11]" AS DATE) as l_commitdate,
  CAST(data->>"$[12]" AS DATE) as l_receiptdate,
  data->>"$[13]" as l_shipinstruct,
  data->>"$[14]" as l_shipmode,
  data->>"$[15]" as l_comment
FROM directory "{}/lineitem.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "lineitem",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO nation
SELECT
  CAST(data->>"$[0]" AS INTEGER) as n_nationkey,
  data->>"$[1]" as n_name,
  CAST(data->>"$[2]" AS INTEGER) as n_regionkey,
  data->>"$[3]" as n_comment
FROM directory "{}/nation.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "nation",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
INSERT INTO region
SELECT
  CAST(data->>"$[0]" AS INTEGER) as r_regionkey,
  data->>"$[1]" as r_name,
  data->>"$[2]" as r_comment
FROM directory "{}/region.tbl" with(delimiter="|")
    "#,
                data_dir
            ),
            "region",
        )?;
        Ok(())
    }

    fn run_queries(&mut self) -> Result<(), Box<dyn Error>> {
        self.mysql_connection
            .query_drop(format!("use tpch_{}", self.s))?;
        run_query(
            &mut self.mysql_connection,
            "Query 1",
            r#"
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date_sub(date '1998-12-01', 90)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
    "#,
        )?;

        run_query(
            &mut self.mysql_connection,
            "Query 3",
            r#"
select
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from
  customer,
  orders,
  lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc,
  o_orderdate
limit 10
    "#,
        )?;

        run_query(
            &mut self.mysql_connection,
            "Query 5",
            r#"
select
  n_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue
from
  customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1995-01-01'
group by
  n_name
order by
  revenue desc
    "#,
        )?;

        run_query(
            &mut self.mysql_connection,
            "Query 6",
            r#"
select
  sum(l_extendedprice*l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1995-01-01'
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 24
    "#,
        )?;

        run_query(
            &mut self.mysql_connection,
            "Query 10",
            r#"
select
  c_custkey,
  c_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
from
  customer,
  orders,
  lineitem,
  nation
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01'
  and o_orderdate < date '1994-01-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
order by
  revenue desc
limit 20
    "#,
        )?;

        Ok(())
    }
}

fn load_table(
    connection: &mut Conn,
    load_script: &str,
    table_name: &str,
) -> Result<(), Box<dyn Error>> {
    eprintln!("Loading {}", table_name);
    let start = Instant::now();
    connection.query_drop(load_script)?;
    println!("  load in         {:?}", start.elapsed());

    let compaction_start = Instant::now();
    connection.query_drop(&format!("COMPACT TABLE {}", table_name))?;
    println!("  compaction in:  {:?}", compaction_start.elapsed());
    println!("  total_load in:  {:?}", start.elapsed());
    Ok(())
}

fn run_query(connection: &mut Conn, query_name: &str, query: &str) -> Result<(), Box<dyn Error>> {
    eprintln!("Running query {}", query_name);
    let start = Instant::now();
    connection.query_drop(query)?;
    println!("  total_time: {:?}", start.elapsed());
    Ok(())
}
