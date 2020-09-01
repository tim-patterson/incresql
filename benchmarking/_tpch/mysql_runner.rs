use super::BenchmarkRunner;
use mysql::prelude::Queryable;
use mysql::Conn;
use std::error::Error;
use std::time::Instant;

pub struct MysqlRunner {
    s: u8,
    mysql_connection: Conn,
}

impl MysqlRunner {
    pub fn new(s: u8) -> Result<Self, Box<dyn Error>> {
        let client_url = "mysql://root@localhost:3306";

        let mysql_connection = mysql::Conn::new(client_url)?;

        Ok(MysqlRunner {
            s,
            mysql_connection,
        })
    }
}

impl BenchmarkRunner for MysqlRunner {
    fn create_tables(&mut self) -> Result<(), Box<dyn Error>> {
        eprintln!("Creating schema/tables");
        self.mysql_connection
            .query_drop(format!("drop database if exists tpch_{}", self.s))?;
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
        self.mysql_connection
            .query_drop("set global local_infile = 1")?;
        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
LOAD DATA INFILE '{}/part.tbl' INTO TABLE part
  FIELDS TERMINATED BY '|'
    "#,
                data_dir
            ),
            "part",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
LOAD DATA INFILE '{}/supplier.tbl' INTO TABLE supplier
  FIELDS TERMINATED BY '|'
    "#,
                data_dir
            ),
            "supplier",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
  LOAD DATA INFILE '{}/partsupp.tbl' INTO TABLE partsupp
  FIELDS TERMINATED BY '|'

    "#,
                data_dir
            ),
            "partsupp",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
  LOAD DATA INFILE '{}/customer.tbl' INTO TABLE customer
  FIELDS TERMINATED BY '|'
    "#,
                data_dir
            ),
            "customer",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
  LOAD DATA INFILE '{}/orders.tbl' INTO TABLE orders
  FIELDS TERMINATED BY '|'
    "#,
                data_dir
            ),
            "orders",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
  LOAD DATA INFILE '{}/lineitem.tbl' INTO TABLE lineitem
  FIELDS TERMINATED BY '|'
    "#,
                data_dir
            ),
            "lineitem",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
  LOAD DATA INFILE '{}/nation.tbl' INTO TABLE nation
  FIELDS TERMINATED BY '|'
    "#,
                data_dir
            ),
            "nation",
        )?;

        load_table(
            &mut self.mysql_connection,
            &format!(
                r#"
  LOAD DATA INFILE '{}/region.tbl' INTO TABLE region
  FIELDS TERMINATED BY '|'
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
    l_shipdate <= date '1998-12-01' - interval '90' day
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
