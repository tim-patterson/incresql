create database tpch_1;

CREATE TABLE tpch_1.lineitem
(
    l_orderkey    BIGINT,
    l_partkey     BIGINT,
    l_suppkey     BIGINT,
    l_linenumber  BIGINT,
    l_quantity    DOUBLE PRECISION not null,
    l_extendedprice  DOUBLE PRECISION not null,
    l_discount    DOUBLE PRECISION not null,
    l_tax         DOUBLE PRECISION not null,
    l_returnflag  CHAR(1) not null,
    l_linestatus  CHAR(1) not null,
    l_shipdate    DATE not null,
    l_commitdate  DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct CHAR(25) not null,
    l_shipmode     CHAR(10) not null,
    l_comment      VARCHAR(44) not null
);


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
);

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
FROM directory "target/dbgen_s1/lineitem.tbl" with(delimiter="|");