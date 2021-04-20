DROP TABLE IF EXISTS lineitem;

CREATE TABLE lineitem
(
    l_orderkey      Int32,  -- PK(1), FK o_orderkey
    l_partkey       Int32,  -- FK ps_partkey
    l_suppkey       Int32,  -- FK ps_suppkey
    l_linenumber    Int32,  -- PK(2)
    l_quantity      Decimal(18,2),
    l_extendedprice Decimal(18,2),
    l_discount      Decimal(18,2),
    l_tax           Decimal(18,2),
    l_returnflag    LowCardinality(String),
    l_linestatus    LowCardinality(String),
    l_shipdate      Date,
    l_commitdate    Date,
    l_receiptdate   Date,
    l_shipinstruct  LowCardinality(String),
    l_shipmode      LowCardinality(String),
    l_comment       String -- variable text size 44
) engine = BaseStorage
PARTITION BY toYYYYMM(l_shipdate)
ORDER BY l_orderkey

DROP TABLE lineitem;

