DROP TABLE IF EXISTS test_tab

CREATE TABLE test_tab(a UInt64)

INSERT INTO test_tab VALUES (1), (2), (3)

:6
select sum(a) from test_tab