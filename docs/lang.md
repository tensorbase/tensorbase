## Supported SQLs/Dialects
TensorBase tries to maintain maximum compatibility with the ClickHouse's SQL syntax in the current phase. It is hoped that you can reuse the knowledge from ClickHouse without much learnings. For details about ClickHouse's SQL, please refer to the offcial [documentation](https://clickhouse.tech/docs/en/sql-reference/).

And therefore, this document only lists the differences between two. Usually, TensorBase implements fewer functions than that of ClickHouse in the current phase. However, TensorBase is not a clone of ClickHouse. They have different goals, designs and implementations. And they may have different interpretations even with the same words. If differences made, these differences will be pointed out here as well.

In the future, more protocols/dialects may be compatible(e.g. MySQL).

### Data Types
* Int8/Int16/Int32/Int64
* UInt8/UInt16/UInt32/UInt64
* Float32/Float64
* Datetime
  * In TensorBase, the Datetime type just means Datetime32. ClickHouse Datetime type uses a mental model with timezone attribute. TensorBase follows this model like ClickHouse.
* Decimal
  * You can use Decimal(P,S) format for further Decimal type customization.
* String
* Date
* FixedString
* Uuid
* More coming soon...

### Statements
* create database 
```sql
CREATE DATABASE IF NOT EXISTS db_name
```
* create table
  * When using `clickhouse-client` to connect to TB server, the `ENGINE` attribute should be always inclued and the attribute value should be `BaseStorage`. Because this attribute is explicitly checked by `clickhouse-client`. This attribute is not necessary when using with language drivers(such as Rust driver or Java JDBC driver).
```sql
CREATE TABLE IF NOT EXISTS [db.]table_name
(
    name1 type1 NOT NULL
) ENGINE = BaseStorage
```
* show databases
```sql
SHOW DATABASES
```
* show tables
```sql
SHOW TABLES IN db
```
* show create table
```sql
SHOW CREATE TABLE [db.]table
```
* desc table
```sql
DESC TABLE [db.]table
```
* drop database 
```sql
DROP DATABASE IF EXISTS db
```
* drop table
```sql
DROP TABLE IF EXISTS [db.]name
```
* truncate table 
```sql
TRUNCATE TABLE IF EXISTS [db.]name
```
* optimize table
```sql
OPTIMIZE TABLE [db.]name
```
* insert into
  * all columns or non-columns should be provided when use 'insert into'. Partial-column inserting is not supported now.
```sql
INSERT INTO [db.]table VALUES (v11, v12, v13), (v21, v22, v23), ...
```
```sql
INSERT INTO [db.]table FORMAT CSV data_set
```

You can use this FORMAT CSV in headless client commands to import the csv data into TensorBase, like:
```bash
clickhouse-client --query="INSERT INTO trips_lite FORMAT CSV" < /some_path_here/trips_lite.csv
```

* insert into ... select
```sql
insert into tab1 select * from tab2
```

* use
```sql
USE db
```

### Aggregate Functions
case-sensitive
* count
* sum
* avg
* min
* max

Example:
```sql
SELECT avg(salary) FROM employees
```

### Built-in Functions (WIP)
case-sensitive
* toYYYY/toYear, toYYYYMM, toYYYYMMDD, toMonth...
* Track all supported functions in this [issues](https://github.com/tensorbase/tensorbase/issues/130)

Example:
```sql
SELECT toYYYYMMDD(1620797481)
```

### Remote Table Functions (WIP)
TensorBase supports remote table access in query statement via [```Remote Table Functions``` like shown in ClickHouse](https://clickhouse.tech/docs/en/sql-reference/table-functions/remote/).

Example:
```sql
select a,b,c,d,i,j from remote('127.0.0.1:9528', test_remote_func)
```
NOTE: now it is required that the column names of select clause should be specified explicitly.

