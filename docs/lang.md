## Supported SQLs/Dialects
TensorBase tries to maintain maximum compatibility with the ClickHouse's SQL syntax in the current phase. It is hoped that you can reuse the knowledge from ClickHouse without much learnings.

And therefore, this document only lists the differences between two. Usually, TensorBase implements fewer functions than that of ClickHouse in the current phase. However, TensorBase is not a clone of ClickHouse. They have different goals, designs and implementations. And they may have different interpretations even with the same words. If differences made, these differences will be pointed out here as well.

In the future, more protocols/dialects may be compatible(e.g. MySQL).

### Data Types
* Int8/Int16/Int32/Int64
* UInt8/UInt16/UInt32/UInt64
* Datetime
In TensorBase, the Datetime type just means Datetime32. ClickHouse Datetime type uses a mental model with timezone attribute. TensorBase follows this model like ClickHouse.
* LowCardinality(String)
* Decimal(WIP)
In TensorBase, the Decimal type just means Decimal(9,2). 
You can use Decimal(P,S) format for further Decimal type customization.
* String(coming soon)

### Statements
* create database 

* create table 

* show databases 

* show tables 

* show create table 

* drop database 

* drop table 

* truncate table 

* optimize table 

* insert into
all columns or non-columns should be provided when use 'insert into'. Partial-column inserting is not supported now.

Example:

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
* use db

### Aggregate Functions
case-sensitive
* count
* sum
* avg
* min
* max

### Built-in Functions
case-sensitive
* toYYYY/toYear
* toYYYYMM
* toYYYYMMDD
* toMonth
* toDayOfMonth
* toUnixTimestamp