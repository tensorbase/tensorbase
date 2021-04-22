## Project organizations

|   path      |  components    |
|:-----------:|:---------------------------|
| arrow-datafusion  |  modified sources from arrow-datafusion <br /> (we may use a dedicate repo in the future) |
| crates/base   | base library for common utils |
| crates/engine | bridge to DataFusion <br /><br />main works: <br />adapt partition tree + mmap based storage model into the DataFusion's MemTable |
| crates/lang | language stuffs for bigdata(a.k.a., SQL dialect) <br /><br />main works: <br />now as ClickHouse SQL dialect parsing library |
| crates/lightjit | a simple jit engine as fast "eval" for expression <br /><br />main works: <br />now used for partition key expression evaluation (but can be extended) |
| crates/meta | stores for schema like thing here <br /><br />main works: <br />1. store::parts for partition tree of storage; 2. store::sys for general schema; 3. basic data type definitions |
| crates/runtime |  runtime to support system management, data read/write, ClickHouse-protocol handling <br />(note: we have not dedicated storage crate because the current storage layer is thin, this may change in the future)  |
| crates/server |  provide the main entry for base's server  |
| crates/test_utils | common test utils as library  |
| crates/tests_integ | integration tests |