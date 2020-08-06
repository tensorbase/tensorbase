## What is TensorBase
TensorBase is a modern engineering effort for building a high performance and cost-effective bigdata analytics infrastructure in an open source culture. 


## Status
TensorBase is in its intial stage (milestone 0) and under heavy development. 

TensorBase is an **architectural performance** design. [It is demonstrated](https://tensorbase.io/2020/08/04/hello-base.html#benchmark) to query ~1.5 billion rows of NYC taxi dataset in ~100 milliseconds for total response time in its milestone 0. The raw speed of core data scanning in kernel saturates the memory bandwidth (for example, ~120GB/s for six-channel single socket). Column-oriented, vectorized, SIMD all have, and big bangs...

TensorBase is written from scratch in the Rust language (system) and C language (runtime kernel). Here, you use the most familiar tools to challenge the most difficult problems. Comfortable languages and minimized dependencies, from-scratch architecting make it a **highly hackable system**. 

Read [launch post](https://tensorbase.io/2020/08/04/hello-base.html) to get more about TensorBase's "Who? Where from? Where go?"

Please give TensorBase a star to help it more grown.


## Try TensorBase
TensorBase is developed for Linux, but should work for any docker enabled system (for example, Windows 10 WSL2).

* from source

TensorBase follows the idiomatic development flow of Rust. Make sure your Rust nightly toolchain works. If you only try to run, just play with [Quick Start](#quick-start). Thanks to the strong rust ecosystem, it is not necessary to run build first.

* docker

This mode is portable but more host resource occupied, and the performance is platform dependent.

TBD.


## Quick Start
Now TensorBase provides two binaries to enable the following workflow:

* baseops: cli/workbench for devops, including kinds of processes/roles starts/stop

* baseshell: query client (now is a monolithic to include everything), m0 only supports query with single integer column type sum aggregation intentionally.

1. run _baseops_ to create a table definition in Base
```bash
cargo run --bin baseops table create -c samples/nyc_taxi_create_table_sample.sql
```
Base explicitly separates write/mutation behaviors into the cli baseops. the provided sql file is just an ansi-SQL DDL script, which can be seen in the [*samples* directory of repo](samples).

2. run _baseops_ to import [nyc_taxi csv dataset](https://clickhouse.tech/docs/en/getting-started/example-datasets/nyc-taxi/) into Base
```bash
cargo run --release --bin baseops import csv -c /jian/nyc-taxi.csv -i nyc_taxi:trip_id,pickup_datetime,passenger_count:0,2,10:51
```
Base import tool uniquely supports to import csv partially into storage like above. Use help to get more infos.

3. run _baseshell_ to issue query against Base
```bash
cargo run --release --bin baseshell
```

[Dev Docs](/docs/dev.md) provides a little more explanation for why above commands work.


## Engineering Efforts
Welcome to join us, you data nerds!

Here are on-going efforts. If you are interested in any effort, do not hesitate to join in.

| subsystem | component   | priority | committers |
|:---       |:---         |:---      |:---        |
| storage*  | | | [@jinmingjian](https://github.com/jinmingjian) |
|           | data layout | | | 
|           | data read | | | 
|           | data write | | |
|           | metadata | | | 
| runtime   | | | [@jinmingjian](https://github.com/jinmingjian) |
|           | base language(sql) | | |     
|           | parsing | | |
|           | base ir | | |
|           | codegen | | |
|           | jit compiler* | | |
|           | kernel execution | | |
| infra     | | | [@jinmingjian](https://github.com/jinmingjian) |
|           | common   | | |
|           | lib      | | |
|           | testing  | | |
|           | bench    | | |
|           | doc      | | | 
|           | project  | | |                
| client    | | | [@jinmingjian](https://github.com/jinmingjian) |  
|           | baseshell | | |
|           | baseops   | | | 
|           | visualization   | | | 


## Communication

Feel free to feedback any problem via [issues](https://github.com/tensorbase/tensorbase/issues).

Mailing list: just open an [issue](https://github.com/tensorbase/tensorbase/issues) with label [type/discuss].

[Slack Channel](https://tensorbase.slack.com/)


## Contributing
Thanks for your contributions!

[Dev Docs](/docs/dev.md)


## License
TensorBase is distributed under the terms of the Apache License (Version 2.0).

See [LICENSE](LICENSE) for details.

