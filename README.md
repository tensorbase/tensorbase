
<p align="center">
    <img src="https://user-images.githubusercontent.com/237573/117403590-fba83180-af3a-11eb-9464-276af1ad1b80.png">
</p>

<p align="center">
<img src="https://img.shields.io/github/license/tensorbase/tensorbase">
<img src="https://img.shields.io/github/issues/tensorbase/tensorbase">
<img src="https://img.shields.io/github/workflow/status/tensorbase/tensorbase/Base%20Integ%20Sanity%20Checks">
<a href="https://discord.com/invite/E72n2jzgKD">
  <img src="https://img.shields.io/discord/794816685978419210?logo=discord"
  alt="chat on Discord">
</a>
</p>

## Status of the Project

TensorBase hasn't been updated for a while. Thanks for friends' concern and inquiries, we reply as follows: 

TensorBase hopes the open source not become a copy game. TensorBase has a clear-cut opposition to fork communities, repeat wheels, or hack traffics for so-called reputations(like Github stars). After thoughts, we decided to temporarily leave the general OLAP field.

Here, let's recap all `the world's first` of TensorBase:
1. The world's first [ClickHouse](https://clickhouse.com/) compatible open-source implementation.
2. 2x faster write throughput than that of ClickHouse (based on [our bug fixed Rust client](https://github.com/tensorbase/tensorbase/tree/main/crates/client), you can get ~1.7x speedup by [our another simple concurrent script here](https://github.com/tensorbase/tools)).
3. Faster query speed in the simple aggregation than that of ClickHouse. 
4. First no-LSM, write and read optimized storage layer proposed.
5. First make "copy-free, lock-free, async-free, dyn-free" happened in an open-source DBMS's critical write path.
6. First DBMS running on the real-world RISC-V hardware.
7. First top-performance whole-lifecycle JIT SQL query engine(not open sourced)...

For people looking for production level data warehouse solutions, we still recommend [ClickHouse](https://clickhouse.com/). We wish that ClickHouse can learn from these work and evolve itself to better.

For people who want to learn how a database like system can be built up, or how to apply modern Rust to the high performance field, or embed a lightweight data analysis system into your big system. You can still try, ask or contribute to TensorBase. The creators and committers are still around the community. We will help you in all kinds of interesting things uncovered or covered in the project. We still maintain the project to look forward to meeting more database geniuses in this world, although no new feature will be added in the near future. 

The core team of TensorBase has moved to another new type of domain-specific database. [We are hiring](https://tensorbase.io/joinus/)!

## What is TensorBase
TensorBase is a new big data warehousing with modern efforts.

TensorBase is building on top of [Rust](https://www.rust-lang.org/), [Apache Arrow](https://github.com/apache/arrow-rs) and [Arrow DataFusion](https://github.com/apache/arrow-datafusion).

TensorBase hopes to change the status quo of bigdata system as follows:
  * low efficiency (in the name of 'scalable')
  * hard to use (for end users) and understand (for developers)
  * not evolving with modern infrastructures (OS, hardware, engineering...)

## Features

* Out-of-the-box to play ( [get started just now](#quick-start) )
* Lighting fast architectural performance in Rust ( [real-world benchmarks](#benchmarks) )
* Modern redesigned columnar storage 
* Top performance network transport server  
* ClickHouse compatible syntax
* Green installation with DBA-Free ops
* Reliability and high availability (WIP)
* Cluster (WIP)
* Cloud-Native Adaptation (WIP)
* Arrow dataLake (...)

## Architecture (in 10,000 meters altitude)

![arch_base](https://user-images.githubusercontent.com/237573/115341887-efeb0a00-a1db-11eb-8aea-0c6cef2ba1ca.jpg)

## Quick Start

![play_out_of_the_box](https://user-images.githubusercontent.com/237573/115368682-e5d80400-a1f9-11eb-9a9e-deeb4d5d58d2.gif)

* [Get Started for Users](/docs/get_started_users.md) 

* [Get Started for Developers](/docs/get_started_developers.md) 

## Benchmarks

TensorBase is **lighting fast**. TensorBase has shown better performance than that of ClickHouse in simple aggregation query on 1.47-billion rows NYC Taxi Dataset.

TensorBase has **enabled full workflow for TPC-H benchmarks from data ingestion to query**.

More detail about all benchmarks seen [in benchmarks](https://github.com/tensorbase/benchmarks).

## Roadmap

* [Base Space Station](https://github.com/tensorbase/tensorbase/issues/141)

## Community Newsletters
* [This Week in TensorBase](https://tensorbase.io/tw/)

## Working Groups

#### Working Group - Engineering
This is a wg for engineering related topics, like codes or features.

#### Working Group - Database
This is a higher kind wg for database related topics, like ideas from papers.

Join these working groups on the [Discussions](https://github.com/tensorbase/tensorbase/discussions) or on [Discord server](https://discord.gg/E72n2jzgKD).


## Communications

* [Discussions](https://github.com/tensorbase/tensorbase/discussions)

* [Discord server](https://discord.gg/E72n2jzgKD)

* [Slack Channel](https://join.slack.com/t/tensorbase/shared_invite/zt-ntwmjvpu-TQ9drOdUwNJWmUTXvxMumA)

Wechat group or other more are on [community](https://tensorbase.io/community/)

## Contributing

We have a great contributing guide in the [Contributing](/docs/CONTRIBUTING.md). 

## Documents (WIP)

More documents will be prepared soon.

Read the [Documents](/docs/docs.md).

## License
TensorBase is distributed under the terms of the Apache License (Version 2.0), which is a commercial-friendly open source license.

It is greatly appreciated that,

* you could give this project a star, if you think these got from TensorBase are helpful.
* you could indicate yourself in [Who is Using TensorBase](/docs/who_using.md), if you are using TensorBase in any project, product or service. 
* you could contribute your changes back to TensorBase, if you want your changes could be helpful for more people.

Your encouragements and helps can make more people realize the value of the project, and motivate the developers and contributors of TensorBase to move forward.

See [LICENSE](LICENSE) for details.

