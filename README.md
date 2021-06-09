
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

## What is TensorBase
TensorBase is a new big data warehousing with modern efforts.

TensorBase is building on top of [Rust](https://www.rust-lang.org/), [Apache Arrow](https://github.com/apache/arrow-rs) and [Arrow DataFusion](https://github.com/apache/arrow-datafusion).

TensorBase hopes to change the status quo of bigdata system as follows:
  * low efficiency (in the name of 'scalable')
  * hard to use (for end users) and understand (for developers)
  * not evolving with modern infrastructures (OS, hardware, engineering...)

## :rocket: Quick News

* [SQL on RISC-V Chip in Rust](https://tensorbase.io/2021/06/08/sql_on_riscv_in_rust.html)

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

* [Milestones](https://github.com/tensorbase/tensorbase/milestones)
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

Read the [Contributing](/docs/CONTRIBUTING.md).

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

