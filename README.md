
<p align="center">
![base_logo](https://user-images.githubusercontent.com/237573/117401469-37d99300-af37-11eb-8154-71909c9cb8bb.png)
</p>
---------------
<p align="center">
<a href="https://discord.gg/hVfUAXvh">
  <img src="https://img.shields.io/discord/794816685978419210?logo=discord"
  alt="chat on Discord">
</a>
<img src="https://img.shields.io/github/license/tensorbase/tensorbase">
</p>

## What is TensorBase
TensorBase is a new big data warehousing with modern efforts.

TensorBase is building on top of Rust, Apache Arrow/DataFusion.

TensorBase hopes to change the status quo of bigdata system as follows:
  * low efficient (in the name of 'scalable')
  * hard to use (for end users) and understand (for developers)
  * not evolve with modern infrastructures (OS, hardware, engineering...)

## Features

* Out-of-the-box to play ( [get started just now](#quick-start) )
* Lighting fast architectural performance In Rust ( [real-world benchmarks](#benchmarks) )
* Modern redesigned columnar storage 
* Top performance network transport server  
* ClickHouse compatible syntax
* Green installation with DBA-Free ops
* Reliability and high availability (WIP)
* Cluster (TBD)
* Cloud neutral and cloud native (TBD)
* Arrow dataLake (...)

## Architecture (in 10,000 meters altitude)

![arch_base](https://user-images.githubusercontent.com/237573/115341887-efeb0a00-a1db-11eb-8aea-0c6cef2ba1ca.jpg)

## Quick Start

![play_out_of_the_box](https://user-images.githubusercontent.com/237573/115368682-e5d80400-a1f9-11eb-9a9e-deeb4d5d58d2.gif)

* [Get Started for Users](/docs/get_started_users.md) 

* [Get Started for Developers](/docs/get_started_developers.md) 

## Benchmarks

For query, TensorBase is faster in simple aggregation, but soon slower in more complex cases. Great start!

|Query |ClickHouse (v21.2.5.5)      | TensorBase (main branch)  | Speedup Ratio of TB   |
|:----:|:---------------------------:|:-----------------------: | :--------------------------: |
| select sum(trip_id) from trips_lite | 0.248 sec  |  0.079 sec | 3.1 (TB is faster) |
| select date_part('year',pickup_datetime), count(1) from trips_lite group by date_part('year',pickup_datetime)* | 0.514 sec |  3.375 sec  | 0.15 (TB is slower)  |

More detail about this benchmark seen [in benchmarks](/docs/benchmarks.md).

## Roadmap

* [Milestones](https://github.com/tensorbase/tensorbase/milestones)
* [This Week in TensorBase](https://tensorbase.io/tw/)

## Dev Meeting

We setup an online dev meeting on Zoom at Wednesday 7:00pm (UTC+8) or at Tencent Meeting at Friday 7:00pm (UTC+8). The meeting url will be shared before the start time in [Discord server](https://discord.gg/E72n2jzgKD) and [Slack Channel](https://join.slack.com/t/tensorbase/shared_invite/zt-ntwmjvpu-TQ9drOdUwNJWmUTXvxMumA) for Zoom, or Wechat group for Tencent Meeting.

(The current time is flexible now, if you want to have a talk but in another timezone just leave a message in any way.)

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

