# Get Started for Developers

> If you are a end user, you may want to start from the binary, read [Get Started for Users](/docs/get_started_users.md).

The development of TensorBase is same to the idiom of Rust engineering.

0. It is assumed that you have setup your own Rust development environment.

    TIPS: You could have Rust toolchains and cargo installed into your path via [rustup](https://rustup.rs/).
    
    NOTE: TensorBase is depending on the Rust nightly toolchain.

1. clone the project and go into that cloned repo directory

        git clone https://github.com/tensorbase/tensorbase.git
        cd tensorbase

2. config a base.conf for server booting

    Here is [an example of base.conf](/crates/server/tests/confs/base.conf). It is suggested that you just copy and change the meta_dirs and data_dirs to your own directory.

3. use cargo to run the server in debug mode (fast compilation but slow run),

        cargo run --bin server -- -c $path_to_base_conf$

    or in release mode (slow compilation but fast run),
        
        cargo run --release --bin server -- -c $path_to_base_conf$

    NOTE:
    + $path_to_base_conf$ is the full path of conf in #2.
    + current release profile is using lto = 'thin'. You could adjust the options in [Cargo.toml](Cargo.toml) for balancing the speed for compilation and running. However, TensorBase uses lto = 'fat' for its binary release.

4. ensure you have the binary of ClickHouse client

    + Download from the [ClickHouse release page](https://github.com/ClickHouse/ClickHouse/releases), stable release is recommended.
    + or [download here](https://github.com/tensorbase/tensorbase/releases/download/v2021.04.24-1/clickhouse_client_repack_linux.zip), a repacked ClickHouse client for your quick start. (Still go to the official release above if you want to try the full or latest ClickHouse.)

5. connect to the TensorBase server with clickhouse-client like this:

        clickhouse-client --port 9528
        
    NOTE: here 9528 is the default port of TensorBase

6. execute query like this:

        create table employees (id UInt64, salary UInt64) ENGINE = BaseStorage
        insert into employees values (0, 1000), (1, 1500)
        select count(id) from employees
        select avg(salary) from employeese

7. more supported statements could be seen [here](/docs/lang.md).

    TensorBase supports high concurrent ingestions from clickhouse-client and native protocol drivers ( [Rust client driver here](/crates/tests_integ/ch_client)). Welcome to practice!

8. and TensorBase thanks for your contributions, read [Contributing](/docs/CONTRIBUTING.md) for more.

## Get Started Live Recording
---------------------------
![play_out_of_the_box](https://user-images.githubusercontent.com/237573/115368682-e5d80400-a1f9-11eb-9a9e-deeb4d5d58d2.gif)
