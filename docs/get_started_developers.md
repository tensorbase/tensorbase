If you are a end user, you may want to start from the binary. You could read [Get Started for Users](/docs/get_started_users.md).

The development of TensorBase is same to the idiom of Rust engineering.

0. It is assumed that you have setup your own Rust development environment.

    TIPS: You could have Rust toolchains and cargo installed into your path via [rustup](https://rustup.rs/).
    
    NOTE: TensorBase is depending on the Rust nightly toolchain.

1. clone the project and go into that cloned repo directory

        git clone https://github.com/tensorbase/tensorbase.git
        cd tensorbase

2. config a base.conf for server booting

    Here is [an example of base.conf](/crates/server/tests/confs/base.conf). It is suggested that you just change the meta_dirs and data_dirs to your own directory.

3. use cargo to run the server in debug mode (fast compilation but slow run),

        cargo run --bin server -- -c $path_to_base_conf$

    or in release mode (slow compilation but fast run),
        
        cargo run --release --bin server -- -c $path_to_base_conf$

    NOTE: $path_to_base_conf$ is the full path of conf done in #2.

4. ensure you have the binary of ClickHouse client

    Download from the [github](https://github.com/ClickHouse/ClickHouse/releases), stable release is recommended.

5. connect to the TensorBase server with clickhouse-client like this:

        clickhouse-client --port 9528
        
    NOTE: here 9528 is the default port of TensorBase

6. execute query like this:

        show tables
        
    or

        select count(trip_id) from trips_lite_n10m

7. more supported statements could be seen [here](/docs/lang.md)