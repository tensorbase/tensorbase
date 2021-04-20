0. ensure you have the binary of ClickHouse client
    
    Download from the [github](https://github.com/ClickHouse/ClickHouse/releases), stable release is recommended.

1. get the binary of TensorBase server
    
    Download from the [github](https://github.com/tensorbase/tensorbase/releases)

2. config a base.conf for server booting
    
    Here is [an example of base.conf](/crates/server/tests/confs/base.conf)

3. start the TensorBase server like this:

        ./server -c $path_to_base_conf$

4. connect to the TensorBase server with clickhouse-client like this:

        clickhouse-client --port 9528
    
    NOTE: here 9528 is the default port of TensorBase

5. execute query like this:

        show tables

    or

        select count(trip_id) from trips_lite

6. more supported statements could be seen [here](/docs/lang.md)

    TensorBase supports high concurrent ingestions from clickhouse-client and native protocol drivers ( [Rust client driver here](/crates/tests_integ/ch_client)). Welcome to practice!


## Get Started Live Recording
---------------------------
![play_out_of_the_box](https://user-images.githubusercontent.com/237573/115368682-e5d80400-a1f9-11eb-9a9e-deeb4d5d58d2.gif)