# Get Started for Users

> If you are a developer, you may want to start from sources, read [Get Started for Developers](/docs/get_started_developers.md).


0. ensure you have the binary of ClickHouse client
    
    + Download from the [ClickHouse release page](https://github.com/ClickHouse/ClickHouse/releases), stable release is recommended.
    + or [download here](https://github.com/tensorbase/tensorbase/releases/download/v2021.07.05/clickhouse_client_repack_linux.zip), a repacked ClickHouse client for your quick start. (Still go to the official release above if you want to try the full or latest ClickHouse.)

1. get the binary of TensorBase server
    
    Download from the [TensorBase release page](https://github.com/tensorbase/tensorbase/releases)

2. config a base.conf for server booting
    
    Here is [an example of base.conf](/crates/server/tests/confs/base.conf)

3. start the TensorBase server like this:

        ./server -c $path_to_base_conf$

4. connect to the TensorBase server with clickhouse-client like this:

        clickhouse-client --port 9528
    
    NOTE: here 9528 is the default port of TensorBase

5. execute query like this:

        create table employees (id UInt64, salary UInt64) ENGINE = BaseStorage
        insert into employees values (0, 1000), (1, 1500)
        select count(id) from employees
        select avg(salary) from employees

6. more supported statements could be seen [here](/docs/lang.md)

    TensorBase supports high concurrent ingestions from clickhouse-client and native protocol drivers ( [Rust client driver here](/crates/tests_integ/ch_client)). Welcome to practice!


## Get Started Live Recording
---------------------------
![play_out_of_the_box](https://user-images.githubusercontent.com/237573/115368682-e5d80400-a1f9-11eb-9a9e-deeb4d5d58d2.gif)