## Benchmarks

A simple benchmark on real-world NYC Taxi dataset has been exercised:

|Query |ClickHouse (v21.2.5.5)      | TensorBase (main branch)  | Speedup Ratio of TB   |
|:----:|:---------------------------:|:-----------------------: | :--------------------------: |
| select sum(trip_id) from trips_lite | 0.248 sec  |  0.079 sec | 3.1 (TB is faster) |
| select date_part('year',pickup_datetime), count(1) from trips_lite group by date_part('year',pickup_datetime)* | 0.514 sec |  3.375 sec  | 0.15 (TB is slower)  |

Note:
* The adoption to ClickHouse has not been completed in that DataFusion supports a different dialect. The query sql in ClickHouse is: select toYear(pickup_datetime), count(1) from trips_lite group by toYear(pickup_datetime)
* Hardware: 1 Socket, Intel Xeon Platinum 8260, 24 cores / 48 hyperthreads, 6-channel DDR4-2400 ECC REG 192GB DRAMs
* trips_lite Dataset: column-stripped NYC TAXI Dataset, 1464781690 rows, two columns(trip_id, pickup_datetime)
* Measurement rules: run 3 times, pick up the best run time. This is an in-memory test. If runs from the cold disk, ClickHouse wins for compression.