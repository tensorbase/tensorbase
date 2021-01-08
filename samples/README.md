All kinds of samples will be put into here.

## Usage

How to use the samples?

1. setup conf file
the build script of related crates will copy and use the sample conf shipped in this directory. you can change the configs as you like in samples/conf_sample/base.conf

2. copy the binaries to 

> mkdir /data/n3/{data,schema}
> cp data_bin/* /data/n3/data
> cp schema_bin/* /data/n3/schema

Note: data_bin/schema_bin are as a tiny demo for nyc taxi dataset. the original data is trips_1m.csv here. In M0, there is no storage layer. Here, the binary data are just array like data memory dump.

3. check your output layout matched with samples/conf_sample/base.conf
> tree /data/n3/      
/data/n3/
├── data
│   ├── 0
│   ├── 1
│   ├── 2
│   └── meta
└── schema
    └── cat