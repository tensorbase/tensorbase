## Base Conventions

### Baisc Logics

* binaries are the entries for all kinds in base
* configs are used to configure the binaries to work 

### Details

```bash
ROOT
 |-- bin -- binaries
 |-- conf -- confs
```

* ROOT are the top directory of dsitribution  
* default binaries are in ROOT/bin
* default configs are in the ROOT/conf
* default configs can be overloaded by command line options
* all other things can and should be configured by conf files or its equivalent cli options
* debug mode extra conf search path: if the containing directory of binaries are not named "bin", then it is assumed debug mode, then it is allowed to search into $binaries_directory/conf for configs after failed to search into ../conf firstly
* configs should be provided in some way for binaries (to make thing work)


## Issue Conventions

| label | |
|:--|:--|
|type | |
|status| | 
|priority| | 
|component| |
|difficulty| | 
|creativity| |


## Base Development

### Prerequisites

Linux(windows 10 wsl2 probably ok) + Rust nightly toolchain(installed via Rustup)

* The minimum kernel version will be specified at some time
* The revision of Rust nightly toolchain will be specified at some time
* it is possible to figure out some work flow for dev under Windows 10 ([feel free to request](https://github.com/tensorbase/tensorbase/issues))

for development, you need more toolings to be installed:
* clang
* cmake
* gcc 2.29+ (gcc is usually Linux distro shipped, for Ububtu, this requires you should have 19.04+ for works. It is considered to provide a native installation package at some time point.)

Hardware:
* x86 processor with avx2 is required for running __baseops__ command now. This is considerd to be changed in m1. stay tunned.

### Tools Recommendation

The committers of Base are usually using VS Code + Rust-analyzer(RA) extension + "Microsoft C/C++ for Visual Studio Code" extension as the development environment.

Feel free to seek more helps from the Base community.

## Code Status

In the early phase of Base, the building may not be guaranteed by daily commits. If you just do a building for some code understandings, just pick up the one from release tags. 

The most recent release tag is [m0](https://github.com/tensorbase/tensorbase/tree/m0).

Feel free to seek more helps from the Base community.