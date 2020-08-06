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

|  |  |
|:--|:--|
|type | |
|status| | 
|priority| | 
|component| |