Try
-----------
 is at its early age and under heavy development, some of the features mentioned above have not been fully implemented.

TensorBase now is in its first preview release. The primary goal is to invite data and performance nerds to join us!

philosophy


Though in first preview release, it shows you the uniqueness of TensorBase. 

performance issues are considered as bug.

explore extreme performance from scratch.

prerequisites
-------------

Operation Systems:
Linux only, but should work for any docker enabled system
official tested:
* Arch with latest stable kernel
* Windows 10 WSL2
(a minimal kernel version may be specified in the near future)

Hardware:
* legacy - best effort in the early stage but may be discontiuned any time
* base - best effort in the early stage but x86 + avx2 in mind (possibly more ISAs supported in future)
* front - latest commodity hardwares (and systems, maybe not yet released) (opt-in)

Get TensorBase
Because just in , it is recommended just to play it in your casual style. 


Thanks to the strong rust ecosystem, just several clicks to build your own binaries (for rustaceans pleasure).

Now TensorBase provides two binaries to enable the following workflow:

baseops: cli/workbench for devops, including kinds of processes/roles starts/stop

Base dsitribution term conventions:

* binaries are the entries for all kinds in base
* configs are used to configure the binaries to work 

more details:

ROOT
 |-- bin -- binaries
 |-- conf -- confs

* ROOT are the top directory of dsitribution  
* default binaries are in ROOT/bin
* default configs are in the ROOT/conf
* default configs can be overloaded by command line options
* all other things can and should be configured by conf files or its equivalent cli options
* debug mode extra conf search path: if the containing directory of binaries are not named "bin", then it is assumed debug mode, then it is allowed to search into $binaries_directory/conf for configs after failed to search into ../conf firstly
* configs should be provided in some way for binaries (to make thing work)


use baseops 
import data from csv to disk 
config the data path for Base
import 


baseshell: shell client for query(read-only intentionally)
In the early stage release, baseshell services as all-in-one role (client + server).

(in a ansi-sql partially-compatible language, called "Base lang")






See Engineering Efforts for more explanations and plans.

Base Shell: 
```bash
cargo run --bin baseops
```

```bash
cargo run --bin baseshell
```

Googling [rustup](https://www.google.com/search?q=rustup) for more if you are Rust newcomers.



For new

Roadmap?

Architecture

Group?

Contributing
Contributions are welcomed and greatly appreciated. See CONTRIBUTING.md for details on submitting patches and the contribution workflow.




cargo run 

Zulip
Telegram
reddit.com

stackoverflow
 

