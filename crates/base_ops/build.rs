/*
*   Copyright (c) 2020 TensorBase, and its contributors
*   All rights reserved.

*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*/

use base::errors::AnyResult;
use cmake::Config;
use std::{env::var, fs::{self, OpenOptions}, path::Path};

fn main() -> AnyResult<()> {
    let dst = Config::new("libbops")
        .define("CMAKE_C_COMPILER", "clang")
        .cflag("-O3 -march=native")
        // .cflag("-O0 -g -march=native")
        .build();

    println!("cargo:rustc-link-search=native={}", dst.display());
    println!("cargo:rustc-link-lib=static=libbops");

    let manifest_dir = var("CARGO_MANIFEST_DIR")?;
    let dbg_conf_src =
        format!("{}/../../samples/conf_sample/base.conf", manifest_dir);
    let dbg_conf_dst = target_path_hack()? + "/conf/base.conf";
    fs::create_dir_all(&(target_path_hack()? + "/conf"))?;
    println!("cargo:warning=Note: sample conf copied to {}", dbg_conf_dst);
    std::fs::copy(&dbg_conf_src, &dbg_conf_dst)?;

    Ok(())
}

fn target_path_hack() -> AnyResult<String> {
    let out_dir = var("OUT_DIR")?;
    // println!("cargo:warning={}/", out_dir);
    let out_path = Path::new(&out_dir);
    let deps_path = out_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent();
    // println!("cargo:warning={}", debug);

    let mut mode = if Ok("release".to_owned()) == var("PROFILE") {
        "release"
    } else {
        "debug"
    };
    let ret = [deps_path.unwrap().to_str().unwrap(), mode].join("/");
    // println!("cargo:warning={:?}/", debug.parse::<bool>().unwrap());
    Ok(ret)
}
