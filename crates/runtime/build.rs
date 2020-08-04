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
use std::{env::var, path::Path};

// use std::io::Command;
fn main() -> AnyResult<()> {
    let manifest_dir = var("CARGO_MANIFEST_DIR")?;
    let lib_dir = format!("{}/{}", manifest_dir, "lib");
    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=dylib=basejitc");

    //just hack for debug but works for release...
    let lib_path_src = format!("{}/{}", lib_dir, "libbasejitc.so");
    let lib_path_dst = &dlib_path_hack()?;
    println!(
        "cargo:warning=Note: The runtime dynamic libray copied to {}",
        lib_path_dst
    );
    std::fs::copy(&lib_path_src, &lib_path_dst)?;

    Ok(())
}

fn dlib_path_hack() -> AnyResult<String> {
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
    let ret = [
        deps_path.unwrap().to_str().unwrap(),
        mode,
        "deps/libbasejitc.so",
    ]
    .join("/");
    // println!("cargo:warning={:?}/", debug.parse::<bool>().unwrap());
    Ok(ret)
}
