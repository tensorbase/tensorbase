/*   Copyright (c) 2020 TensorBase, and its contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

use meta::{conf::Conf};
// use std::{env, fs};

// fn prepare_conf() {
//     let cur_exe_dir = env::current_exe().unwrap();
//     let cur_exe_dir = cur_exe_dir.parent().unwrap().display();
//     let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
//     let dbg_conf_src = debug!(format!(
//         "{}/../../samples/conf_sample/base.conf",
//         manifest_dir
//     ));
//     let dbg_conf_dst = debug!(cur_exe_dir.to_string() + "/conf/");
//     fs::create_dir_all(&dbg_conf_dst).unwrap();
//     println!("===sample conf copied to {}", &dbg_conf_dst);
//     std::fs::copy(&dbg_conf_src, dbg_conf_dst + "base.conf").unwrap();
// }

#[test]
fn basic_check() -> base::errors::Result<()> {
    let conf0 = toml::from_str(
        r#"[schema]
        schema_dir = '/data/n3/schema'
        
        [storage]
        data_dirs = '/data/n3/data'"#,
    )?;
    Conf::save(&conf0, None).unwrap();
    let conf1 = Conf::load(None).unwrap();
    assert_eq!(conf0, conf1);
    // println!("{}", toml::to_string_pretty(&conf0)?);
    Ok(())
}
