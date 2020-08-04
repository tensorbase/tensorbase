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

//NAIVE just PoC?
use base::errors::AnyResult;
use base::debug;
use serde::{Deserialize, Serialize};
use std::{env, fs, path::PathBuf};

// [schema]
// schema_dir = "/data/n3/schema"
// [storage]
// data_dirs = "/data/n3/data"
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Conf {
    pub schema: Schema,
    pub storage: Storage,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Schema {
    pub schema_dir: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Storage {
    pub data_dirs: String, //TODO
}

impl Conf {
    pub fn load(load_path: Option<&str>) -> AnyResult<Conf> {
        let conf_path = match load_path {
            None => {
                let path = env::current_exe()?;
                let path = path.parent().unwrap();
                // println!("The current exe dir is {}", path.display());
                path.join("conf/base.conf")
            }
            Some(path) => PathBuf::from(path),
        };
        let config: Conf =
            toml::from_str(&fs::read_to_string(conf_path)?).unwrap();
        Ok(config)
    }
    pub fn save(conf: &Conf, save_path: Option<&str>) -> AnyResult<()> {
        let conf_path = match save_path {
            None => {
                let path = env::current_exe()?;
                let path = path.parent().unwrap().join("conf");
                fs::create_dir_all(&path).unwrap();
                path.join("base.conf")
            }
            Some(path) => PathBuf::from(path),
        };
        
        let toml = toml::to_string_pretty(&conf)?;
        fs::write(conf_path, toml)?;

        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {}
