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

//NAIVE just PoC, will change when storage layer kicks in

pub mod conf;
pub mod schemas;

use conf::Conf;
use once_cell::sync::Lazy;
use schemas::Catalog;
use std::sync::Mutex;

pub static CAT: Lazy<Mutex<Catalog>> = Lazy::new(|| {
    let conf = Conf::load(None).unwrap();
    let schema_dir = conf.schema.schema_dir;
    Mutex::new(Catalog::load(&schema_dir).unwrap().unwrap_or_default())
});

pub static CONF: Lazy<Conf> = Lazy::new(|| Conf::load(None).unwrap());
