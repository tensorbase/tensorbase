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
use base::fs::validate_path;
use serde::{Deserialize, Serialize};
use std::{env, fs};

use crate::errs::{MetaError, MetaResult};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Conf {
    pub system: System,
    #[serde(default)]
    pub storage: Storage,
    pub server: Server,
    pub clickhouse: Option<CHGroup>,
}

// impl Deref for Conf {
//     type Target;
//     fn deref(&self) -> &Self::Target {
//     }
// }

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct System {
    pub meta_dirs: Vec<String>,
    pub data_dirs: Vec<String>,
    pub timezone: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Server {
    pub tcp: Option<Tcp>,
    pub tls: Option<Tls>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Tcp {
    #[serde(default = "Tcp::default_ip_addr")]
    pub ip_addr: String,
    #[serde(default = "Tcp::default_port")]
    pub port: u16,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Tls {
    #[serde(default = "Tls::default_ip_addr")]
    pub ip_addr: String,
    #[serde(default = "Tls::default_port")]
    pub port: u16,
    pub certificate_file: String,
    pub private_key_file: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct CHGroup {
    pub members: Vec<CHConfig>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct CHConfig {
    pub ip_addr: Option<String>,
    pub host: Option<String>,
    #[serde(default = "CHConfig::default_port")]
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    #[serde(default = "CHConfig::default_pool_min_size")]
    pub pool_min_size: u16,
    #[serde(default = "CHConfig::default_pool_max_size")]
    pub pool_max_size: u16,
    pub compression: Option<String>,
    #[serde(default = "CHConfig::default_ping")]
    pub ping: bool,
}

impl CHConfig {
    fn default_port() -> u16 {
        9000
    }

    fn default_pool_min_size() -> u16 {
        1
    }

    fn default_pool_max_size() -> u16 {
        4
    }

    fn default_ping() -> bool {
        false
    }
}

impl Tcp {
    fn default_ip_addr() -> String {
        "127.0.0.1".to_string()
    }

    fn default_port() -> u16 {
        9000u16
    }
}

impl Default for Tcp {
    fn default() -> Self {
        Tcp {
            ip_addr: Self::default_ip_addr(),
            port: Self::default_port(),
            ..Default::default()
        }
    }
}

impl Tls {
    fn default_ip_addr() -> String {
        "127.0.0.1".to_string()
    }

    fn default_port() -> u16 {
        9440u16
    }
}

impl Default for Tls {
    fn default() -> Self {
        Tls {
            ip_addr: Self::default_ip_addr(),
            port: Self::default_port(),
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Storage {
    pub data_dirs_clickhouse: String,
}

impl Default for Storage {
    fn default() -> Self {
        Storage {
            data_dirs_clickhouse: String::new(),
        }
    }
}

impl Conf {
    pub fn load(load_path: Option<&str>) -> Option<Conf> {
        let conf_opt = match load_path {
            None => {
                let path = env::current_exe().unwrap();
                let path = path.parent().unwrap();
                // println!("The current exe dir is {}", path.display());
                validate_path(path.join("conf/base.conf").to_str().unwrap())
            }
            Some(path) => validate_path(path),
        };

        // if let Some(conf_path) = conf_opt {
        //     let config: Conf =
        //         toml::from_str(&fs::read_to_string(conf_path).unwrap()).unwrap();
        //     Some(config)
        // } else {
        //     None
        // }
        conf_opt.map(|p| toml::from_str(&fs::read_to_string(p).unwrap()).unwrap())
    }
    pub fn save(conf: &Conf, save_path: Option<&str>) -> MetaResult<()> {
        let conf_path = match save_path {
            None => {
                let path = env::current_exe().map_err(|_| MetaError::ConfLoadingError)?;
                let path = path.parent().unwrap().join("conf");
                fs::create_dir_all(&path).unwrap();
                path.join("base.conf")
            }
            Some(path) => validate_path(path).expect("can not find conf file"),
        };

        let toml =
            toml::to_string_pretty(&conf).map_err(|_| MetaError::ConfLoadingError)?;
        fs::write(conf_path, toml).map_err(|_| MetaError::ConfLoadingError)?;

        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {
    use crate::errs::MetaResult;

    use super::Conf;

    #[test]
    fn basic_check_for_conf_str() -> MetaResult<()> {
        let conf0: super::Conf = toml::from_str(
            r#"[system]
            meta_dirs = ["/tmp/tb_schema"]
            data_dirs = ["/tmp/tb_data"]
            timezone = "Etc/GMT-8"
            
            [storage]
            data_dirs_clickhouse = ""
            
            # enable TCP service 
            [server.tcp]
            ip_addr = "localhost"
            port = 9528
            
            # enable TLS-based TCP service, in the same time of above TCP
            # warning: here all files in certs are just test certificates, 
            #          do not use them in production!
            [server.tls]
            ip_addr = "localhost"
            port = 9440
            certificate_file = "../certs/cert.pem"
            private_key_file = "../certs/key.pem"
        "#,
        )
        .unwrap();
        println!("{}", toml::to_string_pretty(&conf0).unwrap());
        Conf::save(&conf0, None).unwrap();
        let conf1 = Conf::load(None).unwrap();
        assert_eq!(conf0, conf1);

        Ok(())
    }
}
