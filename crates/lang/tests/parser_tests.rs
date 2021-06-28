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

#![feature(const_fn_union, untagged_unions, const_raw_ptr_deref)]

use lang::parse::pretty_parse_tree;
use lang::parse::BqlParser;
use lang::parse::Rule;
use lang::parse::Rule::cmd;
use pest::Parser;
use std::fs::{read_dir, read_to_string, DirEntry};
use std::io;
use std::path::Path;

mod common;

#[test]
fn integ_test_parse_tpcds_20() {
    let dir_perfix = common::get_test_root() + "tpcds_20";
    let paths = read_dir(dir_perfix).unwrap();
    paths.for_each(|p: io::Result<DirEntry>| {
        let p = p.unwrap().path();
        let f = p.file_name().unwrap();
        if !f.to_str().unwrap().ends_with("_over") {
            if f == "query1.sql" {
                println!("\n----- for {:?}:", f);
                let c = read_to_string(p).unwrap();
                let pairs = BqlParser::parse(Rule::cmd, c.as_ref())
                    .unwrap_or_else(|e| panic!("{}", e));
                println!("{}", pretty_parse_tree(pairs));
            }
        }
    })
}

#[test]
fn parse_tpch() {
    let dir_perfix = common::get_test_root() + "tpch/q";
    for i in 1..=22 {
        // for i in 1..=1 {
        let f = format!("{}{}{}", dir_perfix, i, ".sql");
        if Path::new(&f).exists() {
            println!("\n----- for {:?}:", f);
            let c = read_to_string(f).unwrap();
            let pairs = BqlParser::parse(Rule::cmd, c.as_ref())
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }
    }
}

// #[test]
fn integ_test_parse_tpcds_full() {
    let paths = read_dir(common::get_test_root() + "tpcds_full").unwrap();
    paths.for_each(|p: io::Result<DirEntry>| {
        let p = p.unwrap().path();
        println!("----- for {:?}:", p.file_name());
        let s = read_to_string(p).unwrap();
        let cmd_node =
            BqlParser::parse(cmd, s.as_ref()).unwrap_or_else(|e| panic!("{}", e));
        // println!("  {}\n", cmd_node);
    })
}
