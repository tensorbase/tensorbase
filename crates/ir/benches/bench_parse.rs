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
use bir::parse::BqlParser;
use bir::parse::Rule;
use bir::parse::Rule::cmd;
use pest::Parser;
use std::fs::{read_dir, read_to_string, DirEntry};
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

const PRJ_ROOT: &'static str = env!("CARGO_MANIFEST_DIR");

//  const N: u32 = (1 << 27) ;
fn integ_test_parse_tpcds_full() -> (usize, Duration, u32) {
    let test_res_root = format!("{}/tests/res/tpcds_20", PRJ_ROOT);
    let mut total_len = 0usize;
    let mut total_dur = Duration::default();
    let mut ct = 0u32;
    for _ in 0..10 {
        let paths = read_dir(&test_res_root).unwrap();
        paths.for_each(|p: io::Result<DirEntry>| {
            let p = p.unwrap().path();
            // println!("----- for {:?}:", p.file_name());
            let s = read_to_string(p).unwrap();
            let timer = Instant::now();
            let cmd_node = BqlParser::parse(cmd, s.as_ref())
                .unwrap_or_else(|e| panic!("{}", e));
            total_dur += timer.elapsed();
            total_len += cmd_node.as_str().len();
            ct += 1;
            // println!("  {}\n", cmd_node);
        });
    }
    (total_len, total_dur, ct)
}

fn main() {
    let (len, d, ct) = integ_test_parse_tpcds_full();
    println!(
        "parse {} of chars into {} parse trees in: {:?}",
        len,
        ct,
        d.as_millis()
    );
}
