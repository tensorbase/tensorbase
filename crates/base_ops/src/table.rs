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

//NAIVE just PoC, will change when storage layer kicks in

use clap::Clap;
use ir::meta::{
    conf::Conf,
    schemas::{parse_creat_table, Catalog, ColumnType}, CAT,
};
use ir::parse::{pretty_parse_tree, BqlParser, Pair, Parser, Rule};

use base::contract;
use std::fs;
use std::path::Path;

/// import table schemas into TensorBase storage
#[derive(Clap)]
pub(crate) struct Create {
    #[clap(
        short,
        about = "[FIXME]create table schemas defined in a ddl script. Example: \
        samples/nyc_taxi_create_table_sample.sql"
    )]
    create: String,
}

pub(crate) fn create_table(create: Create) {
    let ddl_file = create.create;
    let ddl_path = Path::new(&ddl_file);
    contract!(ddl_path.exists());
    let ddl = fs::read_to_string(ddl_path).unwrap();
    let tabs = parse_creat_table(&ddl);
    let mut cat = CAT.lock().unwrap();
    for t in tabs {
        let tn = t.name.clone();
        let has_added = cat.add(t);
        assert!(has_added, "table: {} existed!", &tn);
        log::info!("table: {} has been created.", tn);
    }
    let conf = Conf::load(None).unwrap();
    let schema_dir = conf.schema.schema_dir;
    cat.save(&schema_dir);
}

#[cfg(test)]
mod tests_schema {
    //     use base::errors::Result;
    //     use test_base::fs as tfs;

    //     #[test]
    //     pub fn test_create_table() -> Result<()> {
    //         // let p = env!("CARGO_MANIFEST_DIR");
    //         // let nyc_create_ddl_path = format!(
    //         //     "{}{}",
    //         //     env!("CARGO_MANIFEST_DIR"),
    //         //     "/../base/sample/sample_nyc_taxi_create_table.sql"
    //         // );
    //         // let nyc_create_ddl =
    // tfs::read_as_string(&nyc_create_ddl_path)?;         let
    // nyc_create_ddl = r"-- create nyc taxi table schema CREATE TABLE
    // nyc_taxi (             trip_id INT32 PRIMARY KEY,
    //             pickup_datetime UNIX_TIMESTAMP NOT NULL,
    //             total_amount INT32 NOT NULL
    // )
    // ";
    //         // super::process_ddl(nyc_create_ddl);

    //         Ok(())
    //     }
}
