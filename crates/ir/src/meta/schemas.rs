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

use crate::parse::*;
use base::errors::AnyResult;
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
#[repr(u32)]
pub enum ColumnType {
    INIT,
    INT8,
    UINT8,
    INT32,
    UINT32,
    UNIX_DATETIME,
    // UNIX_TIMESTAMP,
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::INIT
    }
}

impl From<&str> for ColumnType {
    fn from(item: &str) -> Self {
        match item {
            "INT8" => ColumnType::INT8,
            "UINT8" => ColumnType::UINT8,
            "INT32" => ColumnType::INT32,
            "UINT32" => ColumnType::UINT32,
            "UNIX_DATETIME" => ColumnType::UNIX_DATETIME,
            _ => todo!("unsupported column type!"),
        }
    }
}

impl ColumnType {
    pub fn size(self) -> u8 {
        match self {
            ColumnType::INT8 | ColumnType::UINT8 => 1,
            ColumnType::INT32
            | ColumnType::UINT32
            | ColumnType::UNIX_DATETIME => 4,
            _ => todo!(),
        }
    }
}

pub type ColumnId = u64;
pub type TableId = u32;
pub type NamespaceId = u32; //?

#[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
pub struct Catalog {
    tables: Vec<Table>,
    // pub col_ids: HashMap<String, ColumnId>,
}

impl Catalog {
    pub fn load(schema_dir: &str) -> AnyResult<Option<Catalog>> {
        let cat_file = PathBuf::from(schema_dir).join("cat");
        if cat_file.exists() {
            let cat: Catalog =
                bincode::deserialize(&fs::read(cat_file)?).unwrap();
            Ok(Some(cat))
        } else {
            Ok(None)
        }
    }

    pub fn save(&self, schema_dir: &str) -> AnyResult<()> {
        let encoded: Vec<u8> = bincode::serialize(self).unwrap();
        let cat_file = PathBuf::from(schema_dir).join("cat");
        fs::write(cat_file, encoded)?;

        Ok(())
    }

    pub fn contains(&mut self, tab_name: &str) -> bool {
        self.tables.iter().any(|t| t.name == tab_name)
    }

    pub fn add(&mut self, mut tab: Table) -> bool {
        if self.contains(&tab.name) {
            false
        } else {
            tab.invalidate(self.tables.len() as u32); //FIXME pass 0?
            self.tables.push(tab);
            true
        }
    }

    pub fn num_tables(&self) -> usize {
        self.tables.len()
    }

    //NAIVE handle namespace?
    pub fn get_table_by_name(&self, name: &str) -> Option<&Table> {
        self.tables.iter().filter(|t| t.name == name).last()
    }
    pub fn get_table_by_id(&self, id: TableId) -> &Table {
        &self.tables[id as usize]
    }
}

struct CreateTabsContext {
    tables: Vec<Table>,
}

impl CreateTabsContext {
    fn parse(&mut self, pair: Pair<Rule>) {
        let r = pair.as_rule();
        //pre
        match r {
            Rule::qualified_table_name => {
                self.tables.push(Default::default());
            }
            Rule::column_def => {
                let tab = self.tables.last_mut().unwrap();
                tab.columns.push(Default::default());
            }
            _ => {}
        }
        for p in pair.clone().into_inner() {
            self.parse(p);
        }
        //post
        match r {
            // database_name ~ ".")? ~ table_name
            Rule::database_name => {
                let tab = self.tables.last_mut().unwrap();
                tab.namespace = pair.as_str().trim().to_owned();
            }
            Rule::table_name => {
                let tab = self.tables.last_mut().unwrap();
                tab.name = pair.as_str().trim().to_owned();
            }
            Rule::column_name => {
                let tab = self.tables.last_mut().unwrap();
                let col = tab.columns.last_mut().unwrap();
                col.name = pair.as_str().trim().to_string();
            }
            Rule::type_name => {
                let tab = self.tables.last_mut().unwrap();
                let col = tab.columns.last_mut().unwrap();
                let typ = pair.as_str().trim().to_string().to_ascii_uppercase();
                col.data_type = typ.as_str().into();
            }
            Rule::column_constraint => {
                let tab = self.tables.last_mut().unwrap();
                let col = tab.columns.last_mut().unwrap();
                let constr = pair.as_str().trim().to_ascii_uppercase();
                match constr.as_str() {
                    "PRIMARY KEY" => col.is_primary_key = true,
                    "NOT NULL" => col.is_nullable = true,
                    _ => todo!(),
                };
            }
            _ => {}
        }
    }
}

pub fn parse_creat_table(ddl: &str) -> Vec<Table> {
    let ps = BqlParser::parse(Rule::cmd_list, ddl)
        .unwrap_or_else(|e| panic!("{}", e));
    // println!("{}", pretty_parse_tree(ps.clone()));

    let ct = ps.into_iter().next().unwrap();
    let mut ctx = CreateTabsContext { tables: vec![] };
    ctx.parse(ct);
    // println!("{:?}", ctx.tables);
    //FIXME need to validate all tabs for malicious ddls
    ctx.tables
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
pub struct Table {
    pub id: TableId,
    pub namespace: String,
    pub namespace_id: NamespaceId,
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn invalidate(&mut self, tab_id: TableId) {
        self.id = tab_id;
        for (idx, col) in self.columns.iter_mut().enumerate() {
            col.id = ((tab_id as u64) << 32) | idx as u64;
        }
    }

    pub fn contains(&mut self, col_name: &str) -> bool {
        self.columns.iter().any(|t| t.name == col_name)
    }

    pub fn add(&mut self, col: Column) -> bool {
        if self.contains(&col.name) {
            false
        } else {
            self.columns.push(col);
            true
        }
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().filter(|c| c.name == name).last()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
pub struct Column {
    pub id: ColumnId,
    pub name: String,
    pub data_type: ColumnType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
}

// #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
// pub struct ColumnMeta {
//     column_id: ColumnId,
//     column_types: ColumnType,
//     column_indexs: u32,
// }

// impl ColumnMeta {
//     pub fn new(
//         column_id: ColumnId,
//         column_types: ColumnType,
//         column_indexs: u32,
//     ) -> ColumnMeta {
//         ColumnMeta {
//             column_id,
//             column_types,
//             column_indexs,
//         }
//     }
// }

#[cfg(test)]
mod unit_tests {}
