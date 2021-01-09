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
#![allow(unused_variables, dead_code)]
mod import;
mod table;

use clap::Clap;
use simplelog::*;
use std::{fs, path::Path};

#[derive(Clap)]
#[clap(
    version = "0.1",
    author = "TensorBase, and its contributors",
    about = "TensorBase Devops Tool(Early Preview)",
    max_term_width = 80
)]
struct Opts {
    // /// [TODO]Optionally specify a custom config file.
    // #[clap(short, long, default_value = "")]
    // config: String,
    /// A level of verbosity, and can be used multiple times
    // #[clap(short, long, parse(from_occurrences))]
    // verbose: i32,
    #[clap(subcommand)]
    subcmd: OpsSubCommand,
}

#[derive(Clap)]
enum OpsSubCommand {
    // #[clap(version = "0.1", author = "TensorBase, and its contributors")]
    Import(ImportSubCommand),
    Table(TableSubCommand),
    // Server(Server),
}

/// import all kinds of sources into TensorBase storage
#[derive(Clap)]
struct ImportSubCommand {
    #[clap(subcommand)]
    subcmd: ImportSubCommandKind,
}

#[derive(Clap)]
enum ImportSubCommandKind {
    Csv(import::csv::Csv),
}

/// operate on Table definitions of TensorBase's table, like create a
/// table schema
#[derive(Clap)]
struct TableSubCommand {
    #[clap(subcommand)]
    subcmd: TableSubCommandKind,
}

#[derive(Clap)]
enum TableSubCommandKind {
    Create(table::Create),
}

// /// run the instance of TensorBase
// #[derive(Clap)]
// struct Server {
//     #[clap(
//         short,
//         about = "specify a TensorBase configuration file. Default: . If not
// specified...(not implemented)",         default_value = ""
//     )]
//     conf: String,

//     #[clap(
//         short,
//         about = "true: run in a background daemon;false: run will block
// current termial. Default: false. "     )]
//     background: bool,
// }

fn main() {
    TermLogger::init(
        LevelFilter::Trace,
        Config::default(),
        TerminalMode::Mixed,
    )
    .unwrap();
    let opts: Opts = Opts::parse();

    match opts.subcmd {
        OpsSubCommand::Import(t) => match t.subcmd {
            ImportSubCommandKind::Csv(csv) => {
                import::csv::import(csv);
            }
        },
        OpsSubCommand::Table(tsb) => match tsb.subcmd {
            TableSubCommandKind::Create(c) => {
                table::create_table(c);
            }
        },
        // OpsSubCommand::Server(t) => todo!(),
    }
}
