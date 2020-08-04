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
mod repl;

use base::errors::*;
use colored::*;
use repl::Repl;
use repl::{Command, Convert, Parameter, Value};
use std::collections::HashMap;

type CmdArgs = HashMap<String, Value>;
type CmdRet = base::errors::Result<Option<String>>;
struct ShellState {
    mode: &'static str,
}

impl ShellState {
    fn set_mode(&mut self, mode: &'static str) {
        self.mode = mode;
    }
}

static mut GLOBAL_STATE: ShellState = ShellState { mode: "query" };

fn mode(args: CmdArgs, state: &mut ShellState) -> CmdRet {
    let mode: String = args["mode_type"].convert()?;
    macro_rules! mode_arm {
        ($($mode_type:expr,)*) => {
            match mode.as_str() {
            $(
            $mode_type =>  {
                state.set_mode($mode_type);
                Ok(Some(format!("switch to {} mode", $mode_type)))
            }

            )*
            _ => Err(Error::IllegalParameterError(mode))
            }
        };
    }
    mode_arm!("query", "edit",)
}

fn main() -> AnyResult<()> {
    unsafe {
        //FIXME
        let mut repl = Repl::new(&mut GLOBAL_STATE, || {
            macro_rules! prompt_color_match {
                ($($mode_type:expr => $color:ident,)*) => {
                    match GLOBAL_STATE.mode {
                    $(
                        $mode_type => Box::new(
                            (String::from("[") + GLOBAL_STATE.mode + "]>")
                                .$color()
                                .bold(),
                           ),
                    )*
                    _ => unreachable!()
                    }
                };
            }
            prompt_color_match!("query" => green,"edit" => magenta, )
        })
        .with_name("Base Shell")
        .with_version("0.1.0")
        .with_description("Base Shell - the command-line client for TensorBase")
        .add_command(
            Command::new(":mode", mode)
                .with_parameter(
                    Parameter::new("mode_type").set_required(true)?,
                )?
                .with_help("toggle shell mode between 'query' and 'edit'"),
        )
        .add_command(
            Command::new(":exit", |_, _| std::process::exit(0))
                .with_help("toggle shell mode between 'query' and 'edit'"),
        );
        repl.run()
    }
}
