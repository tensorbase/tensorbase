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

pub type AnyResult<T> = core::result::Result<T, Box<dyn std::error::Error>>;

use thiserror::Error;
use std::{backtrace::Backtrace, num};

/// Result type
pub type Result<T> = std::result::Result<T, Error>;

//FIXME Errors could be splitted in future
#[derive(Debug, Error)]
pub enum Error {
    #[error("Parameter '{0}' is illegal")]
    IllegalParameterError(String),

    #[error("Parameter '{0}' is is required when it shouldn't be")]
    IllegalRequiredError(String),

    #[error("Parameter '{0}' is defaulted when it's also required")]
    IllegalDefaultError(String),

    #[error("Missing required argument '{0}' for command '{1}'")]
    MissingRequiredArgument(String, String),

    #[error("Command '{0}' can have no more than {1} arguments")]
    TooManyArguments(String, usize),

    #[error("Parse integer error")]
    ParseIntError(#[from] num::ParseIntError),

    #[error("Parse float error")]
    ParseFloatError(#[from] num::ParseFloatError),

    #[error("Common command error: {0}")]
    CommandError(String),

    #[error("Unknown command error: {0}")]
    UnknownCommand(String),

    #[error("Base IO Error")]
    IOError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
}

// impl fmt::Display for Error {
//     fn fmt(
//         &self,
//         f: &mut fmt::Formatter,
//     ) -> std::result::Result<(), fmt::Error> {
//         match self {
//             Error::IllegalParameterError(parameter) => {
//                 write!(f, "Error: Parameter '{}' is invalid", parameter)
//             }

//             Error::IllegalDefaultError(parameter) => write!(
//                 f,
//                 "Error: Parameter '{}' cannot have a default",
//                 parameter
//             ),
//             Error::IllegalRequiredError(parameter) => {
//                 write!(f, "Error: Parameter '{}' cannot be required", parameter)
//             }
//             Error::MissingRequiredArgument(command, parameter) => write!(
//                 f,
//                 "Error: Missing required argument '{}' for command '{}'",
//                 parameter, command
//             ),
//             Error::TooManyArguments(command, nargs) => write!(
//                 f,
//                 "Error: Command '{}' can have no more than {} arguments",
//                 command, nargs,
//             ),
//             Error::ParseFloatError(error) => write!(f, "Error: {}", error,),
//             Error::ParseIntError(error) => write!(f, "Error: {}", error,),
//             Error::CommandError(error) => write!(f, "Error: {}", error),
//             Error::UnknownCommand(command) => {
//                 write!(f, "Error: Unknown command '{}'", command)
//             }
//         }
//     }
// }

// impl From<num::ParseIntError> for Error {
//     fn from(error: num::ParseIntError) -> Self {
//         Error::ParseIntError(error)
//     }
// }

// impl From<num::ParseFloatError> for Error {
//     fn from(error: num::ParseFloatError) -> Self {
//         Error::ParseFloatError(error)
//     }
// }
