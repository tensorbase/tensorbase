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
use thiserror::Error;

/// Result type
pub type BaseResult<T> = std::result::Result<T, BaseError>;
#[derive(Debug, Error)]
pub enum BaseError {
    #[error("Generic meta store error")]
    GenericError,

    #[error(transparent)]
    WrappingIOError(#[from] std::io::Error),

    #[error("Can not convert the unixtime to valid ymd")]
    UnixtimeToYmdError,

    #[error("Invalid datetime format")]
    InvalidDatetimeFormat,

    #[error("Invalid datetime digit")]
    InvalidDatetimeDigit,

    #[error("Invalid time zone {0}")]
    InvalidTimeZone(String),

    #[error("Failed to mmap")]
    FailedToMmap,

    #[error("Failed to mremap")]
    FailedToMremap,

    #[error("Encoding too long string")]
    EncodingTooLongString,

    #[error("Can not be evaluted")]
    CanotEval,
}
