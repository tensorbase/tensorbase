use std::num::ParseIntError;

use thiserror::Error;

/// Result type
pub type LangResult<T> = std::result::Result<T, LangError>;
#[derive(Debug, Error)]
pub enum LangError {
    #[error("Generic Lang error")]
    GenericError,

    #[error("Error when AST processing")]
    ASTError,

    #[error("Unsupported language feature found")]
    UnsupportedLangFeatureError,

    #[error("Unsupported aggregation function found")]
    UnsupportedAggregationFunction,

    #[error("Unsupported function found")]
    UnsupportedFunction,

    #[error(transparent)]
    WrappingMetaError(#[from] meta::errs::MetaError),

    #[error(transparent)]
    WrappingParseIntError(#[from] ParseIntError),

    #[error("Unsupported system.numbers table naming format value")]
    UnsupportedSystemNumbersNamingFormatError,

    #[error("Error when create database parsing")]
    DatabaseParsingError,

    #[error("Error when create table parsing")]
    CreateTableParsingError,

    #[error("Error when query language parsing")]
    QueryLangParsingError,

    #[error("Unsupported BqlType error")]
    UnsupportedBqlTypeError,
}
