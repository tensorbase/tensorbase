use thiserror::Error;

/// Result type
pub type EngineResult<T> = std::result::Result<T, EngineError>;
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Unexpected data loading error which should not happen")]
    UnexpectedDataLoadingError,

    #[error("Table does not exist")]
    TableNotExist,

    #[error("Column does not exist")]
    ColumnNotExist,

    #[error("Column info does not exist")]
    ColumnInfoNotExist,

    #[error("Unsupported Bql type")]
    UnsupportedBqlType,

    #[error("Error when unwrappring Option")]
    UnwrapOptionError,

    #[error("Unsupported Query")]
    UnsupportedQuery,

    #[error(transparent)]
    WrappingDFError(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    WrappingArrowError(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    WrappingMetaError(#[from] meta::errs::MetaError),

    #[error(transparent)]
    WrappingLangError(#[from] lang::errs::LangError),

    #[error(transparent)]
    WrappingIOError(#[from] std::io::Error),

    #[error(transparent)]
    WrappingClientError(#[from] client::prelude::errors::Error),
}
