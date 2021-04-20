use thiserror::Error;

/// Result type
pub type EngineResult<T> = std::result::Result<T, EngineError>;
#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    WrappingMetaError(#[from] meta::errs::MetaError),

    #[error(transparent)]
    WrappingLangError(#[from] lang::errs::LangError),

    #[error(transparent)]
    WrappingIOError(#[from] std::io::Error),

    #[error("Unexpected gen error which should not happen")]
    UnexpectedGenError,

    #[error("Unexpected data loading error which should not happen")]
    UnexpectedDataLoadingError,

    #[error("Invalid HIR gen result error")]
    InvalidHIRGenResultError,

    #[error("Table does not exist")]
    TableNotExist,

    #[error("Column does not exist")]
    ColumnNotExist,

    #[error("Column info does not exist")]
    ColumnInfoNotExist,

    #[error("Unsupported limit value")]
    UnsupportedLimitValueError,

    #[error("Unsupported Bql type")]
    UnsupportedBqlType,

    #[error(
        "Keys in group by clause can not matched with those in select clause"
    )]
    KeyInGroupByNotMatched,

    #[error("Error when unwrappring Option")]
    UnwrapOptionError,

    #[error("Error when unwrappring Result")]
    UnwrapResultError,

    #[error(transparent)]
    WrappingDFError(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    WrappingArrowError(#[from] arrow::error::ArrowError),
}
