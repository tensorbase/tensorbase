use sled::transaction::TransactionError;
use thiserror::Error;

/// Result type
pub type MetaResult<T> = std::result::Result<T, MetaError>;
#[derive(Debug, Error)]
pub enum MetaError {
    #[error("Generic meta store error")]
    GenericError,

    #[error("Error when loading toml config file")]
    ConfLoadingError,

    #[error("Error when opening meta store")]
    OpenError,

    #[error(
        "Error when id generation, this should not happen, please feedback to the developers"
    )]
    IdGenError,

    #[error("Error when insert into meta store")]
    InsertError,

    #[error("Failed to assert None when insert into meta store")]
    AssertNoneErrorWhenInsert,

    #[error("Error when get value from meta store")]
    GetError,

    #[error("Error when del entity from meta store")]
    EntityDelError,

    #[error("System level entities (like system, default database) can not removed")]
    SystemLevelEntitiesCanNotRemoved,
    
    #[error(
        "Entity should exist but can not be read. Contact with developers for help."
    )]
    EntityShouldExistButNot,

    // #[error("Database [{0}] has existed")]
    // DbExistedError(u64),

    #[error("Entity [{0}] has existed")]
    EntityExistedError(u64),

    #[error("Column [{0}] has existed")]
    ColExistedError(u64),

    #[error("Database has not existed")]
    DbNotExistedError,

    #[error("Error when converting str into BqlType enum")]
    UnknownBqlTypeConversionError,

    #[error(
        "Invalid precision [{0}] (should be in [1, 76]) \
        or scale [{1}] (should be in [0, precision]) of decimal type"
    )]
    InvalidPrecisionOrScaleOfDecimalError(u8, u8),

    #[error("Unsupported Bql type error")]
    UnsupportedBqlTypeError,

    #[error("Error when converting str into EngineType enum")]
    UnknownEngineTypeConversionError,

    #[error("Meta store got type is not expected")]
    StoreGotTypeNotExpectedError,

    #[error("No fixed size for dynamic sized data type")]
    NoFixedSizeDataTypeError,

    #[error("Too long length for String")]
    TooLongLengthForStringError,

    #[error("Get offset error in ps")]
    GetOffsetErrorInPartStore,

    #[error("Error when getting part info")]
    GetPartInfoError,

    #[error("Error when inserting part info")]
    InsertPartInfoError,

    #[error("Can not find part error")]
    CanNotFindPartError,

    // #[error("Error when getting fd size from ps")]
    // GetFdSizeError,

    #[error("TransactionError [{0}] happened")]
    WrappingTransactionError(String),

    #[error(transparent)]
    WrappingIOError(#[from] std::io::Error),

    #[error(transparent)]
    WrappingBaseError(#[from] base::errs::BaseError),

    #[error("No enough space for cache")]
    NoEnoughCacheSpace,

    #[error("Fail to lock table")]
    FailToLockTable,

    #[error("The entity should has lock but not")]
    ShouldHasLockButNot,

    #[error("Option is None But should not")]
    OptionIsNoneButShouldNot,
}


impl<T: std::fmt::Display> From<TransactionError<T>> for MetaError {
    fn from(te: TransactionError<T>) -> Self {
        MetaError::WrappingTransactionError(format!("{}", te))
    }
}