use engine::errs::EngineError;
use thiserror::Error;

/// Result type
/// FIXME it is assumed that it is unlikely to meet IncompleteWireFormat in messaging
/// except for Data message  
pub type BaseRtResult<T> = std::result::Result<T, BaseRtError>;
#[derive(Debug, Error)]
pub enum BaseRtError {
    #[error("Generic TensorBase server error")]
    GenericError,

    #[error("Unsupported client message")]
    UnsupportedClientMessage,

    #[error("Unsupported client version")]
    UnsupportedClientVersion,

    #[error("Unsupported column value conversion")]
    UnsupportedValueConversion,

    #[error("Unsupported block compression format")]
    UnsupportedBlockCompressionFormat,

    #[error("Unsupported command")]
    UnsupportedCommand,

    #[error("Unsupported functionality")]
    UnsupportedFunctionality,

    #[error("Unsupported functionality: {0}")]
    UnsupportedFunctionality2(&'static str),

    #[error("Unsupported low c0ardinality dictionary version")]
    UnsupportedLowCardinalityDictVersion,

    #[error("Unsupported partition key type")]
    UnsupportedPartitionKeyType,

    #[error("Command parsing error")]
    CommandParsingError,

    #[error("Too big block size")]
    TooBigBlockSize,

    #[error("Too big message size")]
    TooBigMessageSize,

    #[error("Too many partitions in one block")]
    TooManyPartsPerBlock,

    #[error("string value '{:?}' doesn't match enum value", _0)]
    InvalidStringConversion(Vec<u8>),

    #[error("Index out of range")]
    IndexOutOfRange,

    #[error("Incomplete wire format")]
    IncompletedWireFormat,

    #[error("Invalid wire format found: '{0}' ")]
    InvalidWireFormatInVarInt(u64),

    #[error("Invalid wire format in one column")]
    InvalidWireFormatInColumn,

    #[error("Got unexpected message")]
    UnexpectedMessage,

    #[error("Got unexpected failure when option unwrapping")]
    FailToUnwrapOpt,

    #[error("Error '{0}' when reading request")]
    RequestReadError(String),

    #[error("Error '{0}' when writing response")]
    ResponseWriteError(String),

    #[error("Too many incompleted packet re-read")]
    TooManyIncompletedPacketReRead,

    #[error("Block decompression error")]
    BlockDecompressionError,

    #[error("Block Compression error")]
    BlockCompressionError,

    #[error("BMS not available")]
    BMSNotAvailableError,

    #[error("Multiquery not supported currently")]
    MultiqueryNotSupportedError,

    #[error("Server command error")]
    CommandError,

    #[error("Database does not exist")]
    DatabaseNotExist,

    #[error("Table does not exist")]
    TableNotExist,

    #[error("Column does not exist")]
    ColumnNotExist,

    #[error("Unsupported conversion to BqlType")]
    UnsupportedConversionToBqlType,

    #[error("No partition key column found when inserting")]
    NoPartitionKeyColumnFoundWhenInserting,

    #[error("Multiple partition keys are not supported now")]
    MultiplePartitionKeyNotSupported,

    #[error("Expression compilation error")]
    LightJitCompilationError,

    #[error(
        "Schema info should exist but can not be read. Contact with developers for help."
    )]
    SchemaInfoShouldExistButNot,

    #[error("Invalid format for insert into value list")]
    InvalidFormatForInsertIntoValueList,

    #[error("Insert into value parsing error")]
    InsertIntoValueParsingError,

    #[error(transparent)]
    WrappingBaseError(#[from] base::errs::BaseError),

    #[error(transparent)]
    WrappingLangError(#[from] lang::errs::LangError),

    #[error(transparent)]
    WrappingMetaError(#[from] meta::errs::MetaError),

    #[error(transparent)]
    WrappingEngineError(#[from] engine::errs::EngineError),

    #[error(transparent)]
    WrappingIoError(#[from] std::io::Error),

    #[error(transparent)]
    WrappingArrowError(#[from] arrow::error::ArrowError),
}

impl BaseRtError {
    pub fn to_err_code(&self) -> u32 {
        match self {
            BaseRtError::GenericError => 1,
            BaseRtError::IncompletedWireFormat => 2,
            BaseRtError::WrappingLangError(_) => 3,
            BaseRtError::WrappingMetaError(_) => 4,
            BaseRtError::WrappingIoError(_) => 5,
            BaseRtError::WrappingEngineError(_) => 6,
            BaseRtError::WrappingBaseError(_) => 7,
            BaseRtError::WrappingArrowError(_) => 8,

            BaseRtError::UnsupportedClientMessage => 21,
            BaseRtError::UnsupportedClientVersion => 22,
            BaseRtError::UnsupportedValueConversion => 23,
            BaseRtError::UnsupportedCommand => 24,
            BaseRtError::UnsupportedFunctionality => 25,
            BaseRtError::UnsupportedFunctionality2(_) => 26,
            BaseRtError::UnsupportedLowCardinalityDictVersion => 27,
            BaseRtError::UnsupportedPartitionKeyType => 28,
            BaseRtError::UnsupportedConversionToBqlType => 29,

            BaseRtError::InvalidStringConversion(_) => 100,
            BaseRtError::IndexOutOfRange => 101,
            BaseRtError::InvalidWireFormatInVarInt(_) => 102,
            BaseRtError::InvalidWireFormatInColumn => 103,
            BaseRtError::UnexpectedMessage => 104,
            BaseRtError::FailToUnwrapOpt => 105,

            BaseRtError::RequestReadError(_) => 201,
            BaseRtError::TooManyIncompletedPacketReRead => 202,
            BaseRtError::UnsupportedBlockCompressionFormat => 203,
            BaseRtError::TooBigBlockSize => 204,
            BaseRtError::TooBigMessageSize => 205,
            BaseRtError::BlockDecompressionError => 206,
            BaseRtError::BlockCompressionError => 207,
            BaseRtError::TooManyPartsPerBlock => 208,

            BaseRtError::ResponseWriteError(_) => 301,

            BaseRtError::BMSNotAvailableError => 401,
            BaseRtError::MultiqueryNotSupportedError => 402,
            BaseRtError::CommandError => 403,
            BaseRtError::CommandParsingError => 404,
            BaseRtError::SchemaInfoShouldExistButNot => 405,
            BaseRtError::DatabaseNotExist => 406,
            BaseRtError::TableNotExist => 407,
            BaseRtError::ColumnNotExist => 408,
            BaseRtError::LightJitCompilationError => 409,
            BaseRtError::NoPartitionKeyColumnFoundWhenInserting => 410,
            BaseRtError::MultiplePartitionKeyNotSupported => 411,
            BaseRtError::InvalidFormatForInsertIntoValueList => 412,
            BaseRtError::InsertIntoValueParsingError => 413,
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::BaseRtError;

    #[test]
    fn test_errors() {
        // assert_eq!(
        //     format!("{}", BaseRtError::GenericError),
        //     "Generic Base server side error"
        // );
        println!("{:?}", BaseRtError::GenericError);
    }
}
