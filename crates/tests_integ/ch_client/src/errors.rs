use std::str::Utf8Error;
use std::{borrow::Cow, io, result};

use thiserror::Error;
use url::ParseError;

use crate::types::SqlType;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("{} {} {}", name, code, message)]
pub struct Exception {
    pub name: String,
    pub code: u32,
    pub message: String,
    #[cfg(test)]
    pub(crate) trace: String,
}

/// This type represents Clickhouse server error.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("Server exception {}", _0[0])]
pub struct ServerError(pub Box<[Exception]>);

/// This type enumerates configuration URL errors.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum UrlError {
    #[error("Connection URL parameter `{}' requires feature `{}'", param, feature)]
    FeatureRequired { feature: String, param: String },

    #[error("Invalid or incomplete connection URL")]
    Invalid,

    #[error("Invalid value `{}' for connection URL parameter `{}'.", value, param)]
    InvalidParamValue { param: String, value: String },

    #[error("Invalid pool constraints: pool_min ({}) > pool_max ({}).", min, max)]
    InvalidPoolConstraints { min: u16, max: u16 },

    #[error("URL parse error: {}", _0)]
    Parse(#[source] ParseError),

    #[error("Unknown connection URL parameter `{}'.", param)]
    UnknownParameter { param: String },

    #[error("Unsupported connection URL scheme `{}'.", scheme)]
    UnsupportedScheme { scheme: String },
}

/// This type enumerates clickhouse driver own errors.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum DriverError {
    #[error("Connection to the server is closed.")]
    ConnectionClosed,

    #[error("Connection timeout.")]
    ConnectionTimeout,

    #[error("Packet {} out of order.", _0)]
    PacketOutOfOrder(u64),

    #[error("Pool was disconnected.")]
    PoolDisconnected,

    #[error("Unexpected packet.")]
    UnexpectedPacket { code: u64, payload: Vec<u8> },

    #[error("Unsupported data type.")]
    UnsupportedType(SqlType),

    #[error("Malformed packet data.")]
    BrokenData,

    #[error("Packet too large.")]
    PacketTooLarge,

    #[error("String too long.")]
    StringTooLong,

    #[error("Bad compressed packet header.")]
    BadCompressedPacketHeader,

    #[error("Bad packet hash.")]
    BadHash,

    #[error("Block row count {} exceed the limit.", _0)]
    RowCountTooMany(u64),

    #[error("Other operation in progress.")]
    OperationInProgress,

    #[error("Index out of range.")]
    IndexOutOfRange,

    #[error("Integrity error")]
    IntegrityError,
}

/// This type enumerates clickhouse data conversion  errors.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum ConversionError {
    #[error("Conversion unsupported")]
    UnsupportedConversion,
    #[error("fixed string length doesn't match column type. FixedString({})", _0)]
    FixedStringLengthNotMatch(u32),
    #[error("string value '{:?}' doesn't match enum value", _0)]
    EnumMismatch(Vec<u8>),
    #[error("Unknown column type '{}'", _0)]
    UnknownColumnType(String),
    #[error("Incorrect utf8 byte sequence")]
    Utf8,
}
/// Consolidation of errors
#[derive(Debug, Error)]
pub enum Error {
    #[error("Driver error: `{}'", _0)]
    Driver(#[source] Box<DriverError>),

    #[error("io error: `{}`", _0)]
    Io(#[source] io::Error),

    #[error("Server error: `{}'", _0)]
    Server(#[source] ServerError),

    #[error("URL error: `{}'", _0)]
    Url(#[source] UrlError),

    #[error("Deserialize error: `{}`", _0)]
    Convertion(ConversionError),

    #[error("Other error: `{}`", _0)]
    Other(Cow<'static, str>),
}

impl Error {
    pub fn is_timeout(&self) -> bool {
        if let Error::Driver(berr) = self {
            **berr == DriverError::ConnectionTimeout
        } else {
            false
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        let kind = err.kind();
        if err.get_ref().is_some() {
            match err.into_inner().unwrap().downcast::<DriverError>() {
                Ok(err) => Error::Driver(err),
                _ => Error::Io(io::Error::from(kind)),
            }
        } else {
            Error::Io(err)
        }
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(error) => error,
            e => io::Error::new(io::ErrorKind::Other, e.to_string()),
        }
    }
}

impl From<ParseError> for UrlError {
    fn from(err: ParseError) -> Self {
        UrlError::Parse(err)
    }
}
/// NotAnError
impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        Error::Other(Cow::Borrowed(""))
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::Url(err.into())
    }
}

impl From<Utf8Error> for Error {
    fn from(_err: Utf8Error) -> Self {
        Error::Convertion(ConversionError::Utf8)
    }
}

impl From<DriverError> for Error {
    fn from(err: DriverError) -> Self {
        Error::Driver(Box::new(err))
    }
}

impl From<ServerError> for Error {
    fn from(err: ServerError) -> Self {
        Error::Server(err)
    }
}

impl From<UrlError> for Error {
    fn from(err: UrlError) -> Self {
        Error::Url(err)
    }
}

impl From<ConversionError> for Error {
    fn from(err: ConversionError) -> Self {
        Error::Convertion(err)
    }
}

impl From<&'static str> for Error {
    fn from(err: &'static str) -> Self {
        Error::Other(Cow::Borrowed(err))
    }
}

impl From<()> for Error {
    fn from(_err: ()) -> Self {
        Error::Other(Cow::Borrowed("unknown error"))
    }
}
