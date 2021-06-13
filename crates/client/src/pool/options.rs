use crate::errors::UrlError;
use std::convert::TryFrom;
use std::fmt;
use std::{borrow::Cow, str::FromStr, time::Duration};
use url::Url;

type Result<T> = std::result::Result<T, UrlError>;

const DEFAULT_MIN_POOL_SIZE: u16 = 2;
const DEFAULT_MAX_POOL_SIZE: u16 = 10;

#[derive(Copy, Clone, PartialEq, Debug)]
/// At the moment Clickhouse_driver supports only LZ4 compression method.
/// It's used by default in Clickhouse 20.x
pub enum CompressionMethod {
    None,
    LZ4,
}

impl CompressionMethod {
    #[inline(always)]
    pub fn is_none(&self) -> bool {
        matches!(self, CompressionMethod::None)
    }
}

/// Clickhouse connection options.
#[derive(Clone)]
pub struct Options {
    /// Address of clickhouse server (defaults to `127.0.0.1:9000`).
    pub(crate) addr: Vec<String>,
    /// Database name. (defaults to `default`).
    pub(crate) database: String,
    /// User name (defaults to `default`).
    pub(crate) username: String,
    /// Access password (defaults to `""`).
    pub(crate) password: String,
    /// Enable compression (defaults to `false`).
    pub(crate) compression: CompressionMethod,
    /// Lower bound of opened connections for `Pool` (defaults to 10).
    pub(crate) pool_min: u16,
    /// Upper bound of opened connections for `Pool` (defaults to 20).
    pub(crate) pool_max: u16,
    /// Whether to enable `TCP_NODELAY` (defaults to `true`).
    //pub(crate) nodelay: bool,
    /// TCP keep alive timeout in milliseconds (defaults to `None`).
    pub(crate) keepalive: Option<Duration>,
    /// Ping server every time before execute any query. (defaults to `true`)
    pub(crate) ping_before_query: bool,
    /// Timeout for ping (defaults to `500 ms`)
    pub(crate) ping_timeout: Duration,
    /// Timeout for connection (defaults to `500 ms`)
    pub(crate) connection_timeout: Duration,
    /// Timeout for queries (defaults to `180 sec`)
    pub(crate) query_timeout: Duration,
    /// Timeout for each block in a query (defaults to `180 sec`)
    pub(crate) query_block_timeout: Duration,
    /// Timeout for inserts (defaults to `180 sec`)
    pub(crate) insert_timeout: Duration,
    /// Timeout for execute (defaults to `180 sec`)
    pub(crate) execute_timeout: Duration,
    /// Enable TLS encryption (defaults to `false`)
    #[cfg(feature = "tls")]
    pub(crate) secure: bool,
    /// Skip certificate verification (default is `false`).
    #[cfg(feature = "tls")]
    pub(crate) skip_verify: bool,

    /// An X509 certificate.
    //#[cfg(feature = "tls")]
    //pub(crate) certificate: Option<Certificate>,

    /// Restricts permissions for read data, write data and change settings queries.
    pub(crate) readonly: u8,

    /// The number of retries to send request to server. (defaults to `3`)
    pub(crate) send_retries: u8,

    /// Amount of time to wait before next retry. (defaults to `1 sec`)
    pub(crate) retry_timeout: Duration,
}

// FIXME: replace with macro
fn parse_param<'a, F, T, E>(param: Cow<'a, str>, value: Cow<'a, str>, parse: F) -> Result<T>
where
    F: Fn(&str) -> std::result::Result<T, E>,
{
    match parse(value.as_ref()) {
        Ok(value) => Ok(value),
        Err(_) => Err(UrlError::InvalidParamValue {
            param: param.into(),
            value: value.into(),
        }),
    }
}

fn get_database_from_url(url: &Url) -> Result<Option<&str>> {
    match url.path_segments() {
        None => Ok(None),
        Some(mut segments) => {
            let head = segments.next();

            if segments.next().is_some() {
                return Err(UrlError::Invalid);
            }

            match head {
                Some(database) if !database.is_empty() => Ok(Some(database)),
                _ => Ok(None),
            }
        }
    }
}

fn parse_duration(source: &str) -> Result<Duration> {
    let (num, unit) = match source.find(|c: char| !c.is_digit(10)) {
        Some(pos) if pos > 0 => (u64::from_str(&source[0..pos]), &source[pos..]),
        None => (u64::from_str(source), "s"),
        _ => {
            return Err(UrlError::Invalid);
        }
    };

    let num = match num {
        Ok(value) => value,
        Err(_) => return Err(UrlError::Invalid),
    };

    match unit {
        "s" => Ok(Duration::from_secs(num)),
        "ms" => Ok(Duration::from_millis(num)),
        _ => Err(UrlError::Invalid),
    }
}

fn parse_opt_duration(source: &str) -> Result<Option<Duration>> {
    if source == "none" {
        return Ok(None);
    }

    let duration = parse_duration(source)?;
    Ok(Some(duration))
}

fn parse_u8(source: &str) -> Result<u8> {
    let duration: u8 = match source.parse() {
        Ok(value) => value,
        Err(_) => return Err(UrlError::Invalid),
    };

    Ok(duration)
}

fn parse_compression(source: &str) -> Result<CompressionMethod> {
    match source {
        "none" => Ok(CompressionMethod::None),
        "lz4" => Ok(CompressionMethod::LZ4),
        _ => Err(UrlError::Invalid),
    }
}

impl Options {
    fn new(url: Url) -> Result<Options> {
        let defport = match url.scheme() {
            "tcp" => 9000,
            "tls" => 9009,
            _ => {
                return Err(UrlError::UnsupportedScheme {
                    scheme: url.scheme().to_string(),
                })
            }
        };

        let mut options = crate::DEF_OPTIONS.clone(); // Options ::default();

        let user = url.username();
        if !user.is_empty() {
            options.username = user.into();
        }

        if let Some(password) = url.password() {
            options.password = password.into();
        }

        let port = url.port().unwrap_or(defport);
        if url.cannot_be_a_base() || !url.has_host() {
            return Err(UrlError::Invalid);
        }

        options.addr.clear();
        options.addr.push(format!(
            "{}:{}",
            url.host_str().unwrap_or("localhost"),
            port
        ));

        if let Some(database) = get_database_from_url(&url)? {
            options.database = database.into();
        }

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "pool_min" => options.pool_min = parse_param(key, value, u16::from_str)?,
                "pool_max" => options.pool_max = parse_param(key, value, u16::from_str)?,
                "keepalive" => options.keepalive = parse_param(key, value, parse_opt_duration)?,
                "ping_before_query" => {
                    options.ping_before_query = parse_param(key, value, bool::from_str)?
                }
                "send_retries" => options.send_retries = parse_param(key, value, u8::from_str)?,
                "retry_timeout" => options.retry_timeout = parse_param(key, value, parse_duration)?,
                "ping_timeout" => options.ping_timeout = parse_param(key, value, parse_duration)?,
                "connection_timeout" => {
                    options.connection_timeout = parse_param(key, value, parse_duration)?
                }
                "query_timeout" => options.query_timeout = parse_param(key, value, parse_duration)?,
                "query_block_timeout" => {
                    options.query_block_timeout = parse_param(key, value, parse_duration)?
                }
                "insert_timeout" => {
                    options.insert_timeout = parse_param(key, value, parse_duration)?
                }
                "execute_timeout" => {
                    options.execute_timeout = parse_param(key, value, parse_duration)?
                }
                "compression" => options.compression = parse_param(key, value, parse_compression)?,
                #[cfg(feature = "tls")]
                "secure" => options.secure = parse_param(key, value, bool::from_str)?,
                #[cfg(feature = "tls")]
                "skip_verify" => options.skip_verify = parse_param(key, value, bool::from_str)?,
                "readonly" => options.readonly = parse_param(key, value, parse_u8)?,
                "host" => options.addr.push(value.into_owned()),
                _ => return Err(UrlError::UnknownParameter { param: key.into() }),
            };
        }

        Ok(options)
    }

    pub fn set_compression(mut self, compression: CompressionMethod) -> Self {
        self.compression = compression;
        self
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.ping_timeout = timeout;
        self.execute_timeout = timeout;
        self.query_timeout = timeout;
        self.insert_timeout = timeout;
        self
    }
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Options")
            .field("addr", &self.addr)
            .field("database", &self.database)
            .field("compression", &self.compression)
            .field("pool_min", &self.pool_min)
            .field("pool_max", &self.pool_max)
            .field("keepalive", &self.keepalive)
            .field("ping_before_query", &self.ping_before_query)
            .field("send_retries", &self.send_retries)
            .field("retry_timeout", &self.retry_timeout)
            .field("ping_timeout", &self.ping_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("query_block_timeout", &self.query_block_timeout)
            .field("insert_timeout", &self.insert_timeout)
            .field("execute_timeout", &self.execute_timeout)
            .field("readonly", &self.readonly)
            .finish()
    }
}

impl Default for Options {
    fn default() -> Self {
        let default_duration = Duration::from_secs(180);
        Self {
            addr: vec!["localhost:9000".into()],
            database: "default".into(),
            username: "default".into(),
            password: "".into(),
            compression: CompressionMethod::LZ4,
            pool_min: DEFAULT_MIN_POOL_SIZE,
            pool_max: DEFAULT_MAX_POOL_SIZE,
            keepalive: None,
            ping_before_query: true,
            send_retries: 3,
            retry_timeout: Duration::from_secs(1),
            ping_timeout: Duration::from_millis(700),
            connection_timeout: Duration::from_millis(500),
            query_timeout: default_duration,
            query_block_timeout: default_duration,
            insert_timeout: default_duration,
            execute_timeout: default_duration,
            #[cfg(feature = "tls")]
            secure: false,
            #[cfg(feature = "tls")]
            skip_verify: false,
            readonly: 0,
        }
    }
}

impl TryFrom<Url> for Options {
    type Error = UrlError;
    fn try_from(value: Url) -> Result<Self> {
        Options::new(value)
    }
}

/// Weird template TryFrom<T> implementation collision
/// ( https://github.com/rust-lang/rust/issues/50133 )
/// with TryFrom<&Url> make us to draw up two separate implementations
/// for &str and String
impl TryFrom<&str> for Options {
    type Error = UrlError;

    fn try_from(value: &str) -> Result<Self> {
        let url = Url::parse(value)?;
        Options::new(url)
    }
}

impl TryFrom<String> for Options {
    type Error = UrlError;

    fn try_from(value: String) -> Result<Self> {
        let url = Url::parse(value.as_ref())?;
        Options::new(url)
    }
}

impl Options {
    pub(crate) fn take_addr(&mut self) -> Vec<String> {
        std::mem::replace(&mut self.addr, Vec::new())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pool::Pool;

    #[test]
    fn test_default_config() -> Result<()> {
        let pool = Pool::create("tcp://localhost?ping_timeout=1ms").unwrap();

        assert_eq!(pool.options().database, "default");
        assert_eq!(pool.options().compression, CompressionMethod::LZ4);
        assert_eq!(pool.options().username, "default");
        assert_eq!(pool.options().password, "");
        assert_eq!(pool.inner.hosts[0], "localhost:9000");
        Ok(())
    }

    #[test]
    fn test_configuration() -> Result<()> {
        let url =
            Url::parse("tcp://localhost/db1?query_block_timeout=300&ping_timeout=110ms&query_timeout=25s&compression=lz4")?;
        let config = Options::new(url)?;

        assert_eq!(config.addr[0], String::from("localhost:9000"));
        assert_eq!(
            config.compression,
            CompressionMethod::LZ4,
            "compression url parameter"
        );
        assert_eq!(config.query_timeout, Duration::from_secs(25));
        assert_eq!(config.ping_timeout, Duration::from_millis(110));
        assert_eq!(config.query_block_timeout, Duration::from_secs(300));
        assert_eq!(config.database, "db1");

        let url = Url::parse(
            "tcp://host1:9001/db2?ping_timeout=110ms&query_timeout=25s&compression=lz4",
        )?;

        assert_eq!(url.host_str(), Some("host1"));
        assert_eq!(url.port(), Some(9001));

        let config = Options::new(url)?;
        assert_eq!(config.addr[0], String::from("host1:9001"));

        let url = Url::parse(
            "tcp://host1,host2:9001/db2?ping_timeout=110ms&query_timeout=25s&compression=lz4",
        )?;

        assert_eq!(url.host_str(), Some("host1,host2"));

        let url =
            Url::parse("tcp://host1:9001/db2?ping_timeout=ms&query_timeout=25s&compression=lz4")?;
        assert!(Options::new(url).is_err());

        let url =
            Url::parse("tcp://host1:9001/db2?ping_timeout=1ms&query_timeout=25s&compression=zlib")?;
        assert!(Options::new(url).is_err());

        let url = Url::parse(
            "tcp://host1:9001/db2?ping_timeout=1ms&query_timeout=25s&pool_min=11&pool_max=10",
        )?;
        let url2 = url.clone();
        let option = Options::new(url);
        assert!(option.is_ok());
        assert!(Pool::create(url2).is_err());

        Ok(())
    }
}
