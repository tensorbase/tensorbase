use super::block::Block;
use super::code::*;
use super::encoder::Encoder;
use super::{ServerInfo, ServerWriter};
use std::io::{Result, Write};
use uuid::Uuid;

/// Query encapsulates Clickhouse sql command
pub struct Query {
    pub(crate) id: Option<Uuid>,
    pub(crate) query: String,
}

macro_rules! add_col {
    ($query:ident, $name:expr) => {
        // $query.push_str("`");
        $query.push_str(&$name);
        // $query.push_str("`");
    };
}

impl Query {
    /// Construct `INSERT` statement from Block columns names
    pub(crate) fn from_block(block: &Block) -> Query {
        let mut query: String = format!("INSERT INTO {} (", block.table);
        query.reserve(256);
        let mut iter = block.column_iter();
        if let Some(col) = iter.next() {
            add_col!(query, col.name);

            for col in iter {
                query.push_str(",");
                add_col!(query, col.name);
            }
        }
        query.push_str(") VALUES");
        Query { query, id: None }
    }
}

impl From<&str> for Query {
    fn from(sql: &str) -> Self {
        let id = Uuid::new_v4();
        Query {
            id: Some(id),
            query: sql.into(),
        }
    }
}

impl From<String> for Query {
    fn from(sql: String) -> Self {
        Query::from(sql.as_str())
    }
}

impl ServerWriter for Query {
    fn write(&self, cx: &ServerInfo, writer: &mut dyn Write) -> Result<()> {
        if cx.revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
            // Query kind
            1u8.encode(writer)?;
            // Initial user
            ().encode(writer)?;
            // Query id
            //BaseFix follow to the official client
            ().encode(writer)?;
            // Network address
            "[::ffff:127.0.0.1]:0".encode(writer)?;
            // Network iface
            1u8.encode(writer)?;
            // OS user name
            ().encode(writer)?;
            // Hostname
            crate::HOSTNAME.encode(writer)?;
            // Client name and version
            crate::CLIENT_NAME.encode(writer)?;
            crate::CLICK_HOUSE_DBMSVERSION_MAJOR.encode(writer)?;
            crate::CLICK_HOUSE_DBMSVERSION_MINOR.encode(writer)?;
            crate::CLICK_HOUSE_REVISION.encode(writer)?;

            if cx.revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
                // Quota key
                ().encode(writer)?;
            }

            if cx.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
                // Client version patch
                0u8.encode(writer)?;
            };
        };

        // "max_block_size".encode(writer)?;
        // (crate::MAX_BLOCK_SIZE as u64).encode(writer)?;
        // let ro = cx.readonly;
        // if ro != 0 {
        //     "readonly".encode(writer)?;
        //     ro.encode(writer)?
        // }
        // Empty string end up settings block
        ().encode(writer)?;

        // Stage:Complete
        STATE_COMPLETE.encode(writer)?;
        let compression: u8 = if cx.compression.is_none() { 0 } else { 1 };

        // Compression (disable=0, enable=1)
        compression.encode(writer)?;

        // Query string
        self.query.encode(writer)?;
        writer.flush()
    }
}
