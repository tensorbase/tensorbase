use crate::types::BaseServerConn;
use bytes::BytesMut;
use meta::types::Id;

use super::blocks::Block;

pub const LZ4_COMPRESSION_METHOD: u8 = 0x82;

// === request ===
#[derive(Debug)]
#[repr(u8)]
pub enum ClientCodes {
    Hello = 0,
    // Name, version, revision, default database.
    Query = 1,
    // Query ID, settings for a single query, information, to what stage to execute the request, whether to use compression, query text (without data for INSERT).
    Data = 2,
    // Data block with or without compression.
    Cancel = 3,
    // Cancel the execution of the request.
    Ping = 4, // Check if the connection to the server is alive.
}

impl Default for ClientCodes {
    fn default() -> Self {
        ClientCodes::Hello
    }
}

#[derive(Debug)]
#[repr(u8)]
pub enum Interface {
    TCP = 1,
    HTTP = 2,
}

#[allow(dead_code)]
#[derive(Debug)]
#[repr(u8)]
pub enum HTTPMethod {
    UNKNOWN = 0,
    GET = 1,
    POST = 2,
}

#[derive(Debug)]
#[repr(u8)]
pub enum QueryKind {
    NoQuery = 0, // Uninitialized object.
    InitialQuery = 1,
    SecondaryQuery = 2, // Query that was initiated by another query for distributed or ON CLUSTER query execution.
}

// #[derive(Debug)]
// #[repr(u8)]
// pub(crate) enum ConnKind {
//     Default = 0,
//     InsertFormatInline = 1,
//     InsertFormatCSV = 2,
// }

// impl Default for ConnKind {
//     fn default() -> Self {
//         ConnKind::Default
//     }
// }

//FIXME remove unsafe with match pat
impl From<u8> for Interface {
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

//FIXME remove unsafe with match pat
impl From<u8> for HTTPMethod {
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

//FIXME remove unsafe with match pat
impl From<u8> for QueryKind {
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

//FIXME remove unsafe with match pat
impl From<u8> for ClientCodes {
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

// struct Hello {
//     client_code: u64,
//     client_name: String,
//     client_ver_maj: u64,
//     client_ver_min: u64,
//     revision: u64,
//     default_database: String,
//     user: String,
//     password: String,
// }

// enum Response {
//     Hello(),
//     Data(),
//     Exception(),
//     Progress(),
//     Pong(),
//     EndOfStream(),
// }

// struct Exception {
//     int code = 0;
//     std::string name;
//     std::string display_text;
//     std::string stack_trace;
//     /// Pointer to nested exception.
//     std::unique_ptr<Exception> nested;
// }

// struct Profile {
//     uint64_t rows = 0;
//     uint64_t blocks = 0;
//     uint64_t bytes = 0;
//     uint64_t rows_before_limit = 0;
//     bool applied_limit = false;
//     bool calculated_rows_before_limit = false;
// }

// struct Progress {
//     uint64_t rows = 0;
//     uint64_t bytes = 0;
//     uint64_t total_rows = 0;
// }

// === response ===
#[derive(Debug)]
#[repr(u8)]
pub enum ServerCodes {
    Hello = 0,                // Name, version, revision.
    Data = 1,                 // Data block with or without compression.
    Exception = 2,            // An exception occurred while processing a request.
    Progress = 3,             // Progress of query execution: rows read, bytes read.
    Pong = 4,                 // Reply to Ping.
    EndOfStream = 5,          // All packets were sent.
    ProfileInfo = 6,          // A package with profiling information.
    Totals = 7,   // Data block with total values, with or without compression.
    Extremes = 8, // Data block with minimums and maximums, similarly.
    TablesStatusResponse = 9, // Response to the TableStatus request.
    Log = 10,     // System log of request execution.
    TableColumns = 11, // Columns' description for default values calculation
}

//FIXME remove unsafe with match pat
impl From<u8> for ServerCodes {
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

#[derive(Debug)]
pub struct ClientInfo {
    pub query_kind: QueryKind,
    pub initial_user: String,
    pub initial_query_id: String,
    pub initial_address_string: String,
    pub interface: Interface,
    pub os_user: String,
    pub client_hostname: String,
    pub client_name: String,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_tcp_protocol_version: u64,
    pub quota_key: String,
    pub client_version_patch: u64,
}

#[derive(Debug)]
pub enum StageKind {
    Default,
    DataEODP,
    DataEODPInsertQuery,
    DataPacket,
    DataBlk,
    // EoS
}

impl Default for StageKind {
    fn default() -> Self {
        StageKind::Default
    }
}

#[derive(Debug)]
pub struct ConnCtx {
    pub query_id: String,
    pub current_db: String,
    pub current_tab_ins: String,
    pub current_tid_ins: Id,
    pub stage: StageKind,
    pub is_compressed: bool,
    pub data_block_opt: Option<Box<Block>>,
    pub raw_blk_req: Option<BytesMut>,
    raw_blk_resp: Option<BytesMut>,
}

impl BaseServerConn for ConnCtx {
    fn get_query_id(&self) -> &str {
        &self.query_id
    }

    fn set_query_id(&mut self, query_id: String) {
        self.query_id = query_id;
    }

    fn get_db(&self) -> &str {
        &self.current_db
    }

    fn set_db(&mut self, db: String) {
        self.current_db = db;
    }

    fn set_compressed(&mut self, is_compressed: bool) {
        self.is_compressed = is_compressed;
    }

    fn is_compressed(&self) -> bool {
        self.is_compressed
    }
}

impl Default for ConnCtx {
    fn default() -> Self {
        ConnCtx {
            query_id: Default::default(),
            current_db: "default".to_string(),
            current_tab_ins: Default::default(),
            current_tid_ins: Default::default(),
            stage: Default::default(),
            is_compressed: true,
            data_block_opt: None,
            raw_blk_req: None,
            raw_blk_resp: None,
        }
    }
}

impl ConnCtx {
    // #[inline]
    // pub fn get_data_block_and_aux_buf(&mut self) -> (&mut Block, &mut BytesMut) {
    //     if let Some(ref mut bb) = self.data_block_opt {
    //         (bb.as_mut(), self.get_raw_blk_req())
    //     } else {
    //         let bb = Box::new(Block::default());
    //         self.data_block_opt = Some(bb);
    //         (self.data_block_opt.as_mut().unwrap(), self.get_raw_blk_req()) //PANIC can not be panic...
    //     }
    // }

    #[inline]
    pub fn get_data_block(&mut self) -> &mut Block {
        if let Some(ref mut bb) = self.data_block_opt {
            bb.as_mut()
        } else {
            let bb = Box::new(Block::default());
            self.data_block_opt = Some(bb);
            self.data_block_opt.as_mut().unwrap() //PANIC can not be panic...
        }
    }

    #[inline]
    pub fn get_raw_blk_req(&mut self) -> &mut BytesMut {
        self.raw_blk_req
            .get_or_insert_with(|| BytesMut::with_capacity(4 * 1024))
        // if let Some(ref mut bs) = self.raw_blk_req {
        //     bs
        // } else {
        //     let bs = BytesMut::with_capacity(4 * 1024);//FIXME how about 1kb?
        //     self.raw_blk_req = Some(bs);
        //     self.raw_blk_req.as_mut().unwrap() //PANIC can not be panic...
        // }
    }

    #[inline]
    pub fn get_raw_blk_resp(&mut self) -> &mut BytesMut {
        if let Some(ref mut bs) = self.raw_blk_resp {
            bs
        } else {
            let bs = BytesMut::with_capacity(4 * 1024);
            self.raw_blk_resp = Some(bs);
            self.raw_blk_resp.as_mut().unwrap() //PANIC can not be panic...
        }
    }
}
