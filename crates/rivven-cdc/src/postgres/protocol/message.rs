//! PostgreSQL pgoutput protocol messages
//!
//! Defines the message types for pgoutput logical replication.

use bytes::Bytes;

/// pgoutput replication message
#[derive(Debug, Clone)]
pub enum ReplicationMessage {
    /// Transaction begin
    Begin(BeginBody),
    /// Transaction commit
    Commit(CommitBody),
    /// Origin information
    Origin(OriginBody),
    /// Relation (table) definition
    Relation(RelationBody),
    /// Type definition
    Type(TypeBody),
    /// Row insert
    Insert(InsertBody),
    /// Row update
    Update(UpdateBody),
    /// Row delete
    Delete(DeleteBody),
    /// Table truncate
    Truncate(TruncateBody),
    // Stream Protocol V2
    StreamStart(StreamStartBody),
    StreamStop(StreamStopBody),
    StreamCommit(StreamCommitBody),
    StreamAbort(StreamAbortBody),
}

/// BEGIN message
#[derive(Debug, Clone)]
pub struct BeginBody {
    pub final_lsn: u64,
    pub timestamp: i64,
    pub xid: u32,
}

/// COMMIT message
#[derive(Debug, Clone)]
pub struct CommitBody {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub timestamp: i64,
}

/// ORIGIN message
#[derive(Debug, Clone)]
pub struct OriginBody {
    pub commit_lsn: u64,
    pub name: String,
}

/// RELATION message (table definition)
#[derive(Debug, Clone)]
pub struct RelationBody {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

/// Column definition within a relation
#[derive(Debug, Clone)]
pub struct Column {
    pub flags: u8,
    pub name: String,
    pub type_id: i32,
    pub type_mode: i32,
}

/// TYPE message
#[derive(Debug, Clone)]
pub struct TypeBody {
    pub id: u32,
    pub namespace: String,
    pub name: String,
}

/// INSERT message
#[derive(Debug, Clone)]
pub struct InsertBody {
    pub relation_id: u32,
    pub tuple: Tuple,
}

/// UPDATE message
#[derive(Debug, Clone)]
pub struct UpdateBody {
    pub relation_id: u32,
    pub key_tuple: Option<Tuple>,
    pub new_tuple: Tuple,
}

/// DELETE message
#[derive(Debug, Clone)]
pub struct DeleteBody {
    pub relation_id: u32,
    pub key_tuple: Option<Tuple>,
}

/// TRUNCATE message
#[derive(Debug, Clone)]
pub struct TruncateBody {
    pub relation_ids: Vec<u32>,
    pub options: u8,
}

/// Row tuple
#[derive(Debug, Clone)]
pub struct Tuple(pub Vec<TupleData>);

/// Column data within a tuple
#[derive(Debug, Clone)]
pub enum TupleData {
    /// NULL value
    Null,
    /// TOAST value (unchanged)
    Toast,
    /// Text representation
    Text(Bytes),
}

// Stream Protocol V2 messages

/// Stream start (V2)
#[derive(Debug, Clone)]
pub struct StreamStartBody {
    pub xid: u32,
    pub first_segment: u8,
}

/// Stream stop (V2)
#[derive(Debug, Clone)]
pub struct StreamStopBody;

/// Stream commit (V2)
#[derive(Debug, Clone)]
pub struct StreamCommitBody {
    pub xid: u32,
    pub flags: u8,
    pub commit_lsn: u64,
    pub transaction_end_lsn: u64,
    pub commit_time: i64,
}

/// Stream abort (V2)
#[derive(Debug, Clone)]
pub struct StreamAbortBody {
    pub xid: u32,
    pub sub_xid: u32,
}

/// XLog data types
#[derive(Debug, Clone)]
pub enum XLogData {
    /// Standby status update
    StandbyStatusUpdate {
        wal_write_position: u64,
        wal_flush_position: u64,
        wal_apply_position: u64,
        client_time: i64,
        reply_requested: bool,
    },
    /// Primary keepalive
    PrimaryKeepAlive {
        wal_end: u64,
        timestamp: i64,
        reply_requested: bool,
    },
    /// Actual WAL data
    XLogData {
        wal_start: u64,
        wal_end: u64,
        timestamp: i64,
        data: Bytes,
    },
}
