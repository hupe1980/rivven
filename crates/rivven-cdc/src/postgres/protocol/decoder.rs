//! pgoutput decoder
//!
//! Decodes binary pgoutput messages from PostgreSQL logical replication.

use super::message::*;
use bytes::{Buf, Bytes};
use thiserror::Error;

/// Decoder errors
#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("Not enough data")]
    NotEnoughData,
    #[error("Invalid message type: {0}")]
    InvalidType(u8),
    #[error("UTF8 Error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// pgoutput decoder
pub struct PgOutputDecoder;

impl PgOutputDecoder {
    /// Decode a pgoutput message
    pub fn decode(data: &mut Bytes) -> Result<ReplicationMessage, DecodeError> {
        if !data.has_remaining() {
            return Err(DecodeError::NotEnoughData);
        }

        let msg_type = data.get_u8();

        match msg_type {
            b'B' => Self::decode_begin(data).map(ReplicationMessage::Begin),
            b'C' => Self::decode_commit(data).map(ReplicationMessage::Commit),
            b'R' => Self::decode_relation(data).map(ReplicationMessage::Relation),
            b'I' => Self::decode_insert(data).map(ReplicationMessage::Insert),
            b'U' => Self::decode_update(data).map(ReplicationMessage::Update),
            b'D' => Self::decode_delete(data).map(ReplicationMessage::Delete),
            b'O' => Self::decode_origin(data).map(ReplicationMessage::Origin),
            b'Y' => Self::decode_type(data).map(ReplicationMessage::Type),
            b'T' => Self::decode_truncate(data).map(ReplicationMessage::Truncate),
            // Streaming protocol V2
            b'S' => Self::decode_stream_start(data).map(ReplicationMessage::StreamStart),
            b'E' => Self::decode_stream_stop(data).map(ReplicationMessage::StreamStop),
            b'c' => Self::decode_stream_commit(data).map(ReplicationMessage::StreamCommit),
            b'A' => Self::decode_stream_abort(data).map(ReplicationMessage::StreamAbort),
            t => Err(DecodeError::InvalidType(t)),
        }
    }

    fn decode_begin(buf: &mut Bytes) -> Result<BeginBody, DecodeError> {
        let final_lsn = buf.get_u64();
        let timestamp = buf.get_i64();
        let xid = buf.get_u32();
        Ok(BeginBody {
            final_lsn,
            timestamp,
            xid,
        })
    }

    fn decode_commit(buf: &mut Bytes) -> Result<CommitBody, DecodeError> {
        let flags = buf.get_u8();
        let commit_lsn = buf.get_u64();
        let end_lsn = buf.get_u64();
        let timestamp = buf.get_i64();
        Ok(CommitBody {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    fn decode_origin(buf: &mut Bytes) -> Result<OriginBody, DecodeError> {
        let commit_lsn = buf.get_u64();
        let name = read_string(buf)?;
        Ok(OriginBody { commit_lsn, name })
    }

    fn decode_relation(buf: &mut Bytes) -> Result<RelationBody, DecodeError> {
        let id = buf.get_u32();
        let namespace = read_string(buf)?;
        let name = read_string(buf)?;
        let replica_identity = buf.get_u8();
        let num_columns = buf.get_u16();

        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let flags = buf.get_u8();
            let col_name = read_string(buf)?;
            let type_id = buf.get_i32();
            let type_mode = buf.get_i32();
            columns.push(Column {
                flags,
                name: col_name,
                type_id,
                type_mode,
            });
        }

        Ok(RelationBody {
            id,
            namespace,
            name,
            replica_identity,
            columns,
        })
    }

    fn decode_type(buf: &mut Bytes) -> Result<TypeBody, DecodeError> {
        let id = buf.get_u32();
        let namespace = read_string(buf)?;
        let name = read_string(buf)?;
        Ok(TypeBody {
            id,
            namespace,
            name,
        })
    }

    fn decode_insert(buf: &mut Bytes) -> Result<InsertBody, DecodeError> {
        let relation_id = buf.get_u32();
        let char_n = buf.get_u8();
        if char_n != b'N' {
            return Err(DecodeError::Protocol("Expected N for new tuple".into()));
        }
        let tuple = decode_tuple(buf)?;
        Ok(InsertBody { relation_id, tuple })
    }

    fn decode_update(buf: &mut Bytes) -> Result<UpdateBody, DecodeError> {
        let relation_id = buf.get_u32();
        let msg_type = buf.get_u8();

        let (key_tuple, new_tuple) = match msg_type {
            b'K' => {
                let key = Some(decode_tuple(buf)?);
                let char_n = buf.get_u8();
                if char_n != b'N' {
                    return Err(DecodeError::Protocol("Expected N after K".into()));
                }
                let new = decode_tuple(buf)?;
                (key, new)
            }
            b'O' => {
                let old = Some(decode_tuple(buf)?);
                let char_n = buf.get_u8();
                if char_n != b'N' {
                    return Err(DecodeError::Protocol("Expected N after O".into()));
                }
                let new = decode_tuple(buf)?;
                (old, new)
            }
            b'N' => {
                let new = decode_tuple(buf)?;
                (None, new)
            }
            t => return Err(DecodeError::InvalidType(t)),
        };

        Ok(UpdateBody {
            relation_id,
            key_tuple,
            new_tuple,
        })
    }

    fn decode_delete(buf: &mut Bytes) -> Result<DeleteBody, DecodeError> {
        let relation_id = buf.get_u32();
        let msg_type = buf.get_u8();

        let key_tuple = match msg_type {
            b'K' | b'O' => Some(decode_tuple(buf)?),
            t => return Err(DecodeError::InvalidType(t)),
        };

        Ok(DeleteBody {
            relation_id,
            key_tuple,
        })
    }

    fn decode_truncate(buf: &mut Bytes) -> Result<TruncateBody, DecodeError> {
        let num_rels = buf.get_u32();
        let options = buf.get_u8();
        let mut relation_ids = Vec::with_capacity(num_rels as usize);
        for _ in 0..num_rels {
            relation_ids.push(buf.get_u32());
        }
        Ok(TruncateBody {
            options,
            relation_ids,
        })
    }

    fn decode_stream_start(buf: &mut Bytes) -> Result<StreamStartBody, DecodeError> {
        let xid = buf.get_u32();
        let first_segment = buf.get_u8();
        Ok(StreamStartBody { xid, first_segment })
    }

    fn decode_stream_stop(_buf: &mut Bytes) -> Result<StreamStopBody, DecodeError> {
        Ok(StreamStopBody)
    }

    fn decode_stream_commit(buf: &mut Bytes) -> Result<StreamCommitBody, DecodeError> {
        let xid = buf.get_u32();
        let flags = buf.get_u8();
        let commit_lsn = buf.get_u64();
        let transaction_end_lsn = buf.get_u64();
        let commit_time = buf.get_i64();
        Ok(StreamCommitBody {
            xid,
            flags,
            commit_lsn,
            transaction_end_lsn,
            commit_time,
        })
    }

    fn decode_stream_abort(buf: &mut Bytes) -> Result<StreamAbortBody, DecodeError> {
        let xid = buf.get_u32();
        let sub_xid = buf.get_u32();
        Ok(StreamAbortBody { xid, sub_xid })
    }
}

fn read_string(buf: &mut Bytes) -> Result<String, DecodeError> {
    if !buf.has_remaining() {
        return Err(DecodeError::NotEnoughData);
    }
    let n = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or(DecodeError::NotEnoughData)?;
    let s_bytes = buf.copy_to_bytes(n);
    buf.advance(1); // skip null
    Ok(std::str::from_utf8(&s_bytes)?.to_string())
}

fn decode_tuple(buf: &mut Bytes) -> Result<Tuple, DecodeError> {
    let num_cols = buf.get_u16();
    let mut columns = Vec::with_capacity(num_cols as usize);

    for _ in 0..num_cols {
        let type_code = buf.get_u8();
        let data = match type_code {
            b'n' => TupleData::Null,
            b'u' => TupleData::Toast,
            b't' => {
                let len = buf.get_u32() as usize;
                let data = buf.copy_to_bytes(len);
                TupleData::Text(data)
            }
            t => return Err(DecodeError::InvalidType(t)),
        };
        columns.push(data);
    }

    Ok(Tuple(columns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_begin() {
        // Simulated BEGIN message
        let mut data = bytes::BytesMut::new();
        data.extend_from_slice(&[b'B']); // message type
        data.extend_from_slice(&0x0000000100000000u64.to_be_bytes()); // final_lsn
        data.extend_from_slice(&1705000000000000i64.to_be_bytes()); // timestamp
        data.extend_from_slice(&1u32.to_be_bytes()); // xid

        let mut bytes = data.freeze();
        let msg = PgOutputDecoder::decode(&mut bytes).unwrap();

        match msg {
            ReplicationMessage::Begin(body) => {
                assert_eq!(body.xid, 1);
            }
            _ => panic!("Expected Begin message"),
        }
    }
}
