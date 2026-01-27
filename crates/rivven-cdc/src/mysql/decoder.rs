//! MySQL binlog event decoder
//!
//! Decodes MySQL binary log events:
//! - FORMAT_DESCRIPTION_EVENT
//! - TABLE_MAP_EVENT  
//! - WRITE_ROWS_EVENT (v1 and v2)
//! - UPDATE_ROWS_EVENT (v1 and v2)
//! - DELETE_ROWS_EVENT (v1 and v2)
//! - ROTATE_EVENT
//! - GTID_LOG_EVENT
//! - XID_EVENT (transaction commit)
//! - QUERY_EVENT

use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use tracing::{debug, trace};

/// Binlog event types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventType {
    Unknown = 0,
    StartEventV3 = 1,
    QueryEvent = 2,
    StopEvent = 3,
    RotateEvent = 4,
    IntvarEvent = 5,
    LoadEvent = 6,
    SlaveEvent = 7,
    CreateFileEvent = 8,
    AppendBlockEvent = 9,
    ExecLoadEvent = 10,
    DeleteFileEvent = 11,
    NewLoadEvent = 12,
    RandEvent = 13,
    UserVarEvent = 14,
    FormatDescriptionEvent = 15,
    XidEvent = 16,
    BeginLoadQueryEvent = 17,
    ExecuteLoadQueryEvent = 18,
    TableMapEvent = 19,
    PreGaWriteRowsEvent = 20,
    PreGaUpdateRowsEvent = 21,
    PreGaDeleteRowsEvent = 22,
    WriteRowsEventV1 = 23,
    UpdateRowsEventV1 = 24,
    DeleteRowsEventV1 = 25,
    IncidentEvent = 26,
    HeartbeatLogEvent = 27,
    IgnorableLogEvent = 28,
    RowsQueryLogEvent = 29,
    WriteRowsEventV2 = 30,
    UpdateRowsEventV2 = 31,
    DeleteRowsEventV2 = 32,
    GtidLogEvent = 33,
    AnonymousGtidLogEvent = 34,
    PreviousGtidsLogEvent = 35,
    TransactionContextEvent = 36,
    ViewChangeEvent = 37,
    XaPrepareLogEvent = 38,
    PartialUpdateRowsEvent = 39,
    TransactionPayloadEvent = 40,
}

impl EventType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => EventType::StartEventV3,
            2 => EventType::QueryEvent,
            3 => EventType::StopEvent,
            4 => EventType::RotateEvent,
            5 => EventType::IntvarEvent,
            15 => EventType::FormatDescriptionEvent,
            16 => EventType::XidEvent,
            19 => EventType::TableMapEvent,
            23 => EventType::WriteRowsEventV1,
            24 => EventType::UpdateRowsEventV1,
            25 => EventType::DeleteRowsEventV1,
            27 => EventType::HeartbeatLogEvent,
            30 => EventType::WriteRowsEventV2,
            31 => EventType::UpdateRowsEventV2,
            32 => EventType::DeleteRowsEventV2,
            33 => EventType::GtidLogEvent,
            34 => EventType::AnonymousGtidLogEvent,
            35 => EventType::PreviousGtidsLogEvent,
            _ => EventType::Unknown,
        }
    }

    pub fn is_row_event(&self) -> bool {
        matches!(
            self,
            EventType::WriteRowsEventV1
                | EventType::WriteRowsEventV2
                | EventType::UpdateRowsEventV1
                | EventType::UpdateRowsEventV2
                | EventType::DeleteRowsEventV1
                | EventType::DeleteRowsEventV2
        )
    }
}

/// MySQL column types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnType {
    Decimal = 0,
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    Timestamp = 7,
    LongLong = 8,
    Int24 = 9,
    Date = 10,
    Time = 11,
    DateTime = 12,
    Year = 13,
    NewDate = 14,
    Varchar = 15,
    Bit = 16,
    Timestamp2 = 17,
    DateTime2 = 18,
    Time2 = 19,
    TypedArray = 20,
    Json = 245,
    NewDecimal = 246,
    Enum = 247,
    Set = 248,
    TinyBlob = 249,
    MediumBlob = 250,
    LongBlob = 251,
    Blob = 252,
    VarString = 253,
    String = 254,
    Geometry = 255,
}

impl ColumnType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => ColumnType::Decimal,
            1 => ColumnType::Tiny,
            2 => ColumnType::Short,
            3 => ColumnType::Long,
            4 => ColumnType::Float,
            5 => ColumnType::Double,
            6 => ColumnType::Null,
            7 => ColumnType::Timestamp,
            8 => ColumnType::LongLong,
            9 => ColumnType::Int24,
            10 => ColumnType::Date,
            11 => ColumnType::Time,
            12 => ColumnType::DateTime,
            13 => ColumnType::Year,
            14 => ColumnType::NewDate,
            15 => ColumnType::Varchar,
            16 => ColumnType::Bit,
            17 => ColumnType::Timestamp2,
            18 => ColumnType::DateTime2,
            19 => ColumnType::Time2,
            245 => ColumnType::Json,
            246 => ColumnType::NewDecimal,
            247 => ColumnType::Enum,
            248 => ColumnType::Set,
            249 => ColumnType::TinyBlob,
            250 => ColumnType::MediumBlob,
            251 => ColumnType::LongBlob,
            252 => ColumnType::Blob,
            253 => ColumnType::VarString,
            254 => ColumnType::String,
            255 => ColumnType::Geometry,
            _ => ColumnType::VarString, // Default to varchar for unknown
        }
    }
}

/// Binlog event header
#[derive(Debug, Clone)]
pub struct EventHeader {
    pub timestamp: u32,
    pub event_type: EventType,
    pub server_id: u32,
    pub event_length: u32,
    pub next_position: u32,
    pub flags: u16,
}

impl EventHeader {
    pub const SIZE: usize = 19;

    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            bail!("Event header too short: {} bytes", data.len());
        }

        let mut cursor = Cursor::new(data);

        let timestamp = cursor.get_u32_le();
        let event_type = EventType::from_u8(cursor.get_u8());
        let server_id = cursor.get_u32_le();
        let event_length = cursor.get_u32_le();
        let next_position = cursor.get_u32_le();
        let flags = cursor.get_u16_le();

        Ok(Self {
            timestamp,
            event_type,
            server_id,
            event_length,
            next_position,
            flags,
        })
    }
}

/// Decoded binlog event
#[derive(Debug, Clone)]
pub enum BinlogEvent {
    /// Format description - contains binlog format info
    FormatDescription(FormatDescriptionEvent),

    /// Table map - maps table ID to schema
    TableMap(TableMapEvent),

    /// Row insert
    WriteRows(RowsEvent),

    /// Row update  
    UpdateRows(RowsEvent),

    /// Row delete
    DeleteRows(RowsEvent),

    /// Transaction commit
    Xid(XidEvent),

    /// Query (DDL/DML statements)
    Query(QueryEvent),

    /// Rotate to new binlog file
    Rotate(RotateEvent),

    /// GTID for transaction
    Gtid(GtidEvent),

    /// Heartbeat
    Heartbeat,

    /// Unknown or unhandled event
    Unknown(EventType),
}

/// Format description event
#[derive(Debug, Clone)]
pub struct FormatDescriptionEvent {
    pub binlog_version: u16,
    pub server_version: String,
    pub create_timestamp: u32,
    pub header_length: u8,
    pub checksum_type: u8,
}

/// Table map event - describes table structure
#[derive(Debug, Clone)]
pub struct TableMapEvent {
    pub table_id: u64,
    pub flags: u16,
    pub schema_name: String,
    pub table_name: String,
    pub column_count: usize,
    pub column_types: Vec<ColumnType>,
    pub column_metadata: Vec<u16>,
    pub null_bitmap: Vec<u8>,
}

/// Rows event (INSERT/UPDATE/DELETE)
#[derive(Debug, Clone)]
pub struct RowsEvent {
    pub table_id: u64,
    pub flags: u16,
    pub column_count: usize,
    pub columns_before_image: Vec<u8>,        // bitmap
    pub columns_after_image: Option<Vec<u8>>, // bitmap (for UPDATE)
    pub rows: Vec<RowData>,
}

/// Row data
#[derive(Debug, Clone)]
pub struct RowData {
    pub before: Option<Vec<ColumnValue>>, // For UPDATE/DELETE
    pub after: Option<Vec<ColumnValue>>,  // For INSERT/UPDATE
}

/// Column value
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    SignedInt(i64),
    UnsignedInt(u64),
    Float(f32),
    Double(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    Time {
        hours: u8,
        minutes: u8,
        seconds: u8,
        microseconds: u32,
        negative: bool,
    },
    DateTime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    },
    Timestamp(u32),
    Year(u16),
    Json(serde_json::Value),
    Enum(u16),
    Set(u64),
    Bit(Vec<u8>),
}

/// XID event (transaction commit)
#[derive(Debug, Clone)]
pub struct XidEvent {
    pub xid: u64,
}

/// Query event
#[derive(Debug, Clone)]
pub struct QueryEvent {
    pub thread_id: u32,
    pub exec_time: u32,
    pub error_code: u16,
    pub schema: String,
    pub query: String,
}

/// Rotate event
#[derive(Debug, Clone)]
pub struct RotateEvent {
    pub position: u64,
    pub next_binlog: String,
}

/// GTID event
#[derive(Debug, Clone)]
pub struct GtidEvent {
    pub flags: u8,
    pub uuid: [u8; 16],
    pub gno: u64,
    pub logical_clock_ts_type: u8,
}

impl GtidEvent {
    pub fn uuid_string(&self) -> String {
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self.uuid[0], self.uuid[1], self.uuid[2], self.uuid[3],
            self.uuid[4], self.uuid[5],
            self.uuid[6], self.uuid[7],
            self.uuid[8], self.uuid[9],
            self.uuid[10], self.uuid[11], self.uuid[12], self.uuid[13], self.uuid[14], self.uuid[15]
        )
    }

    pub fn gtid_string(&self) -> String {
        format!("{}:{}", self.uuid_string(), self.gno)
    }
}

/// Binlog decoder with table cache
pub struct BinlogDecoder {
    /// Table map cache (table_id -> TableMapEvent)
    table_cache: HashMap<u64, TableMapEvent>,
    /// Format description for current binlog
    format: Option<FormatDescriptionEvent>,
}

impl Default for BinlogDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BinlogDecoder {
    pub fn new() -> Self {
        Self {
            table_cache: HashMap::new(),
            format: None,
        }
    }

    /// Decode a binlog event
    pub fn decode(&mut self, data: &Bytes) -> Result<BinlogEvent> {
        if data.len() < EventHeader::SIZE {
            bail!("Event data too short: {} bytes", data.len());
        }

        let header = EventHeader::parse(data)?;
        let payload = &data[EventHeader::SIZE..];

        trace!(
            "Decoding {:?} event, {} bytes payload",
            header.event_type,
            payload.len()
        );

        match header.event_type {
            EventType::FormatDescriptionEvent => {
                let event = self.decode_format_description(payload)?;
                self.format = Some(event.clone());
                Ok(BinlogEvent::FormatDescription(event))
            }
            EventType::TableMapEvent => {
                let event = self.decode_table_map(payload)?;
                self.table_cache.insert(event.table_id, event.clone());
                Ok(BinlogEvent::TableMap(event))
            }
            EventType::WriteRowsEventV1 | EventType::WriteRowsEventV2 => {
                let event = self.decode_rows_event(payload, false, header.event_type)?;
                Ok(BinlogEvent::WriteRows(event))
            }
            EventType::UpdateRowsEventV1 | EventType::UpdateRowsEventV2 => {
                let event = self.decode_rows_event(payload, true, header.event_type)?;
                Ok(BinlogEvent::UpdateRows(event))
            }
            EventType::DeleteRowsEventV1 | EventType::DeleteRowsEventV2 => {
                let event = self.decode_rows_event(payload, false, header.event_type)?;
                Ok(BinlogEvent::DeleteRows(event))
            }
            EventType::XidEvent => {
                let event = self.decode_xid(payload)?;
                Ok(BinlogEvent::Xid(event))
            }
            EventType::QueryEvent => {
                let event = self.decode_query(payload)?;
                Ok(BinlogEvent::Query(event))
            }
            EventType::RotateEvent => {
                let event = self.decode_rotate(payload)?;
                Ok(BinlogEvent::Rotate(event))
            }
            EventType::GtidLogEvent | EventType::AnonymousGtidLogEvent => {
                let event = self.decode_gtid(payload)?;
                Ok(BinlogEvent::Gtid(event))
            }
            EventType::HeartbeatLogEvent => Ok(BinlogEvent::Heartbeat),
            other => {
                debug!("Unhandled event type: {:?}", other);
                Ok(BinlogEvent::Unknown(other))
            }
        }
    }

    /// Get table info from cache
    pub fn get_table(&self, table_id: u64) -> Option<&TableMapEvent> {
        self.table_cache.get(&table_id)
    }

    fn decode_format_description(&self, data: &[u8]) -> Result<FormatDescriptionEvent> {
        let mut cursor = Cursor::new(data);

        let binlog_version = cursor.get_u16_le();

        let mut server_version_bytes = [0u8; 50];
        cursor.read_exact(&mut server_version_bytes)?;
        let server_version = String::from_utf8_lossy(&server_version_bytes)
            .trim_end_matches('\0')
            .to_string();

        let create_timestamp = cursor.get_u32_le();
        let header_length = cursor.get_u8();

        // Skip event type header lengths
        let remaining = data.len() - cursor.position() as usize;
        if remaining > 0 {
            cursor.advance(remaining - 1);
        }

        let checksum_type = if remaining > 0 { cursor.get_u8() } else { 0 };

        Ok(FormatDescriptionEvent {
            binlog_version,
            server_version,
            create_timestamp,
            header_length,
            checksum_type,
        })
    }

    fn decode_table_map(&self, data: &[u8]) -> Result<TableMapEvent> {
        let mut cursor = Cursor::new(data);

        // Table ID (6 bytes)
        let table_id = read_table_id(&mut cursor)?;

        // Flags
        let flags = cursor.get_u16_le();

        // Schema name (length-prefixed)
        let schema_len = cursor.get_u8() as usize;
        let mut schema_bytes = vec![0u8; schema_len];
        cursor.read_exact(&mut schema_bytes)?;
        let schema_name = String::from_utf8_lossy(&schema_bytes).to_string();
        cursor.get_u8(); // null terminator

        // Table name (length-prefixed)
        let table_len = cursor.get_u8() as usize;
        let mut table_bytes = vec![0u8; table_len];
        cursor.read_exact(&mut table_bytes)?;
        let table_name = String::from_utf8_lossy(&table_bytes).to_string();
        cursor.get_u8(); // null terminator

        // Column count (packed integer)
        let column_count = read_packed_int(&mut cursor)? as usize;

        // Column types
        let mut column_types = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            column_types.push(ColumnType::from_u8(cursor.get_u8()));
        }

        // Metadata length (packed integer)
        let metadata_len = read_packed_int(&mut cursor)? as usize;

        // Column metadata
        let column_metadata =
            self.decode_column_metadata(&column_types, &mut cursor, metadata_len)?;

        // Null bitmap
        let null_bitmap_len = column_count.div_ceil(8);
        let mut null_bitmap = vec![0u8; null_bitmap_len];
        cursor.read_exact(&mut null_bitmap)?;

        Ok(TableMapEvent {
            table_id,
            flags,
            schema_name,
            table_name,
            column_count,
            column_types,
            column_metadata,
            null_bitmap,
        })
    }

    fn decode_column_metadata(
        &self,
        column_types: &[ColumnType],
        cursor: &mut Cursor<&[u8]>,
        _metadata_len: usize,
    ) -> Result<Vec<u16>> {
        let mut metadata = Vec::with_capacity(column_types.len());

        for col_type in column_types {
            let meta = match col_type {
                ColumnType::Float
                | ColumnType::Double
                | ColumnType::Blob
                | ColumnType::TinyBlob
                | ColumnType::MediumBlob
                | ColumnType::LongBlob
                | ColumnType::Json
                | ColumnType::Geometry => cursor.get_u8() as u16,
                ColumnType::Bit | ColumnType::Varchar | ColumnType::VarString => {
                    cursor.get_u16_le()
                }
                ColumnType::NewDecimal => {
                    let precision = cursor.get_u8();
                    let scale = cursor.get_u8();
                    ((precision as u16) << 8) | (scale as u16)
                }
                ColumnType::String | ColumnType::Enum | ColumnType::Set => cursor.get_u16_le(),
                ColumnType::Time2 | ColumnType::DateTime2 | ColumnType::Timestamp2 => {
                    cursor.get_u8() as u16
                }
                _ => 0,
            };
            metadata.push(meta);
        }

        Ok(metadata)
    }

    fn decode_rows_event(
        &self,
        data: &[u8],
        is_update: bool,
        event_type: EventType,
    ) -> Result<RowsEvent> {
        let mut cursor = Cursor::new(data);

        // Table ID (6 bytes)
        let table_id = read_table_id(&mut cursor)?;

        // Flags
        let flags = cursor.get_u16_le();

        // Extra data length for v2 events
        if matches!(
            event_type,
            EventType::WriteRowsEventV2
                | EventType::UpdateRowsEventV2
                | EventType::DeleteRowsEventV2
        ) {
            let extra_len = cursor.get_u16_le();
            if extra_len > 2 {
                cursor.advance((extra_len - 2) as usize);
            }
        }

        // Column count
        let column_count = read_packed_int(&mut cursor)? as usize;

        // Columns bitmap (present columns)
        let bitmap_len = column_count.div_ceil(8);
        let mut columns_before_image = vec![0u8; bitmap_len];
        cursor.read_exact(&mut columns_before_image)?;

        // For UPDATE events, second bitmap
        let columns_after_image = if is_update {
            let mut bitmap = vec![0u8; bitmap_len];
            cursor.read_exact(&mut bitmap)?;
            Some(bitmap)
        } else {
            None
        };

        // Decode rows
        let table_map = self.table_cache.get(&table_id);
        let rows = self.decode_row_data(
            &mut cursor,
            table_map,
            column_count,
            &columns_before_image,
            columns_after_image.as_deref(),
            is_update,
            event_type,
        )?;

        Ok(RowsEvent {
            table_id,
            flags,
            column_count,
            columns_before_image,
            columns_after_image,
            rows,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_row_data(
        &self,
        cursor: &mut Cursor<&[u8]>,
        table_map: Option<&TableMapEvent>,
        column_count: usize,
        columns_bitmap: &[u8],
        update_bitmap: Option<&[u8]>,
        is_update: bool,
        event_type: EventType,
    ) -> Result<Vec<RowData>> {
        let mut rows = Vec::new();

        while cursor.has_remaining() {
            // Check if we have enough data for at least a null bitmap
            let present_count = count_set_bits(columns_bitmap);
            let null_bitmap_len = present_count.div_ceil(8);

            if cursor.remaining() < null_bitmap_len {
                break;
            }

            let is_delete = matches!(
                event_type,
                EventType::DeleteRowsEventV1 | EventType::DeleteRowsEventV2
            );

            if is_update || is_delete {
                // Before image (for UPDATE/DELETE)
                let mut null_bitmap = vec![0u8; null_bitmap_len];
                cursor.read_exact(&mut null_bitmap)?;

                let values = self.decode_row_values(
                    cursor,
                    table_map,
                    column_count,
                    columns_bitmap,
                    &null_bitmap,
                )?;

                if is_update {
                    // After image
                    let update_present_count =
                        count_set_bits(update_bitmap.unwrap_or(columns_bitmap));
                    let update_null_bitmap_len = update_present_count.div_ceil(8);

                    if cursor.remaining() < update_null_bitmap_len {
                        rows.push(RowData {
                            before: Some(values),
                            after: None,
                        });
                        break;
                    }

                    let mut after_null_bitmap = vec![0u8; update_null_bitmap_len];
                    cursor.read_exact(&mut after_null_bitmap)?;

                    let after_values = self.decode_row_values(
                        cursor,
                        table_map,
                        column_count,
                        update_bitmap.unwrap_or(columns_bitmap),
                        &after_null_bitmap,
                    )?;

                    rows.push(RowData {
                        before: Some(values),
                        after: Some(after_values),
                    });
                } else {
                    rows.push(RowData {
                        before: Some(values),
                        after: None,
                    });
                }
            } else {
                // INSERT - only after image
                let mut null_bitmap = vec![0u8; null_bitmap_len];
                cursor.read_exact(&mut null_bitmap)?;

                let values = self.decode_row_values(
                    cursor,
                    table_map,
                    column_count,
                    columns_bitmap,
                    &null_bitmap,
                )?;
                rows.push(RowData {
                    before: None,
                    after: Some(values),
                });
            }
        }

        Ok(rows)
    }

    fn decode_row_values(
        &self,
        cursor: &mut Cursor<&[u8]>,
        table_map: Option<&TableMapEvent>,
        column_count: usize,
        columns_bitmap: &[u8],
        null_bitmap: &[u8],
    ) -> Result<Vec<ColumnValue>> {
        let mut values = Vec::with_capacity(column_count);
        let mut null_idx = 0;

        for col_idx in 0..column_count {
            // Check if column is present
            if !is_bit_set(columns_bitmap, col_idx) {
                continue;
            }

            // Check if value is null
            if is_bit_set(null_bitmap, null_idx) {
                values.push(ColumnValue::Null);
                null_idx += 1;
                continue;
            }

            let col_type = table_map
                .and_then(|tm| tm.column_types.get(col_idx))
                .copied()
                .unwrap_or(ColumnType::VarString);

            let metadata = table_map
                .and_then(|tm| tm.column_metadata.get(col_idx))
                .copied()
                .unwrap_or(0);

            let value = self.decode_column_value(cursor, col_type, metadata)?;
            values.push(value);
            null_idx += 1;
        }

        Ok(values)
    }

    fn decode_column_value(
        &self,
        cursor: &mut Cursor<&[u8]>,
        col_type: ColumnType,
        metadata: u16,
    ) -> Result<ColumnValue> {
        match col_type {
            ColumnType::Tiny => Ok(ColumnValue::SignedInt(cursor.get_i8() as i64)),
            ColumnType::Short => Ok(ColumnValue::SignedInt(cursor.get_i16_le() as i64)),
            ColumnType::Int24 => {
                let b1 = cursor.get_u8() as u32;
                let b2 = cursor.get_u8() as u32;
                let b3 = cursor.get_u8() as u32;
                let val = b1 | (b2 << 8) | (b3 << 16);
                // Sign extend
                let signed = if val & 0x800000 != 0 {
                    (val | 0xFF000000) as i32
                } else {
                    val as i32
                };
                Ok(ColumnValue::SignedInt(signed as i64))
            }
            ColumnType::Long => Ok(ColumnValue::SignedInt(cursor.get_i32_le() as i64)),
            ColumnType::LongLong => Ok(ColumnValue::SignedInt(cursor.get_i64_le())),
            ColumnType::Float => Ok(ColumnValue::Float(cursor.get_f32_le())),
            ColumnType::Double => Ok(ColumnValue::Double(cursor.get_f64_le())),
            ColumnType::Year => {
                let year = cursor.get_u8() as u16 + 1900;
                Ok(ColumnValue::Year(year))
            }
            ColumnType::Date => {
                let packed = cursor.get_u8() as u32
                    | ((cursor.get_u8() as u32) << 8)
                    | ((cursor.get_u8() as u32) << 16);
                let day = (packed & 0x1F) as u8;
                let month = ((packed >> 5) & 0x0F) as u8;
                let year = ((packed >> 9) & 0x7FFF) as u16;
                Ok(ColumnValue::Date { year, month, day })
            }
            ColumnType::Time => {
                let packed = cursor.get_u8() as u32
                    | ((cursor.get_u8() as u32) << 8)
                    | ((cursor.get_u8() as u32) << 16);
                let seconds = (packed % 100) as u8;
                let minutes = ((packed / 100) % 100) as u8;
                let hours = (packed / 10000) as u8;
                Ok(ColumnValue::Time {
                    hours,
                    minutes,
                    seconds,
                    microseconds: 0,
                    negative: false,
                })
            }
            ColumnType::DateTime => {
                let packed = cursor.get_u64_le();
                let second = (packed % 100) as u8;
                let minute = ((packed / 100) % 100) as u8;
                let hour = ((packed / 10000) % 100) as u8;
                let day = ((packed / 1000000) % 100) as u8;
                let month = ((packed / 100000000) % 100) as u8;
                let year = (packed / 10000000000) as u16;
                Ok(ColumnValue::DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    microsecond: 0,
                })
            }
            ColumnType::Timestamp => Ok(ColumnValue::Timestamp(cursor.get_u32_le())),
            ColumnType::Timestamp2 => {
                let _ts = cursor.get_u32();
                let frac = read_fractional_seconds(cursor, metadata as u8)?;
                Ok(ColumnValue::DateTime {
                    year: 0, // Would need to convert from unix timestamp
                    month: 0,
                    day: 0,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    microsecond: frac,
                })
            }
            ColumnType::DateTime2 => {
                // Packed datetime2
                let packed = read_datetime2_packed(cursor)?;
                let frac = read_fractional_seconds(cursor, metadata as u8)?;

                let year_month = (packed >> 22) & 0x1FFFF;
                let year = (year_month / 13) as u16;
                let month = (year_month % 13) as u8;
                let day = ((packed >> 17) & 0x1F) as u8;
                let hour = ((packed >> 12) & 0x1F) as u8;
                let minute = ((packed >> 6) & 0x3F) as u8;
                let second = (packed & 0x3F) as u8;

                Ok(ColumnValue::DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    microsecond: frac,
                })
            }
            ColumnType::Time2 => {
                let packed = read_time2_packed(cursor)?;
                let frac = read_fractional_seconds(cursor, metadata as u8)?;

                let negative = (packed & 0x800000) == 0;
                let value = if negative {
                    0x800000 - (packed & 0x7FFFFF)
                } else {
                    packed & 0x7FFFFF
                };

                let hours = ((value >> 12) & 0x3FF) as u8;
                let minutes = ((value >> 6) & 0x3F) as u8;
                let seconds = (value & 0x3F) as u8;

                Ok(ColumnValue::Time {
                    hours,
                    minutes,
                    seconds,
                    microseconds: frac,
                    negative,
                })
            }
            ColumnType::Varchar | ColumnType::VarString => {
                let len = if metadata < 256 {
                    cursor.get_u8() as usize
                } else {
                    cursor.get_u16_le() as usize
                };
                let mut bytes = vec![0u8; len];
                cursor.read_exact(&mut bytes)?;
                Ok(ColumnValue::String(
                    String::from_utf8_lossy(&bytes).to_string(),
                ))
            }
            ColumnType::String => {
                let real_type = (metadata >> 8) as u8;
                let max_len = metadata & 0xFF;

                if real_type == ColumnType::Enum as u8 {
                    let val = if max_len == 1 {
                        cursor.get_u8() as u16
                    } else {
                        cursor.get_u16_le()
                    };
                    Ok(ColumnValue::Enum(val))
                } else if real_type == ColumnType::Set as u8 {
                    let byte_count = max_len.div_ceil(8);
                    let mut val = 0u64;
                    for i in 0..byte_count {
                        val |= (cursor.get_u8() as u64) << (i * 8);
                    }
                    Ok(ColumnValue::Set(val))
                } else {
                    let len = if max_len < 256 {
                        cursor.get_u8() as usize
                    } else {
                        cursor.get_u16_le() as usize
                    };
                    let mut bytes = vec![0u8; len];
                    cursor.read_exact(&mut bytes)?;
                    Ok(ColumnValue::String(
                        String::from_utf8_lossy(&bytes).to_string(),
                    ))
                }
            }
            ColumnType::Blob
            | ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob => {
                let len_bytes = metadata as usize;
                let len = match len_bytes {
                    1 => cursor.get_u8() as usize,
                    2 => cursor.get_u16_le() as usize,
                    3 => {
                        let b1 = cursor.get_u8() as usize;
                        let b2 = cursor.get_u8() as usize;
                        let b3 = cursor.get_u8() as usize;
                        b1 | (b2 << 8) | (b3 << 16)
                    }
                    4 => cursor.get_u32_le() as usize,
                    _ => cursor.get_u8() as usize,
                };
                let mut bytes = vec![0u8; len];
                cursor.read_exact(&mut bytes)?;
                Ok(ColumnValue::Bytes(bytes))
            }
            ColumnType::Json => {
                let len = match metadata {
                    1 => cursor.get_u8() as usize,
                    2 => cursor.get_u16_le() as usize,
                    3 => {
                        let b1 = cursor.get_u8() as usize;
                        let b2 = cursor.get_u8() as usize;
                        let b3 = cursor.get_u8() as usize;
                        b1 | (b2 << 8) | (b3 << 16)
                    }
                    4 => cursor.get_u32_le() as usize,
                    _ => cursor.get_u8() as usize,
                };
                let mut bytes = vec![0u8; len];
                cursor.read_exact(&mut bytes)?;
                // MySQL stores JSON in binary format, but we'll just return as bytes for now
                // A full implementation would decode the MySQL JSON binary format
                Ok(ColumnValue::Bytes(bytes))
            }
            ColumnType::NewDecimal => {
                let precision = (metadata >> 8) as usize;
                let scale = (metadata & 0xFF) as usize;
                let decimal = decode_decimal(cursor, precision, scale)?;
                Ok(ColumnValue::Decimal(decimal))
            }
            ColumnType::Bit => {
                let nbits = ((metadata >> 8) * 8 + (metadata & 0xFF)) as usize;
                let len = nbits.div_ceil(8);
                let mut bytes = vec![0u8; len];
                cursor.read_exact(&mut bytes)?;
                Ok(ColumnValue::Bit(bytes))
            }
            ColumnType::Enum => {
                let val = if metadata == 1 {
                    cursor.get_u8() as u16
                } else {
                    cursor.get_u16_le()
                };
                Ok(ColumnValue::Enum(val))
            }
            ColumnType::Set => {
                let byte_count = metadata as usize;
                let mut val = 0u64;
                for i in 0..byte_count {
                    val |= (cursor.get_u8() as u64) << (i * 8);
                }
                Ok(ColumnValue::Set(val))
            }
            _ => {
                // Default: try to read as variable-length string
                let len = cursor.get_u8() as usize;
                let mut bytes = vec![0u8; len];
                cursor.read_exact(&mut bytes)?;
                Ok(ColumnValue::Bytes(bytes))
            }
        }
    }

    fn decode_xid(&self, data: &[u8]) -> Result<XidEvent> {
        let mut cursor = Cursor::new(data);
        let xid = cursor.get_u64_le();
        Ok(XidEvent { xid })
    }

    fn decode_query(&self, data: &[u8]) -> Result<QueryEvent> {
        let mut cursor = Cursor::new(data);

        let thread_id = cursor.get_u32_le();
        let exec_time = cursor.get_u32_le();
        let schema_len = cursor.get_u8() as usize;
        let error_code = cursor.get_u16_le();

        // Status vars length
        let status_vars_len = cursor.get_u16_le() as usize;
        cursor.advance(status_vars_len);

        // Schema
        let mut schema_bytes = vec![0u8; schema_len];
        cursor.read_exact(&mut schema_bytes)?;
        let schema = String::from_utf8_lossy(&schema_bytes).to_string();
        cursor.get_u8(); // null terminator

        // Query
        let remaining = data.len() - cursor.position() as usize;
        let query_len = if remaining > 4 {
            remaining - 4
        } else {
            remaining
        }; // Exclude checksum
        let mut query_bytes = vec![0u8; query_len];
        cursor.read_exact(&mut query_bytes)?;
        let query = String::from_utf8_lossy(&query_bytes).to_string();

        Ok(QueryEvent {
            thread_id,
            exec_time,
            error_code,
            schema,
            query,
        })
    }

    fn decode_rotate(&self, data: &[u8]) -> Result<RotateEvent> {
        let mut cursor = Cursor::new(data);

        let position = cursor.get_u64_le();

        let remaining = data.len() - cursor.position() as usize;
        let name_len = if remaining > 4 {
            remaining - 4
        } else {
            remaining
        };
        let mut name_bytes = vec![0u8; name_len];
        cursor.read_exact(&mut name_bytes)?;
        let next_binlog = String::from_utf8_lossy(&name_bytes)
            .trim_end_matches('\0')
            .to_string();

        Ok(RotateEvent {
            position,
            next_binlog,
        })
    }

    fn decode_gtid(&self, data: &[u8]) -> Result<GtidEvent> {
        let mut cursor = Cursor::new(data);

        let flags = cursor.get_u8();

        let mut uuid = [0u8; 16];
        cursor.read_exact(&mut uuid)?;

        let gno = cursor.get_u64_le();

        let logical_clock_ts_type = if cursor.has_remaining() {
            cursor.get_u8()
        } else {
            0
        };

        Ok(GtidEvent {
            flags,
            uuid,
            gno,
            logical_clock_ts_type,
        })
    }
}

// Helper functions

fn read_table_id(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let b1 = cursor.get_u8() as u64;
    let b2 = cursor.get_u8() as u64;
    let b3 = cursor.get_u8() as u64;
    let b4 = cursor.get_u8() as u64;
    let b5 = cursor.get_u8() as u64;
    let b6 = cursor.get_u8() as u64;
    Ok(b1 | (b2 << 8) | (b3 << 16) | (b4 << 24) | (b5 << 32) | (b6 << 40))
}

fn read_packed_int(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let first = cursor.get_u8();
    match first {
        0..=250 => Ok(first as u64),
        252 => Ok(cursor.get_u16_le() as u64),
        253 => {
            let b1 = cursor.get_u8() as u64;
            let b2 = cursor.get_u8() as u64;
            let b3 = cursor.get_u8() as u64;
            Ok(b1 | (b2 << 8) | (b3 << 16))
        }
        254 => Ok(cursor.get_u64_le()),
        _ => bail!("Invalid packed int: {}", first),
    }
}

fn read_fractional_seconds(cursor: &mut Cursor<&[u8]>, fsp: u8) -> Result<u32> {
    let bytes = (fsp as usize).div_ceil(2);
    let mut val = 0u32;
    for _i in 0..bytes {
        val = (val << 8) | (cursor.get_u8() as u32);
    }
    // Convert to microseconds
    Ok(val * (10u32.pow(6 - fsp as u32)))
}

fn read_datetime2_packed(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let b1 = cursor.get_u8() as u64;
    let b2 = cursor.get_u8() as u64;
    let b3 = cursor.get_u8() as u64;
    let b4 = cursor.get_u8() as u64;
    let b5 = cursor.get_u8() as u64;
    // Big-endian packed value
    Ok((b1 << 32) | (b2 << 24) | (b3 << 16) | (b4 << 8) | b5)
}

fn read_time2_packed(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let b1 = cursor.get_u8() as u32;
    let b2 = cursor.get_u8() as u32;
    let b3 = cursor.get_u8() as u32;
    // Big-endian packed value
    Ok((b1 << 16) | (b2 << 8) | b3)
}

fn count_set_bits(bitmap: &[u8]) -> usize {
    bitmap.iter().map(|b| b.count_ones() as usize).sum()
}

fn is_bit_set(bitmap: &[u8], idx: usize) -> bool {
    let byte_idx = idx / 8;
    let bit_idx = idx % 8;
    byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
}

fn decode_decimal(cursor: &mut Cursor<&[u8]>, precision: usize, scale: usize) -> Result<String> {
    // MySQL DECIMAL encoding
    let int_digits = precision - scale;
    let int_words = int_digits / 9;
    let int_leftover = int_digits % 9;
    let frac_words = scale / 9;
    let frac_leftover = scale % 9;

    let leftover_bytes = |digits: usize| -> usize {
        match digits {
            0 => 0,
            1..=2 => 1,
            3..=4 => 2,
            5..=6 => 3,
            7..=9 => 4,
            _ => 4,
        }
    };

    let int_leftover_bytes = leftover_bytes(int_leftover);
    let frac_leftover_bytes = leftover_bytes(frac_leftover);

    let total_bytes = int_leftover_bytes + int_words * 4 + frac_words * 4 + frac_leftover_bytes;

    let mut bytes = vec![0u8; total_bytes];
    cursor.read_exact(&mut bytes)?;

    // Flip sign bit (stored inverted for sorting)
    let negative = (bytes[0] & 0x80) == 0;
    bytes[0] ^= 0x80;

    // If negative, flip all bytes (stored as complement for sorting)
    if negative {
        for b in bytes.iter_mut() {
            *b = !*b;
        }
    }

    let mut result = String::new();
    if negative {
        result.push('-');
    }

    let mut cursor_bytes = Cursor::new(bytes.as_slice());
    let mut int_part = String::new();

    // Integer leftover
    if int_leftover_bytes > 0 {
        let val = read_be_int(&mut cursor_bytes, int_leftover_bytes)?;
        if val > 0 || int_words == 0 {
            int_part.push_str(&val.to_string());
        }
    }

    // Integer words
    for _ in 0..int_words {
        let val = cursor_bytes.get_u32();
        if int_part.is_empty() && val == 0 {
            continue;
        }
        if int_part.is_empty() {
            int_part.push_str(&val.to_string());
        } else {
            int_part.push_str(&format!("{:09}", val));
        }
    }

    if int_part.is_empty() {
        int_part.push('0');
    }

    result.push_str(&int_part);

    if scale > 0 {
        result.push('.');

        // Fractional words
        for _ in 0..frac_words {
            let val = cursor_bytes.get_u32();
            result.push_str(&format!("{:09}", val));
        }

        // Fractional leftover
        if frac_leftover_bytes > 0 {
            let val = read_be_int(&mut cursor_bytes, frac_leftover_bytes)?;
            result.push_str(&format!("{:0width$}", val, width = frac_leftover));
        }
    }

    Ok(result)
}

fn read_be_int(cursor: &mut Cursor<&[u8]>, bytes: usize) -> Result<u32> {
    let mut val = 0u32;
    for _ in 0..bytes {
        val = (val << 8) | (cursor.get_u8() as u32);
    }
    Ok(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_u8() {
        assert_eq!(EventType::from_u8(15), EventType::FormatDescriptionEvent);
        assert_eq!(EventType::from_u8(19), EventType::TableMapEvent);
        assert_eq!(EventType::from_u8(30), EventType::WriteRowsEventV2);
        assert_eq!(EventType::from_u8(31), EventType::UpdateRowsEventV2);
        assert_eq!(EventType::from_u8(32), EventType::DeleteRowsEventV2);
        assert_eq!(EventType::from_u8(16), EventType::XidEvent);
        assert_eq!(EventType::from_u8(4), EventType::RotateEvent);
        assert_eq!(EventType::from_u8(33), EventType::GtidLogEvent);
        assert_eq!(EventType::from_u8(255), EventType::Unknown);
    }

    #[test]
    fn test_is_row_event() {
        assert!(EventType::WriteRowsEventV2.is_row_event());
        assert!(EventType::UpdateRowsEventV2.is_row_event());
        assert!(EventType::DeleteRowsEventV2.is_row_event());
        assert!(EventType::WriteRowsEventV1.is_row_event());
        assert!(EventType::UpdateRowsEventV1.is_row_event());
        assert!(EventType::DeleteRowsEventV1.is_row_event());
        assert!(!EventType::QueryEvent.is_row_event());
        assert!(!EventType::FormatDescriptionEvent.is_row_event());
        assert!(!EventType::TableMapEvent.is_row_event());
    }

    #[test]
    fn test_count_set_bits() {
        assert_eq!(count_set_bits(&[0b11111111]), 8);
        assert_eq!(count_set_bits(&[0b10101010]), 4);
        assert_eq!(count_set_bits(&[0b00000000]), 0);
        assert_eq!(count_set_bits(&[0b11111111, 0b11111111]), 16);
        assert_eq!(count_set_bits(&[0b00000001]), 1);
        assert_eq!(count_set_bits(&[]), 0);
    }

    #[test]
    fn test_is_bit_set() {
        let bitmap = vec![0b00000101];
        assert!(is_bit_set(&bitmap, 0));
        assert!(!is_bit_set(&bitmap, 1));
        assert!(is_bit_set(&bitmap, 2));
        assert!(!is_bit_set(&bitmap, 3));

        // Multi-byte bitmap
        let bitmap2 = vec![0b10000000, 0b00000001];
        assert!(is_bit_set(&bitmap2, 7));
        assert!(is_bit_set(&bitmap2, 8));
        assert!(!is_bit_set(&bitmap2, 0));
    }

    #[test]
    fn test_gtid_event_uuid_string() {
        let event = GtidEvent {
            flags: 0,
            uuid: [
                0x3E, 0x11, 0xFA, 0x47, 0x71, 0xCA, 0x11, 0xE1, 0x9E, 0x33, 0xC8, 0x0A, 0xA9, 0x42,
                0x95, 0x62,
            ],
            gno: 1,
            logical_clock_ts_type: 0,
        };
        assert_eq!(event.uuid_string(), "3e11fa47-71ca-11e1-9e33-c80aa9429562");
        assert_eq!(
            event.gtid_string(),
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"
        );
    }

    #[test]
    fn test_gtid_event_high_gno() {
        let event = GtidEvent {
            flags: 1,
            uuid: [0x00; 16],
            gno: u64::MAX,
            logical_clock_ts_type: 2,
        };
        assert!(event.gtid_string().ends_with(&format!(":{}", u64::MAX)));
    }

    #[test]
    fn test_column_type_from_u8() {
        assert_eq!(ColumnType::from_u8(0), ColumnType::Decimal);
        assert_eq!(ColumnType::from_u8(1), ColumnType::Tiny);
        assert_eq!(ColumnType::from_u8(2), ColumnType::Short);
        assert_eq!(ColumnType::from_u8(3), ColumnType::Long);
        assert_eq!(ColumnType::from_u8(4), ColumnType::Float);
        assert_eq!(ColumnType::from_u8(5), ColumnType::Double);
        assert_eq!(ColumnType::from_u8(252), ColumnType::Blob);
        assert_eq!(ColumnType::from_u8(253), ColumnType::VarString);
        assert_eq!(ColumnType::from_u8(254), ColumnType::String);
        // Unknown types default to VarString
        assert_eq!(ColumnType::from_u8(200), ColumnType::VarString);
    }

    #[test]
    fn test_binlog_decoder_new() {
        let decoder = BinlogDecoder::new();
        assert!(decoder.format.is_none());
        assert!(decoder.table_cache.is_empty());
    }

    #[test]
    fn test_binlog_decoder_get_table_empty() {
        let decoder = BinlogDecoder::new();
        assert!(decoder.get_table(1).is_none());
        assert!(decoder.get_table(u64::MAX).is_none());
    }

    #[test]
    fn test_event_header_parse_too_short() {
        let data = Bytes::from(vec![0u8; 10]); // Less than EventHeader::SIZE (19)
        let result = EventHeader::parse(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_event_header_parse_minimal() {
        // Minimum valid header: 19 bytes
        let mut data = vec![0u8; 19];
        // timestamp: 4 bytes LE
        data[0..4].copy_from_slice(&100u32.to_le_bytes());
        // event_type: 1 byte (15 = FormatDescriptionEvent)
        data[4] = 15;
        // server_id: 4 bytes LE
        data[5..9].copy_from_slice(&1u32.to_le_bytes());
        // event_length: 4 bytes LE
        data[9..13].copy_from_slice(&50u32.to_le_bytes());
        // next_position: 4 bytes LE
        data[13..17].copy_from_slice(&69u32.to_le_bytes());
        // flags: 2 bytes LE
        data[17..19].copy_from_slice(&0u16.to_le_bytes());

        let header = EventHeader::parse(&Bytes::from(data)).unwrap();
        assert_eq!(header.timestamp, 100);
        assert_eq!(header.event_type, EventType::FormatDescriptionEvent);
        assert_eq!(header.server_id, 1);
        assert_eq!(header.event_length, 50);
        assert_eq!(header.next_position, 69);
        assert_eq!(header.flags, 0);
    }

    #[test]
    fn test_column_value_variants() {
        // Test that all variants can be created
        let _ = ColumnValue::Null;
        let _ = ColumnValue::SignedInt(-42);
        let _ = ColumnValue::UnsignedInt(42);
        let _ = ColumnValue::Float(1.23);
        let _ = ColumnValue::Double(4.56789);
        let _ = ColumnValue::Decimal("123.45".to_string());
        let _ = ColumnValue::String("hello".to_string());
        let _ = ColumnValue::Bytes(vec![1, 2, 3]);
        let _ = ColumnValue::Date {
            year: 2024,
            month: 1,
            day: 15,
        };
        let _ = ColumnValue::Time {
            hours: 12,
            minutes: 30,
            seconds: 45,
            microseconds: 0,
            negative: false,
        };
        let _ = ColumnValue::DateTime {
            year: 2024,
            month: 1,
            day: 15,
            hour: 12,
            minute: 30,
            second: 45,
            microsecond: 0,
        };
        let _ = ColumnValue::Timestamp(1705312245);
        let _ = ColumnValue::Year(2024);
        let _ = ColumnValue::Json(serde_json::json!({"key": "value"}));
        let _ = ColumnValue::Enum(1);
        let _ = ColumnValue::Set(7);
        let _ = ColumnValue::Bit(vec![0b10101010]);
    }

    #[test]
    fn test_rows_event_structure() {
        let row_data = RowData {
            before: Some(vec![ColumnValue::SignedInt(1)]),
            after: Some(vec![ColumnValue::SignedInt(2)]),
        };

        let event = RowsEvent {
            table_id: 42,
            flags: 0,
            column_count: 1,
            columns_before_image: vec![0b1],
            columns_after_image: Some(vec![0b1]),
            rows: vec![row_data],
        };

        assert_eq!(event.table_id, 42);
        assert_eq!(event.rows.len(), 1);
        assert!(event.rows[0].before.is_some());
        assert!(event.rows[0].after.is_some());
    }
}
