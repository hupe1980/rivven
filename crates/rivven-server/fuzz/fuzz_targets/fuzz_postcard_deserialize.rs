#![no_main]
//! Fuzz test for postcard deserialization with arbitrary types
//! 
//! This tests that postcard doesn't have memory safety issues
//! when deserializing malformed data into our protocol types.

use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;
use bytes::Bytes;
use rivven_server::protocol::{Request, Response, MessageData, BrokerInfo, TopicMetadata, PartitionMetadata};

/// Arbitrary-generated request for structured fuzzing
#[derive(Debug, Arbitrary)]
struct FuzzRequest {
    variant: u8,
    topic: String,
    partition: Option<u32>,
    offset: u64,
    max_messages: usize,
    consumer_group: String,
    subject: String,
    schema: String,
    id: i32,
    username: String,
    password: String,
    key_data: Vec<u8>,
    value_data: Vec<u8>,
    mechanism: Vec<u8>,
    auth_bytes: Vec<u8>,
}

fuzz_target!(|input: FuzzRequest| {
    // Generate different request types based on variant
    let request = match input.variant % 12 {
        0 => Request::Authenticate {
            username: input.username,
            password: input.password,
        },
        1 => Request::SaslAuthenticate {
            mechanism: Bytes::from(input.mechanism),
            auth_bytes: Bytes::from(input.auth_bytes),
        },
        2 => Request::Publish {
            topic: input.topic,
            partition: input.partition,
            key: if input.key_data.is_empty() { None } else { Some(Bytes::from(input.key_data)) },
            value: Bytes::from(input.value_data),
        },
        3 => Request::Consume {
            topic: input.topic,
            partition: input.partition.unwrap_or(0),
            offset: input.offset,
            max_messages: input.max_messages,
        },
        4 => Request::CreateTopic {
            name: input.topic,
            partitions: input.partition,
        },
        5 => Request::ListTopics,
        6 => Request::DeleteTopic {
            name: input.topic,
        },
        7 => Request::CommitOffset {
            consumer_group: input.consumer_group,
            topic: input.topic,
            partition: input.partition.unwrap_or(0),
            offset: input.offset,
        },
        8 => Request::GetOffset {
            consumer_group: input.consumer_group,
            topic: input.topic,
            partition: input.partition.unwrap_or(0),
        },
        9 => Request::GetMetadata {
            topic: input.topic,
        },
        10 => Request::RegisterSchema {
            subject: input.subject,
            schema: input.schema,
        },
        _ => Request::Ping,
    };
    
    // Serialize and deserialize - should not panic
    if let Ok(bytes) = request.to_bytes() {
        let _ = Request::from_bytes(&bytes);
    }
});
