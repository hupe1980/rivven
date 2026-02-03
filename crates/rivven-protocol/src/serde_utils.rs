//! Serde utilities for bytes serialization
//!
//! Provides efficient serialization/deserialization for `bytes::Bytes` types.

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serializer};

/// Serde module for `Bytes` fields
pub mod bytes_serde {
    use super::*;

    pub fn serialize<S>(val: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(&val[..], serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Ok(Bytes::from(v))
    }
}

/// Serde module for `Option<Bytes>` fields
pub mod option_bytes_serde {
    use super::*;

    pub fn serialize<S>(val: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(v) => serializer.serialize_some(&serde_bytes::Bytes::new(&v[..])),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v: Option<Vec<u8>> = Deserialize::deserialize(deserializer)?;
        Ok(v.map(Bytes::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        #[serde(with = "bytes_serde")]
        data: Bytes,
        #[serde(with = "option_bytes_serde")]
        optional: Option<Bytes>,
    }

    #[test]
    fn test_bytes_serde_roundtrip() {
        let msg = TestMessage {
            data: Bytes::from("hello"),
            optional: Some(Bytes::from("world")),
        };

        let encoded = postcard::to_allocvec(&msg).unwrap();
        let decoded: TestMessage = postcard::from_bytes(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_bytes_serde_none() {
        let msg = TestMessage {
            data: Bytes::from("test"),
            optional: None,
        };

        let encoded = postcard::to_allocvec(&msg).unwrap();
        let decoded: TestMessage = postcard::from_bytes(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }
}
