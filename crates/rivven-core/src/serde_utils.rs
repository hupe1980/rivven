use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serializer};

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

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        #[serde(with = "bytes_serde")]
        data: Bytes,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestOptional {
        #[serde(with = "option_bytes_serde")]
        data: Option<Bytes>,
    }

    #[test]
    fn test_bytes_serde_roundtrip() {
        let original = TestStruct {
            data: Bytes::from(vec![1, 2, 3, 4, 5]),
        };
        
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TestStruct = serde_json::from_str(&json).unwrap();
        
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_bytes_serde_empty() {
        let original = TestStruct {
            data: Bytes::new(),
        };
        
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TestStruct = serde_json::from_str(&json).unwrap();
        
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_option_bytes_some() {
        let original = TestOptional {
            data: Some(Bytes::from(vec![10, 20, 30])),
        };
        
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TestOptional = serde_json::from_str(&json).unwrap();
        
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_option_bytes_none() {
        let original = TestOptional { data: None };
        
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TestOptional = serde_json::from_str(&json).unwrap();
        
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_bincode_serialization() {
        let original = TestStruct {
            data: Bytes::from(b"hello world".to_vec()),
        };
        
        let bytes = bincode::serialize(&original).unwrap();
        let deserialized: TestStruct = bincode::deserialize(&bytes).unwrap();
        
        assert_eq!(original, deserialized);
    }
}
