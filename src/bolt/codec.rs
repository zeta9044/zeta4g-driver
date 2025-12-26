//! Bolt protocol codec for tokio_util.
//!
//! Implements chunked message framing as per Bolt protocol specification.
//! Messages are split into chunks with a 2-byte length prefix.

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::packstream::{decode, encode, PackStreamValue};
use super::message::{BoltRequest, BoltResponse};
use super::BoltError;

/// Maximum chunk size (16KB)
pub const MAX_CHUNK_SIZE: usize = 16384;

/// Minimum chunk size
pub const MIN_CHUNK_SIZE: usize = 8;

/// End of message marker (0x00 0x00)
pub const END_MARKER: [u8; 2] = [0x00, 0x00];

/// Bolt message codec for framing.
#[derive(Debug)]
pub struct BoltCodec {
    /// Maximum message size
    max_message_size: usize,
    /// Buffer for accumulating chunks
    message_buffer: BytesMut,
    /// State: are we in the middle of a message?
    in_message: bool,
}

impl BoltCodec {
    /// Create a new codec with default settings.
    pub fn new() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024, // 16MB default
            message_buffer: BytesMut::with_capacity(4096),
            in_message: false,
        }
    }

    /// Create a codec with custom max message size.
    pub fn with_max_size(max_message_size: usize) -> Self {
        Self {
            max_message_size,
            message_buffer: BytesMut::with_capacity(4096),
            in_message: false,
        }
    }

    /// Encode a message into chunks.
    fn encode_chunked(&self, data: &[u8], dst: &mut BytesMut) {
        let mut offset = 0;

        while offset < data.len() {
            let remaining = data.len() - offset;
            let chunk_size = remaining.min(MAX_CHUNK_SIZE);

            // Write chunk header (2-byte big-endian length)
            dst.put_u16(chunk_size as u16);
            // Write chunk data
            dst.put_slice(&data[offset..offset + chunk_size]);

            offset += chunk_size;
        }

        // Write end marker
        dst.put_slice(&END_MARKER);
    }
}

impl Default for BoltCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for BoltCodec {
    type Item = PackStreamValue;
    type Error = BoltError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            // Need at least 2 bytes for chunk header
            if src.len() < 2 {
                return Ok(None);
            }

            // Peek at chunk size
            let chunk_size = u16::from_be_bytes([src[0], src[1]]) as usize;

            // Check for end marker
            if chunk_size == 0 {
                src.advance(2); // Consume end marker

                if self.message_buffer.is_empty() {
                    // Empty message - could be NOOP, skip
                    continue;
                }

                // Decode complete message
                let message_data = self.message_buffer.split();
                self.in_message = false;

                let value = decode(&message_data).map_err(|e| BoltError::PackStream(e))?;

                return Ok(Some(value));
            }

            // Need chunk header + chunk data
            if src.len() < 2 + chunk_size {
                return Ok(None);
            }

            // Check message size limit
            if self.message_buffer.len() + chunk_size > self.max_message_size {
                return Err(BoltError::MessageTooLarge {
                    size: self.message_buffer.len() + chunk_size,
                    max: self.max_message_size,
                });
            }

            // Consume chunk header
            src.advance(2);

            // Append chunk data to message buffer
            self.message_buffer.extend_from_slice(&src[..chunk_size]);
            src.advance(chunk_size);
            self.in_message = true;
        }
    }
}

impl Encoder<PackStreamValue> for BoltCodec {
    type Error = BoltError;

    fn encode(&mut self, item: PackStreamValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = encode(&item).map_err(|e| BoltError::PackStream(e))?;
        self.encode_chunked(&encoded, dst);
        Ok(())
    }
}

/// Codec specifically for Bolt request messages.
#[derive(Debug, Default)]
pub struct BoltRequestCodec {
    inner: BoltCodec,
}

impl BoltRequestCodec {
    /// Create a new request codec.
    pub fn new() -> Self {
        Self {
            inner: BoltCodec::new(),
        }
    }
}

impl Decoder for BoltRequestCodec {
    type Item = BoltRequest;
    type Error = BoltError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src)? {
            Some(value) => {
                let structure = value
                    .as_structure()
                    .ok_or_else(|| BoltError::Protocol("Expected structure".to_string()))?;

                let request = BoltRequest::from_structure(structure)
                    .map_err(|e| BoltError::PackStream(e))?;

                Ok(Some(request))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<BoltResponse> for BoltRequestCodec {
    type Error = BoltError;

    fn encode(&mut self, item: BoltResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let structure = item.to_structure();
        let value = PackStreamValue::Structure(structure);
        self.inner.encode(value, dst)
    }
}

/// Codec specifically for Bolt response messages (client-side).
#[derive(Debug, Default)]
pub struct BoltResponseCodec {
    inner: BoltCodec,
}

impl BoltResponseCodec {
    /// Create a new response codec.
    pub fn new() -> Self {
        Self {
            inner: BoltCodec::new(),
        }
    }
}

impl Decoder for BoltResponseCodec {
    type Item = BoltResponse;
    type Error = BoltError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src)? {
            Some(value) => {
                let structure = value
                    .as_structure()
                    .ok_or_else(|| BoltError::Protocol("Expected structure".to_string()))?;

                let response = BoltResponse::from_structure(structure)
                    .map_err(|e| BoltError::PackStream(e))?;

                Ok(Some(response))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<BoltRequest> for BoltResponseCodec {
    type Error = BoltError;

    fn encode(&mut self, item: BoltRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let structure = item.to_structure();
        let value = PackStreamValue::Structure(structure);
        self.inner.encode(value, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bolt::message::{HelloMessage, SuccessMessage};
    use crate::bolt::packstream::PackStreamStructure;

    #[test]
    fn test_encode_decode_value() {
        let mut codec = BoltCodec::new();
        let mut buf = BytesMut::new();

        // Encode a simple value
        let value = PackStreamValue::String("Hello, Bolt!".to_string());
        codec.encode(value.clone(), &mut buf).unwrap();

        // Should have chunk header + data + end marker
        assert!(buf.len() > 2);

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.as_str().unwrap(), "Hello, Bolt!");
    }

    #[test]
    fn test_encode_decode_structure() {
        let mut codec = BoltCodec::new();
        let mut buf = BytesMut::new();

        // Encode a structure
        let structure = PackStreamStructure::new(0x70, vec![
            PackStreamValue::Map(std::collections::HashMap::new()),
        ]);
        let value = PackStreamValue::Structure(structure);
        codec.encode(value, &mut buf).unwrap();

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert!(decoded.as_structure().is_some());
    }

    #[test]
    fn test_chunked_large_message() {
        let mut codec = BoltCodec::new();
        let mut buf = BytesMut::new();

        // Create a large string (larger than MAX_CHUNK_SIZE)
        let large_data = "x".repeat(MAX_CHUNK_SIZE * 2 + 100);
        let value = PackStreamValue::String(large_data.clone());
        codec.encode(value, &mut buf).unwrap();

        // Should have multiple chunks
        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.as_str().unwrap(), large_data);
    }

    #[test]
    fn test_partial_chunk() {
        let mut codec = BoltCodec::new();
        let mut buf = BytesMut::new();

        // Encode a message
        let value = PackStreamValue::Integer(42);
        codec.encode(value, &mut buf).unwrap();

        // Save full buffer
        let full_buf = buf.clone();

        // Try to decode with only partial data
        let mut partial = BytesMut::from(&full_buf[..2]); // Only chunk header
        assert!(codec.decode(&mut partial).unwrap().is_none());

        // Now with full data
        let mut complete = full_buf;
        let decoded = codec.decode(&mut complete).unwrap().unwrap();
        assert_eq!(decoded.as_int().unwrap(), 42);
    }

    #[test]
    fn test_request_codec() {
        let mut codec = BoltRequestCodec::new();
        let mut buf = BytesMut::new();

        // Encode a SUCCESS response
        let response = BoltResponse::Success(SuccessMessage::hello_success("Zeta4G/1.0", "conn-1"));
        codec.encode(response, &mut buf).unwrap();

        // Decode should give us a structure
        let decoded = codec.inner.decode(&mut buf).unwrap().unwrap();
        assert!(decoded.as_structure().is_some());
    }

    #[test]
    fn test_message_too_large() {
        let mut codec = BoltCodec::with_max_size(100);
        let mut buf = BytesMut::new();

        // Create chunk header for large data
        buf.put_u16(200); // Claim 200 bytes
        buf.extend_from_slice(&[0u8; 200]); // Add data

        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(BoltError::MessageTooLarge { .. })));
    }

    #[test]
    fn test_empty_message_skipped() {
        let mut codec = BoltCodec::new();
        let mut buf = BytesMut::new();

        // Add empty message (just end marker)
        buf.put_slice(&END_MARKER);

        // Then add a real message
        let value = PackStreamValue::Boolean(true);
        let encoded = encode(&value).unwrap();
        buf.put_u16(encoded.len() as u16);
        buf.put_slice(&encoded);
        buf.put_slice(&END_MARKER);

        // Should skip empty and return the boolean
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.as_bool().unwrap(), true);
    }

    #[test]
    fn test_multiple_messages() {
        let mut codec = BoltCodec::new();
        let mut buf = BytesMut::new();

        // Encode multiple messages
        codec.encode(PackStreamValue::Integer(1), &mut buf).unwrap();
        codec.encode(PackStreamValue::Integer(2), &mut buf).unwrap();
        codec.encode(PackStreamValue::Integer(3), &mut buf).unwrap();

        // Decode all
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().as_int().unwrap(), 1);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().as_int().unwrap(), 2);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().as_int().unwrap(), 3);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
