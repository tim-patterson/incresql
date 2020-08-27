use rust_decimal::prelude::*;
use std::convert::TryInto;
use std::fmt::Debug;
use std::fmt::Formatter;

/// Json Data
/// There's a few in memory representations we could use for json.
/// 1. As a json serialized string, this can be efficiently passed around, read and written to the
///    underlying data store, but any function that interacts with the data needs expensive
///    parsing (and potentially serialization to write back out)
///
/// 2. As a DOM structure, this should be efficient enough for functions to access, theres a minor
///    risk of chasing pointers all over the heap causing cache misses but surely most json isn't
///    THAT nested...  The big downside with this approach is we still need to serialize/deserialize
///    to get the data off disk and that may be tricky to do without hitting the allocator really
///    hard.
///
/// 3. As a binary json or "tape" serialized representation, this is basically #1 with the hard bits
///    of the parsing already done. Functions should be able to be efficiently walk and extract data
///    This is what we will base our json data type on, we'll pay the cost of json parsing once
///    during data load and then queries should be relatively snappy.
///
/// Our encoding will be as follows:
/// 0x00 - NULL, len = 1
/// 0x01 - FALSE, len = 1
/// 0x02 - TRUE, len = 1
/// 0x03 - Decimal s=0, m=1 bytes, len=2
/// 0x04 - Decimal s=0, m=2 bytes, len=3
/// 0x05 - Decimal s=0, m=4 bytes, len=5
/// 0x06 - Decimal s=0, m=8 bytes, len=9
/// 0x07 - Decimal s=0, m=12 bytes, len=13
/// 0x08 - Decimal s=1, m=1 bytes, len=2
/// 0x09 - Decimal s=1, m=2 bytes, len=3
/// 0x0a - Decimal s=1, m=4 bytes, len=5
/// 0x0b - Decimal s=1, m=8 bytes, len=9
/// 0x0c - Decimal s=1, m=12 bytes, len=13
/// 0x0d - Decimal s=2, m=1 bytes, len=2
/// 0x0e - Decimal s=2, m=2 bytes, len=3
/// 0x0f - Decimal s=2, m=4 bytes, len=5
/// 0x10 - Decimal s=2, m=8 bytes, len=9
/// 0x11 - Decimal s=2, m=12 bytes, len=13
/// 0x12 - Decimal m=1 bytes, len=3
/// 0x13 - Decimal m=2 bytes, len=4
/// 0x14 - Decimal m=4 bytes, len=6
/// 0x15 - Decimal m=8 bytes, len=10
/// 0x16 - Decimal m=12 bytes, len=14
/// 0x17 - String empty, len=1
/// 0x18 - String, size = 1 bytes, len=var
/// 0x19 - String, size = 2 bytes, len=var
/// 0x1a - String, size = 4 bytes, len=var
/// 0x1b - Array empty, len=1
/// 0x1c - Array, size = 1 bytes, len=var
/// 0x1d - Array, size = 2 bytes, len=var
/// 0x1e - Array, size = 4 bytes, len=var
/// 0x1f - Object empty, len=1
/// 0x20 - Object, size = 1 bytes, len=var
/// 0x21 - Object, size = 2 bytes, len=var
/// 0x22 - Object, size = 4 bytes, len=var

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Json<'a> {
    pub(crate) bytes: &'a [u8],
}

/// This is just a wrapper around a vec<u8>, its whats returned from
/// the json builder
#[derive(Clone, Eq, PartialEq)]
pub struct OwnedJson {
    pub(crate) bytes: Vec<u8>,
}

impl OwnedJson {
    pub fn as_json(&self) -> Json<'_> {
        Json { bytes: &self.bytes }
    }

    pub fn parse(s: &str) -> Option<OwnedJson> {
        serde_json::from_str(s).ok()
    }
}

impl Debug for OwnedJson {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(&self.as_json()).unwrap())
    }
}

impl<'a> Json<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        Json { bytes }
    }

    /// Returns the node type
    pub fn json_type(&self) -> JsonType {
        if self.bytes.is_empty() {
            return JsonType::Null;
        }

        match self.bytes[0] {
            0x00 => JsonType::Null,
            0x01..=0x02 => JsonType::Boolean,
            0x03..=0x16 => JsonType::Number,
            0x17..=0x1a => JsonType::String,
            0x1b..=0x1e => JsonType::Array,
            0x1f..=0x22 => JsonType::Object,
            b => panic!("Unknown json type {}", b),
        }
    }

    /// Returns the len in bytes for this node, including
    /// the discriminator tag
    pub fn size(&self) -> usize {
        if self.bytes.is_empty() {
            return 0;
        }

        match self.bytes[0] {
            // Null and bools
            0x00..=0x02 => 1,
            // Number fixed m=1
            0x03 | 0x08 | 0x0d => 2,
            // Number fixed m=2
            0x04 | 0x09 | 0x0e => 3,
            // Number fixed m=4
            0x05 | 0x0a | 0x0f => 5,
            // Number fixed m=8
            0x06 | 0x0b | 0x10 => 9,
            // Number fixed m=12
            0x07 | 0x0c | 0x11 => 13,
            // Number var s, fixed m=1
            0x12 => 3,
            // Number var s, fixed m=2
            0x13 => 5,
            // Number var s, fixed m=4
            0x14 => 6,
            // Number var s, fixed m=8
            0x15 => 10,
            // Number var s, fixed m=12
            0x16 => 14,
            // Empty str, array, object
            0x17 | 0x1b | 0x1f => 1,
            // Non empty str
            0x18..=0x1a => self.read_varlen(0x18).0,
            // Non empty array
            0x1c..=0x1e => self.read_varlen(0x1c).0,
            // Non empty object
            0x20..=0x22 => self.read_varlen(0x20).0,
            b => panic!("Unknown json type {}", b),
        }
    }

    /// Returns true if this json node is empty
    pub fn is_null(&self) -> bool {
        self.bytes.is_empty() || self.bytes[0] == 0x00
    }

    /// Returns the boolean data from this node
    pub fn get_boolean(&'a self) -> Option<bool> {
        match self.bytes[0] {
            0x01 => Some(false),
            0x02 => Some(true),
            _ => None,
        }
    }

    pub fn get_number(&'a self) -> Option<Decimal> {
        // Figure out the scale
        let (scale, rest) = match self.bytes[0] {
            0x03..=0x07 => (0_u8, &self.bytes[1..]),
            0x08..=0x0c => (1, &self.bytes[1..]),
            0x0d..=0x11 => (2, &self.bytes[1..]),
            0x12..=0x16 => (self.bytes[1], &self.bytes[2..]),
            _ => return None,
        };

        // Read in m
        match self.bytes[0] {
            0x03 | 0x08 | 0x0d | 0x12 => Some(Decimal::new(rest[0] as i8 as i64, scale as u32)),
            0x04 | 0x09 | 0x0e | 0x13 => {
                let m = i16::from_le_bytes(rest[..2].as_ref().try_into().unwrap());
                Some(Decimal::new(m as i64, scale as u32))
            }
            0x05 | 0x0a | 0x0f | 0x14 => {
                let m = i32::from_le_bytes(rest[..4].as_ref().try_into().unwrap());
                Some(Decimal::new(m as i64, scale as u32))
            }
            0x06 | 0x0b | 0x10 | 0x15 => {
                let m = i64::from_le_bytes(rest[..8].as_ref().try_into().unwrap());
                Some(Decimal::new(m, scale as u32))
            }
            0x07 | 0x0c | 0x11 | 0x16 => {
                let mut bytes = [0_u8; 16];
                bytes[..12].copy_from_slice(&rest[..12]);
                // We need to sign extend...
                if bytes[11] & 128 == 128 {
                    bytes[12..].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF])
                }
                let m = i128::from_le_bytes(bytes);
                Some(Decimal::from_i128_with_scale(m, scale as u32))
            }
            _ => None,
        }
    }

    /// Returns the string contents of this node
    pub fn get_string(&self) -> Option<&'a str> {
        match self.bytes[0] {
            0x17 => Some(""),
            0x18..=0x1a => Some(unsafe { std::str::from_utf8_unchecked(self.read_varlen(0x18).1) }),
            _ => None,
        }
    }

    /// Iter over an array
    pub fn iter_array(self) -> Option<impl Iterator<Item = Json<'a>>> {
        if self.json_type() == JsonType::Array {
            Some(JsonIter {
                json: Json {
                    bytes: if self.bytes[0] == 0x1b {
                        [].as_ref()
                    } else {
                        self.read_varlen(0x1c).1
                    },
                },
            })
        } else {
            None
        }
    }

    /// Iter over an object
    pub fn iter_object(self) -> Option<impl Iterator<Item = (&'a str, Json<'a>)>> {
        if self.json_type() == JsonType::Object {
            Some(JsonObjectIter {
                inner: JsonIter {
                    json: Json {
                        bytes: if self.bytes[0] == 0x1f {
                            [].as_ref()
                        } else {
                            self.read_varlen(0x20).1
                        },
                    },
                },
            })
        } else {
            None
        }
    }

    /// Reads the full packet len for var length types, and returns the rest of the bytes
    /// Takes the base tag where len = 1.
    fn read_varlen(&self, base: u8) -> (usize, &'a [u8]) {
        match self.bytes[0] - base {
            // 1 byte
            0 => (self.bytes[1] as usize + 2, &self.bytes[2..]),
            // 2 bytes
            1 => {
                let len = u16::from_le_bytes(self.bytes[1..3].as_ref().try_into().unwrap());
                (len as usize + 3, &self.bytes[3..])
            }
            // 4 bytes
            2 => {
                let len = u32::from_le_bytes(self.bytes[1..5].as_ref().try_into().unwrap());
                (len as usize + 5, &self.bytes[5..])
            }
            _ => panic!(),
        }
    }

    /// This function is for the iters for arrays and objects where we've got a
    /// json that actually contains a bunch of objects end to end, this function
    /// splits off first item and returns the rest.
    /// Size is passed in here as its not cheap to calculate and in most cases
    /// we already have evaluated in the calling function.
    fn split_first(&self, size: usize) -> (Json<'a>, Json<'a>) {
        (
            Json {
                bytes: &self.bytes[..size],
            },
            Json {
                bytes: &self.bytes[size..],
            },
        )
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum JsonType {
    Null,
    Boolean,
    Number,
    String,
    Object,
    Array,
}

/// An iterator that iterates over json nodes.
struct JsonIter<'a> {
    json: Json<'a>,
}

impl<'a> Iterator for JsonIter<'a> {
    type Item = Json<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let size = self.json.size();
        if size == 0 {
            None
        } else {
            let (next, rest) = self.json.split_first(size);
            self.json = rest;
            Some(next)
        }
    }
}

/// An iterator that iterates over json objects
struct JsonObjectIter<'a> {
    inner: JsonIter<'a>,
}

impl<'a> Iterator for JsonObjectIter<'a> {
    type Item = (&'a str, Json<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let (Some(key), Some(value)) = (self.inner.next(), self.inner.next()) {
            Some((key.get_string().unwrap(), value))
        } else {
            None
        }
    }
}

/// A builder to build json tapes
pub struct JsonBuilder {
    pub(crate) inner: JsonBuilderInner,
}

impl Default for JsonBuilder {
    fn default() -> Self {
        JsonBuilder {
            inner: JsonBuilderInner::default(),
        }
    }
}

impl JsonBuilder {
    /// Creates a json tape containing a single null
    pub fn null(mut self) -> OwnedJson {
        self.inner.push_null();
        self.inner.build()
    }

    /// Creates a json tape containing a single bool
    pub fn bool(mut self, b: bool) -> OwnedJson {
        self.inner.push_bool(b);
        self.inner.build()
    }

    /// Creates a json tape containing a single number
    pub fn int(mut self, i: i64) -> OwnedJson {
        self.inner.push_int(i);
        self.inner.build()
    }

    /// Creates a json tape containing a single number
    pub fn decimal(mut self, d: Decimal) -> OwnedJson {
        self.inner.push_decimal(d);
        self.inner.build()
    }

    /// Creates a json tape containing a single string
    pub fn string(mut self, s: &str) -> OwnedJson {
        self.inner.push_string(s);
        self.inner.build()
    }

    /// Creates a json array
    pub fn array<F: FnOnce(&mut ArrayJsonBuilder)>(mut self, f: F) -> OwnedJson {
        self.inner.push_array(f);
        self.inner.build()
    }

    /// Creates a json object
    pub fn object<F: FnOnce(&mut ObjectJsonBuilder)>(mut self, f: F) -> OwnedJson {
        self.inner.push_object(f);
        self.inner.build()
    }
}

/// Builder for arrays
pub struct ArrayJsonBuilder<'a> {
    pub(crate) inner: &'a mut JsonBuilderInner,
}

impl ArrayJsonBuilder<'_> {
    /// Append a null
    pub fn push_null(&mut self) {
        self.inner.push_null();
    }

    /// Append a bool
    pub fn push_bool(&mut self, b: bool) {
        self.inner.push_bool(b);
    }

    /// Append a number
    pub fn push_int(&mut self, i: i64) {
        self.inner.push_int(i);
    }

    /// Append a number
    pub fn push_decimal(&mut self, d: Decimal) {
        self.inner.push_decimal(d);
    }

    /// Append a string
    pub fn push_string(&mut self, s: &str) {
        self.inner.push_string(s);
    }

    /// Append a json array
    pub fn push_array<F: FnOnce(&mut ArrayJsonBuilder)>(&mut self, f: F) {
        self.inner.push_array(f);
    }

    /// Append a json object
    pub fn push_object<F: FnOnce(&mut ObjectJsonBuilder)>(&mut self, f: F) {
        self.inner.push_object(f);
    }

    /// Append an existing json object/reference
    pub fn push_json(&mut self, j: Json) {
        self.inner.push_json(j);
    }
}

/// Builder for objects
pub struct ObjectJsonBuilder<'a> {
    pub(crate) inner: &'a mut JsonBuilderInner,
}

impl ObjectJsonBuilder<'_> {
    /// Append a null
    pub fn push_null(&mut self, key: &str) {
        self.inner.push_string(key);
        self.inner.push_null();
    }

    /// Append a bool
    pub fn push_bool(&mut self, key: &str, b: bool) {
        self.inner.push_string(key);
        self.inner.push_bool(b);
    }

    /// Append a number
    pub fn push_int(&mut self, key: &str, i: i64) {
        self.inner.push_string(key);
        self.inner.push_int(i);
    }

    /// Append a number
    pub fn push_decimal(&mut self, key: &str, d: Decimal) {
        self.inner.push_string(key);
        self.inner.push_decimal(d);
    }

    /// Append a string
    pub fn push_string(&mut self, key: &str, s: &str) {
        self.inner.push_string(key);
        self.inner.push_string(s);
    }

    /// Append a json array
    pub fn push_array<F: FnOnce(&mut ArrayJsonBuilder)>(&mut self, key: &str, f: F) {
        self.inner.push_string(key);
        self.inner.push_array(f);
    }

    /// Append a json object
    pub fn push_object<F: FnOnce(&mut ObjectJsonBuilder)>(&mut self, key: &str, f: F) {
        self.inner.push_string(key);
        self.inner.push_object(f);
    }
}

/// Impl part of JsonBuilder that knows how to work with all the types.
#[derive(Default, Debug)]
pub(crate) struct JsonBuilderInner {
    bytes: Vec<u8>,
}

impl JsonBuilderInner {
    /// Push a null onto the json tape
    pub(crate) fn push_null(&mut self) {
        self.bytes.push(0x00);
    }

    /// Push a bool onto the json tape
    pub(crate) fn push_bool(&mut self, b: bool) {
        if b {
            self.bytes.push(0x02);
        } else {
            self.bytes.push(0x01);
        }
    }

    /// Push an int onto the json tape
    pub(crate) fn push_int(&mut self, i: i64) {
        if i8::MIN as i64 <= i && i <= i8::MAX as i64 {
            self.bytes.push(0x03);
            self.bytes.push(i as i8 as u8);
        } else if i16::MIN as i64 <= i && i <= i16::MAX as i64 {
            self.bytes.push(0x04);
            self.bytes.extend_from_slice(&(i as i16).to_le_bytes());
        } else if i32::MIN as i64 <= i && i <= i32::MAX as i64 {
            self.bytes.push(0x05);
            self.bytes.extend_from_slice(&(i as i32).to_le_bytes());
        } else {
            self.bytes.push(0x06);
            self.bytes.extend_from_slice(&i.to_le_bytes());
        }
    }

    /// Push a decimal onto the json tape
    pub(crate) fn push_decimal(&mut self, d: Decimal) {
        d.normalize();
        let unpacked = d.unpack();
        let mut m =
            unpacked.lo as i128 + ((unpacked.mid as i128) << 32) + ((unpacked.hi as i128) << 64);
        if unpacked.is_negative {
            m = -m;
        }
        let (tags, s) = match unpacked.scale {
            0 => ([0x03_u8, 0x04, 0x05, 0x06, 0x07], None),
            1 => ([0x08_u8, 0x09, 0x0a, 0x0b, 0x0c], None),
            2 => ([0x0d_u8, 0x0e, 0x0f, 0x10, 0x11], None),
            s => ([0x12_u8, 0x13, 0x14, 0x15, 0x16], Some(s as u8)),
        };

        if i8::MIN as i128 <= m && m <= i8::MAX as i128 {
            self.bytes.push(tags[0]);
            if let Some(scale) = s {
                self.bytes.push(scale);
            }
            self.bytes.push(m as i8 as u8);
        } else if i16::MIN as i128 <= m && m <= i16::MAX as i128 {
            self.bytes.push(tags[1]);
            if let Some(scale) = s {
                self.bytes.push(scale);
            }
            self.bytes.extend_from_slice(&(m as i16).to_le_bytes());
        } else if i32::MIN as i128 <= m && m <= i32::MAX as i128 {
            self.bytes.push(tags[2]);
            if let Some(scale) = s {
                self.bytes.push(scale);
            }
            self.bytes.extend_from_slice(&(m as i32).to_le_bytes());
        } else if i64::MIN as i128 <= m && m <= i64::MAX as i128 {
            self.bytes.push(tags[3]);
            if let Some(scale) = s {
                self.bytes.push(scale);
            }
            self.bytes.extend_from_slice(&(m as i64).to_le_bytes());
        } else {
            self.bytes.push(tags[4]);
            if let Some(scale) = s {
                self.bytes.push(scale);
            }
            // Copy over sign bit
            let mut bytes = m.to_le_bytes();
            if bytes[15] & 128 == 128 {
                bytes[11] |= 128;
            }
            self.bytes.extend_from_slice(&bytes[..12]);
        }
    }

    /// Push string onto the json tape
    pub(crate) fn push_string(&mut self, s: &str) {
        let len = s.len();
        if len == 0 {
            self.bytes.push(0x17);
        } else if len <= u8::MAX as usize {
            self.bytes.push(0x18);
            self.bytes.push(len as u8);
            self.bytes.extend_from_slice(s.as_ref());
        } else if len <= u16::MAX as usize {
            self.bytes.push(0x19);
            self.bytes.extend_from_slice(&(len as u16).to_le_bytes());
            self.bytes.extend_from_slice(s.as_ref());
        } else if len <= u32::MAX as usize {
            self.bytes.push(0x1a);
            self.bytes.extend_from_slice(&(len as u32).to_le_bytes());
            self.bytes.extend_from_slice(s.as_ref());
        } else {
            panic!("String too large @ {} bytes", len);
        }
    }

    /// Creates a json array
    pub(crate) fn push_array<F: FnOnce(&mut ArrayJsonBuilder)>(&mut self, f: F) {
        // We'll optimistically hope that our array is <= 256 bytes long, so we'll only
        // reserve 1 byte for the length, if we're wrong we'll just have to do an memmove.
        self.bytes.push(0x1b);
        self.bytes.push(0x00);
        let len_before = self.bytes.len();
        let mut array_builder = ArrayJsonBuilder { inner: self };
        f(&mut array_builder);
        let len_after = self.bytes.len();
        let array_len = len_after - len_before;
        if array_len == 0 {
            // Zero sized, pop off the len byte
            self.bytes.pop();
        } else if array_len <= u8::MAX as usize {
            self.bytes[len_before - 2] = 0x1c;
            self.bytes[len_before - 1] = array_len as u8;
        } else if array_len <= u16::MAX as usize {
            self.bytes[len_before - 2] = 0x1d;
            self.bytes.insert(len_before - 1, 0x00);
            self.bytes[(len_before - 1)..(len_before + 1)]
                .copy_from_slice(&(array_len as u16).to_le_bytes());
        } else if array_len <= u32::MAX as usize {
            self.bytes[len_before - 2] = 0x1e;
            // Move things over 3 bytes
            self.bytes.push(0x00);
            self.bytes.push(0x00);
            self.bytes.push(0x00);
            self.bytes
                .copy_within(len_before..len_after, len_before + 3);
            self.bytes[(len_before - 1)..(len_before + 3)]
                .copy_from_slice(&(array_len as u32).to_le_bytes());
        } else {
            panic!("Oversized array {}", array_len);
        }
    }

    /// Creates a json array
    pub(crate) fn push_object<F: FnOnce(&mut ObjectJsonBuilder)>(&mut self, f: F) {
        // We'll optimistically hope that our array is <= 256 bytes long, so we'll only
        // reserve 1 byte for the length, if we're wrong we'll just have to do an memmove.
        self.bytes.push(0x1f);
        self.bytes.push(0x00);
        let len_before = self.bytes.len();
        let mut object_builder = ObjectJsonBuilder { inner: self };
        f(&mut object_builder);
        let len_after = self.bytes.len();
        let array_len = len_after - len_before;
        if array_len == 0 {
            // Zero sized, pop off the len byte
            self.bytes.pop();
        } else if array_len <= u8::MAX as usize {
            self.bytes[len_before - 2] = 0x20;
            self.bytes[len_before - 1] = array_len as u8;
        } else if array_len <= u16::MAX as usize {
            self.bytes[len_before - 2] = 0x21;
            self.bytes.insert(len_before - 1, 0x00);
            self.bytes[(len_before - 1)..(len_before + 1)]
                .copy_from_slice(&(array_len as u16).to_le_bytes());
        } else if array_len <= u32::MAX as usize {
            self.bytes[len_before - 2] = 0x22;
            // Move things over 3 bytes
            self.bytes.push(0x00);
            self.bytes.push(0x00);
            self.bytes.push(0x00);
            self.bytes
                .copy_within(len_before..len_after, len_before + 3);
            self.bytes[(len_before - 1)..(len_before + 3)]
                .copy_from_slice(&(array_len as u32).to_le_bytes());
        } else {
            panic!("Oversized object {}", array_len);
        }
    }

    pub(crate) fn push_json(&mut self, j: Json) {
        if j.bytes.is_empty() {
            self.push_null()
        } else {
            self.bytes.extend_from_slice(j.bytes);
        }
    }

    /// Return the tape as a vector of bytes
    pub(crate) fn build(self) -> OwnedJson {
        OwnedJson { bytes: self.bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let builder = JsonBuilder::default();
        let owned_json = builder.null();
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Null);
        assert_eq!(json.size(), 1);
        assert_eq!(json.bytes.len(), 1);
        assert!(json.is_null());
    }

    #[test]
    fn test_bool() {
        let builder = JsonBuilder::default();
        let owned_json = builder.bool(true);
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Boolean);
        assert_eq!(json.size(), 1);
        assert_eq!(json.bytes.len(), 1);
        assert_eq!(json.get_boolean(), Some(true));
    }

    #[test]
    fn test_small_int() {
        let builder = JsonBuilder::default();
        let owned_json = builder.int(-10);
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Number);
        assert_eq!(json.size(), 2);
        assert_eq!(json.bytes.len(), 2);
        assert_eq!(json.get_number(), Some(Decimal::new(-10, 0)));
    }

    #[test]
    fn test_med_int() {
        let builder = JsonBuilder::default();
        let owned_json = builder.int(100000);
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Number);
        assert_eq!(json.size(), 5);
        assert_eq!(json.bytes.len(), 5);
        assert_eq!(json.get_number(), Some(Decimal::new(100000, 0)));
    }

    #[test]
    fn test_decimal() {
        let builder = JsonBuilder::default();
        let owned_json = builder.decimal(Decimal::from_i128_with_scale(
            -1234567890123456789012345,
            10,
        ));
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Number);
        assert_eq!(json.size(), 14);
        assert_eq!(json.bytes.len(), 14);
        assert_eq!(
            json.get_number(),
            Some(Decimal::from_i128_with_scale(
                -1234567890123456789012345_i128,
                10
            ))
        );
    }

    #[test]
    fn test_string() {
        let builder = JsonBuilder::default();
        let owned_json = builder.string("hello world");
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::String);
        assert_eq!(json.size(), 13);
        assert_eq!(json.bytes.len(), 13);
        assert_eq!(json.get_string(), Some("hello world"));
    }

    #[test]
    fn test_array() {
        let builder = JsonBuilder::default();
        let owned_json = builder.array(|array| {
            array.push_int(1);
            array.push_array(|array| {
                array.push_int(2);
            });
            array.push_int(3);
        });
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Array);
        assert_eq!(json.size(), 10);
        assert_eq!(json.bytes.len(), 10);
        let mut iter = json.iter_array().unwrap();

        let first = iter.next().unwrap();
        assert_eq!(first.json_type(), JsonType::Number);
        assert_eq!(first.size(), 2);
        assert_eq!(first.bytes.len(), 2);
        assert_eq!(first.get_number(), Some(Decimal::new(1, 0)));

        let second = iter.next().unwrap();
        assert_eq!(second.json_type(), JsonType::Array);

        let third = iter.next().unwrap();
        assert_eq!(third.json_type(), JsonType::Number);

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_medium_array() {
        let builder = JsonBuilder::default();
        let owned_json = builder.array(|array| {
            for _ in 0..50 {
                // with tag and size this should be 10 bytes
                array.push_string("12345678");
            }
        });
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Array);
        // 500 for contents + 1 byte tag + 2 bytes size
        assert_eq!(json.size(), 503);
        assert_eq!(json.bytes.len(), 503);
        let iter = json.iter_array().unwrap();
        assert_eq!(iter.count(), 50);
    }

    #[test]
    fn test_large_array() {
        let builder = JsonBuilder::default();
        let owned_json = builder.array(|array| {
            for _ in 0..10000 {
                // with tag and size this should be 10 bytes
                array.push_string("12345678");
            }
        });
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Array);
        // 100000 for contents + 1 byte tag + 4 bytes size
        assert_eq!(json.size(), 100005);
        assert_eq!(json.bytes.len(), 100005);
        let iter = json.iter_array().unwrap();
        assert_eq!(iter.count(), 10000);
    }

    #[test]
    fn test_object() {
        let builder = JsonBuilder::default();
        let owned_json = builder.object(|object| {
            object.push_int("first", 1);
            object.push_object("empty_obj", |_| {});
            object.push_int("last", -1);
        });
        let json = owned_json.as_json();

        assert_eq!(json.json_type(), JsonType::Object);
        assert_eq!(json.size(), 31);
        assert_eq!(json.bytes.len(), 31);
        let mut iter = json.iter_object().unwrap();

        let (first_key, first) = iter.next().unwrap();
        assert_eq!(first_key, "first");
        assert_eq!(first.json_type(), JsonType::Number);
        assert_eq!(first.size(), 2);
        assert_eq!(first.bytes.len(), 2);
        assert_eq!(first.get_number(), Some(Decimal::new(1, 0)));

        let (_second_key, second) = iter.next().unwrap();
        assert_eq!(second.json_type(), JsonType::Object);

        let (last_key, last) = iter.next().unwrap();
        assert_eq!(last_key, "last");
        assert_eq!(last.json_type(), JsonType::Number);

        assert_eq!(iter.next(), None);
    }
}
