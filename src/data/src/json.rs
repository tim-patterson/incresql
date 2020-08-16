use std::convert::TryInto;

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
/// 0x03 - 1 byte signed integer, len = 2
/// 0x04 - 2 byte signed integer, len = 3
/// 0x05 - 4 byte signed integer, len = 5
/// 0x06 - 8 byte signed integer, len = 9
/// 0x07 - Decimal s=1, m=1 bytes, len=2
/// 0x08 - Decimal s=1, m=2 bytes, len=3
/// 0x09 - Decimal s=1, m=4 bytes, len=5
/// 0x0a - Decimal s=1, m=8 bytes, len=9
/// 0x0b - Decimal s=1, m=12 bytes, len=13
/// 0x0c - Decimal s=2, m=1 bytes, len=2
/// 0x0d - Decimal s=2, m=2 bytes, len=3
/// 0x0e - Decimal s=2, m=4 bytes, len=5
/// 0x0f - Decimal s=2, m=8 bytes, len=9
/// 0x10 - Decimal s=2, m=12 bytes, len=13
/// 0x11 - Decimal m=1 bytes, len=3
/// 0x12 - Decimal m=2 bytes, len=4
/// 0x13 - Decimal m=4 bytes, len=6
/// 0x14 - Decimal m=8 bytes, len=10
/// 0x15 - Decimal m=12 bytes, len=14
/// 0x16 - String empty, len=1
/// 0x17 - String, size = 1 bytes, len=var
/// 0x18 - String, size = 2 bytes, len=var
/// 0x19 - String, size = 4 bytes, len=var
/// 0x1a - Array empty, len=1
/// 0x1b - Array, size = 1 bytes, len=var
/// 0x1c - Array, size = 2 bytes, len=var
/// 0x1d - Array, size = 4 bytes, len=var
/// 0x1e - Object empty, len=1
/// 0x1f - Object, size = 1 bytes, len=var
/// 0x20 - Object, size = 2 bytes, len=var
/// 0x21 - Object, size = 4 bytes, len=var

pub struct Json<'a> {
    bytes: &'a [u8],
}

impl<'a> Json<'a> {
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

    /// Returns the string contents of this node
    pub fn get_string(&'a self) -> Option<&'a [u8]> {
        match self.bytes[0] {
            0x16 => Some(&[]),
            0x17..=0x19 => Some(self.read_varlen(0x17).1),
            _ => None,
        }
    }

    /// Reads the data len for var length types, and returns the rest of the bytes
    /// Takes the base tag where len = 1.
    fn read_varlen(&self, base: u8) -> (usize, &[u8]) {
        match self.bytes[0] - base {
            // 1 byte
            0 => (self.bytes[1] as usize, &self.bytes[2..]),
            // 2 bytes
            1 => {
                let len = u16::from_le_bytes(self.bytes[1..3].as_ref().try_into().unwrap());
                (len as usize, &self.bytes[3..])
            }
            // 4 bytes
            2 => {
                let len = u32::from_le_bytes(self.bytes[1..5].as_ref().try_into().unwrap());
                (len as usize, &self.bytes[5..])
            }
            _ => panic!(),
        }
    }
}
