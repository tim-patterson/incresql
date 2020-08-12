use std::convert::TryInto;

/// Serializes Self to bytes while maintaining lexicographical sorting
pub trait SortableEncoding: Sized {
    /// Serializes Self to bytes while maintaining lexicographical sorting
    /// bytes are appended to buffer
    fn write_sortable_bytes(&self, buffer: &mut Vec<u8>);

    /// Deserializes from buffer
    /// returns the "rest" of the buffer that didn't get consumed
    fn read_sortable_bytes<'a>(&mut self, buffer: &'a [u8]) -> &'a [u8];
}

impl SortableEncoding for u64 {
    fn write_sortable_bytes(&self, buffer: &mut Vec<u8>) {
        write_varint_unsigned(*self, buffer);
    }

    fn read_sortable_bytes<'a>(&mut self, buffer: &'a [u8]) -> &'a [u8] {
        read_varint_unsigned(self, buffer)
    }
}

impl SortableEncoding for i64 {
    fn write_sortable_bytes(&self, buffer: &mut Vec<u8>) {
        write_varint_signed(*self, buffer);
    }

    fn read_sortable_bytes<'a>(&mut self, buffer: &'a [u8]) -> &'a [u8] {
        read_varint_signed(self, buffer)
    }
}

/// Writes an unsigned int into a buffer with lexicographical sort attempting
/// to not use too much space
fn write_varint_unsigned(i: u64, buffer: &mut Vec<u8>) {
    // To maintain the lexicographical sorting we'll use the first byte to encode the size of
    // the integer, with the integer itself encoded as bigendian we'll encode very small values
    // into the discriminator.
    if i < 253 {
        buffer.push(i as u8);
    } else if i <= u16::MAX as u64 {
        buffer.push(253);
        buffer.extend_from_slice((i as u16).to_be_bytes().as_ref());
    } else if i <= u32::MAX as u64 {
        buffer.push(254);
        buffer.extend_from_slice((i as u32).to_be_bytes().as_ref());
    } else {
        buffer.push(255);
        buffer.extend_from_slice(i.to_be_bytes().as_ref());
    }
}

/// Read an unsigned int from a buffer
fn read_varint_unsigned<'a>(i: &mut u64, buffer: &'a [u8]) -> &'a [u8] {
    let rem = &buffer[1..];
    match buffer[0] {
        253 => {
            *i = u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap()) as u64;
            &rem[2..]
        }
        254 => {
            *i = u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap()) as u64;
            &rem[4..]
        }
        255 => {
            *i = u64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
            &rem[8..]
        }
        b => {
            *i = b as u64;
            rem
        }
    }
}

/// Writes a signed int into a buffer with lexicographical sort attempting
/// to not use too much space
fn write_varint_signed(i: i64, buffer: &mut Vec<u8>) {
    // To maintain the lexicographical sorting we'll use the first byte to encode the size and sign
    // of the integer.
    // 0 for -i64, 1 for -u32, 2 for -u16, 3 for -u8
    // 255 for i64, 254 for u32, 253 for u16, 252 for u8
    // As we're using the discriminator to store the sign we'll use unsigned encoding to
    // squeeze a tiny bit more space out without having to resort to bit shifting etc
    // That leaves space for 248 small values, positives will be more likely so we'll
    // make 4 = -100, which means 251 = 148 with a "displacement" of 103
    #[allow(clippy::collapsible_if)]
    if i >= 0 {
        if i <= 148 {
            buffer.push(i as u8 + 103);
        } else if i <= u8::MAX as i64 {
            buffer.push(252);
            buffer.push(i as u8);
        } else if i <= u16::MAX as i64 {
            buffer.push(253);
            buffer.extend_from_slice((i as u16).to_be_bytes().as_ref());
        } else if i <= u32::MAX as i64 {
            buffer.push(254);
            buffer.extend_from_slice((i as u32).to_be_bytes().as_ref());
        } else {
            buffer.push(255);
            buffer.extend_from_slice(i.to_be_bytes().as_ref());
        }
    } else {
        if i >= -99 {
            buffer.push((i + 103) as u8);
        } else if i >= -(u8::MAX as i64) {
            buffer.push(3);
            buffer.push((-i as u8) ^ 0xFF);
        } else if i >= -(u16::MAX as i64) {
            buffer.push(2);
            buffer.extend_from_slice(((-i as u16) ^ 0xFFFF).to_be_bytes().as_ref());
        } else if i >= -(u32::MAX as i64) {
            buffer.push(1);
            buffer.extend_from_slice(((-i as u32) ^ 0xFFFFFFFF).to_be_bytes().as_ref());
        } else {
            buffer.push(0);
            buffer.extend_from_slice(i.to_be_bytes().as_ref());
        }
    }
}

/// Read an signed int from a buffer
fn read_varint_signed<'a>(i: &mut i64, buffer: &'a [u8]) -> &'a [u8] {
    let rem = &buffer[1..];
    match buffer[0] {
        0 => {
            *i = i64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
            &rem[8..]
        }
        1 => {
            *i = -((u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap()) ^ 0xFFFFFFFF) as i64);
            &rem[4..]
        }
        2 => {
            *i = -((u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap()) ^ 0xFFFF) as i64);
            &rem[2..]
        }
        3 => {
            *i = -((rem[0] ^ 0xFF) as i64);
            &rem[1..]
        }
        252 => {
            *i = rem[0] as i64;
            &rem[1..]
        }
        253 => {
            *i = u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap()) as i64;
            &rem[2..]
        }
        254 => {
            *i = u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap()) as i64;
            &rem[4..]
        }
        255 => {
            *i = i64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
            &rem[8..]
        }
        b => {
            *i = b as i64 - 103;
            rem
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_unsigned() {
        let mut byte_arrays = vec![];
        let numbers = [67_u64, 9123, 32000, 7832432, 8920398049823];

        // Encode into separate buffers
        for i in &numbers {
            let mut buf = vec![];
            i.write_sortable_bytes(&mut buf);
            byte_arrays.push(buf);
        }

        // Sort the buffers!
        byte_arrays.sort();

        assert_eq!(byte_arrays.len(), numbers.len());

        // Decode and make sure we're still in numeric order
        for (expected, buf) in numbers.iter().zip(byte_arrays) {
            let mut actual = 0_u64;
            let rem = actual.read_sortable_bytes(&buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());
        }
    }

    #[test]
    fn test_varint_signed() {
        let mut byte_arrays = vec![];
        let numbers = [
            -8920398049823,
            -37843794,
            -101,
            -100,
            -99,
            67_i64,
            252,
            9123,
            32000,
            7832432,
            8920398049823,
        ];

        // Encode into separate buffers
        for i in &numbers {
            let mut buf = vec![];
            i.write_sortable_bytes(&mut buf);
            byte_arrays.push(buf);
        }

        // Sort the buffers!
        byte_arrays.sort();

        assert_eq!(byte_arrays.len(), numbers.len());

        // Decode and make sure we're still in numeric order
        for (expected, buf) in numbers.iter().zip(byte_arrays) {
            let mut actual = 0_i64;
            let rem = actual.read_sortable_bytes(&buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());
        }
    }
}
