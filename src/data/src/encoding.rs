use crate::SortOrder;
use std::cmp::min;
use std::convert::TryInto;

/// Serializes Self to bytes while maintaining lexicographical sorting
pub trait SortableEncoding {
    /// Serializes Self to bytes while maintaining lexicographical sorting
    /// bytes are appended to buffer
    fn write_sortable_bytes(&self, sort_order: SortOrder, buffer: &mut Vec<u8>);

    /// Deserializes from buffer
    /// returns the "rest" of the buffer that didn't get consumed
    fn read_sortable_bytes<'a>(&mut self, sort_order: SortOrder, buffer: &'a [u8]) -> &'a [u8];
}

impl SortableEncoding for u64 {
    fn write_sortable_bytes(&self, sort_order: SortOrder, buffer: &mut Vec<u8>) {
        write_varint_unsigned(*self, sort_order, buffer);
    }

    fn read_sortable_bytes<'a>(&mut self, sort_order: SortOrder, buffer: &'a [u8]) -> &'a [u8] {
        read_varint_unsigned(self, sort_order, buffer)
    }
}

impl SortableEncoding for i64 {
    fn write_sortable_bytes(&self, sort_order: SortOrder, buffer: &mut Vec<u8>) {
        write_varint_signed(*self, sort_order, buffer);
    }

    fn read_sortable_bytes<'a>(&mut self, sort_order: SortOrder, buffer: &'a [u8]) -> &'a [u8] {
        read_varint_signed(self, sort_order, buffer)
    }
}

impl SortableEncoding for Vec<u8> {
    fn write_sortable_bytes(&self, sort_order: SortOrder, buffer: &mut Vec<u8>) {
        write_sortable_bytes(&self, sort_order, buffer);
    }

    fn read_sortable_bytes<'a>(&mut self, sort_order: SortOrder, buffer: &'a [u8]) -> &'a [u8] {
        read_sortable_bytes(self, sort_order, buffer)
    }
}

impl SortableEncoding for [u8] {
    fn write_sortable_bytes(&self, sort_order: SortOrder, buffer: &mut Vec<u8>) {
        write_sortable_bytes(&self, sort_order, buffer);
    }

    fn read_sortable_bytes<'a>(&mut self, _sort_order: SortOrder, _buffer: &'a [u8]) -> &'a [u8] {
        unimplemented!("Attempted to read var length data into a fixed size slice")
    }
}

/// Writes an unsigned int into a buffer with lexicographical sort attempting
/// to not use too much space
fn write_varint_unsigned(i: u64, sort_order: SortOrder, buffer: &mut Vec<u8>) {
    // To maintain the lexicographical sorting we'll use the first byte to encode the size of
    // the integer, with the integer itself encoded as bigendian we'll encode very small values
    // into the discriminator, for desc, we'll just flip all the bits
    #[allow(clippy::collapsible_if)]
    if sort_order.is_desc() {
        if i < 253 {
            buffer.push(!(i as u8));
        } else if i <= u16::MAX as u64 {
            buffer.push(!253);
            buffer.extend_from_slice((!(i as u16)).to_be_bytes().as_ref());
        } else if i <= u32::MAX as u64 {
            buffer.push(!254);
            buffer.extend_from_slice((!(i as u32)).to_be_bytes().as_ref());
        } else {
            buffer.push(!255);
            buffer.extend_from_slice((!i).to_be_bytes().as_ref());
        }
    } else {
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
}

/// Read an unsigned int from a buffer
fn read_varint_unsigned<'a>(i: &mut u64, sort_order: SortOrder, buffer: &'a [u8]) -> &'a [u8] {
    let rem = &buffer[1..];
    if sort_order.is_desc() {
        match buffer[0] {
            // !253
            2 => {
                *i = !u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap()) as u64;
                &rem[2..]
            }
            // !254
            1 => {
                *i = !u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap()) as u64;
                &rem[4..]
            }
            // ! 255
            0 => {
                *i = !u64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
                &rem[8..]
            }
            b => {
                *i = !b as u64;
                rem
            }
        }
    } else {
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
}

/// Writes a signed int into a buffer with lexicographical sort attempting
/// to not use too much space
fn write_varint_signed(mut i: i64, sort_order: SortOrder, buffer: &mut Vec<u8>) {
    // To maintain the lexicographical sorting we'll use the first byte to encode the size and sign
    // of the integer.
    // 0 for -i64, 1 for -u32, 2 for -u16, 3 for -u8
    // 255 for i64, 254 for u32, 253 for u16, 252 for u8
    // As we're using the discriminator to store the sign we'll use unsigned encoding to
    // squeeze a tiny bit more space out without having to resort to bit shifting etc
    // That leaves space for 248 small values, positives will be more likely so we'll
    // make 4 = -100, which means 251 = 148 with a "displacement" of 103

    // To support the desc sort logically we can just store the negative of the value, however
    // there's an edge case due to 2's complement supporting bigger negative numbers than positive..
    if sort_order.is_desc() && i == i64::MIN {
        buffer.push(255);
        // doesn't match the rest of the encoding but as long as we special case it on the
        // decode we should be fine
        buffer.extend_from_slice(u64::MAX.to_be_bytes().as_ref());
        return;
    } else if sort_order.is_desc() {
        i = -i;
    }

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
            buffer.push(!(-i as u8));
        } else if i >= -(u16::MAX as i64) {
            buffer.push(2);
            buffer.extend_from_slice((!(-i as u16)).to_be_bytes().as_ref());
        } else if i >= -(u32::MAX as i64) {
            buffer.push(1);
            buffer.extend_from_slice((!(-i as u32)).to_be_bytes().as_ref());
        } else {
            buffer.push(0);
            buffer.extend_from_slice(i.to_be_bytes().as_ref());
        }
    }
}

/// Read an signed int from a buffer
fn read_varint_signed<'a>(i: &mut i64, sort_order: SortOrder, buffer: &'a [u8]) -> &'a [u8] {
    let mut rem = &buffer[1..];
    rem = match buffer[0] {
        0 => {
            *i = i64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
            &rem[8..]
        }
        1 => {
            *i = -(!(u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap())) as i64);
            &rem[4..]
        }
        2 => {
            *i = -(!(u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap())) as i64);
            &rem[2..]
        }
        3 => {
            *i = -(!rem[0] as i64);
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
            let u = u64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
            if sort_order.is_desc() && u == u64::MAX {
                *i = i64::MIN;
                return &rem[8..];
            }
            *i = u as i64;
            &rem[8..]
        }
        b => {
            *i = b as i64 - 103;
            rem
        }
    };
    if sort_order.is_desc() {
        *i = -(*i);
    }
    rem
}

/// Writes the slice given in a manner that can be sorted, even across slices of different lengths
fn write_sortable_bytes(mut bytes: &[u8], sort_order: SortOrder, buffer: &mut Vec<u8>) {
    // While null terminated strings might work, dealing with embedded nulls etc will be tricky, not
    // to mention we'd have the reverse issue with 0xFF if sorted desc.
    // The myrocks solution is to break the bytes into chunks of 8, write out each as a fixed sized
    // chunk with an addition byte signalling to used size and if there's another packet to follow.
    // we can sort desc by just flipping all the bits.
    // we'll use 1->8 as bytes used and 9 as "more data"
    if sort_order.is_asc() {
        while bytes.len() > 8 {
            buffer.extend_from_slice(&bytes[..8]);
            buffer.push(9);
            bytes = &bytes[8..];
        }
        // Buffer should be 8 or less bytes in length now
        buffer.extend_from_slice(bytes);
        for _ in 0..(8 - bytes.len()) {
            buffer.push(0);
        }
        buffer.push(bytes.len() as u8);
    } else {
        // Big chunks
        while bytes.len() > 8 {
            let inverted = !u64::from_le_bytes(bytes[..8].as_ref().try_into().unwrap());
            buffer.extend_from_slice(&inverted.to_le_bytes());
            buffer.push(!9);
            bytes = &bytes[8..];
        }
        // Remaining
        for b in bytes {
            buffer.push(!(*b));
        }
        // Padding
        for _ in 0..(8 - bytes.len()) {
            buffer.push(!0);
        }
        // Last size
        buffer.push(!(bytes.len() as u8));
    }
}

/// Reads in bytes written by write_sortable_bytes
fn read_sortable_bytes<'a>(
    bytes: &mut Vec<u8>,
    sort_order: SortOrder,
    buffer: &'a [u8],
) -> &'a [u8] {
    let mut rem = buffer;
    bytes.clear();
    if sort_order.is_asc() {
        loop {
            let t = rem[8];
            let chunk_len = min(t, 8);
            bytes.extend_from_slice(&rem[..(chunk_len as usize)]);
            rem = &rem[9..];
            if t != 9 {
                break;
            }
        }
    } else {
        loop {
            let t = !rem[8];
            let chunk_len = min(t, 8);
            if chunk_len >= 8 {
                let double_inverted = !u64::from_le_bytes(rem[..8].as_ref().try_into().unwrap());
                bytes.extend_from_slice(&double_inverted.to_le_bytes());
            } else {
                for b in &rem[..(chunk_len as usize)] {
                    bytes.push(!(*b));
                }
            }
            rem = &rem[9..];
            if t != 9 {
                break;
            }
        }
    }

    rem
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_unsigned() {
        let mut numbers = [
            0_u64,
            123,
            u8::MAX.into(),
            u16::MAX.into(),
            u32::MAX.into(),
            u64::MAX,
        ];
        let mut asc_byte_arrays = vec![];
        let mut desc_byte_arrays = vec![];

        // Encode into separate buffers
        for i in &numbers {
            let mut buf = vec![];
            i.write_sortable_bytes(SortOrder::Asc, &mut buf);
            asc_byte_arrays.push(buf);

            let mut buf = vec![];
            i.write_sortable_bytes(SortOrder::Desc, &mut buf);
            desc_byte_arrays.push(buf);
        }

        // Sort the buffers and the numbers;
        asc_byte_arrays.sort();
        desc_byte_arrays.sort();
        desc_byte_arrays.reverse();
        numbers.sort();

        assert_eq!(asc_byte_arrays.len(), numbers.len());

        // Decode and make sure we're still in numeric order
        for ((expected, asc_buf), desc_buf) in
            numbers.iter().zip(asc_byte_arrays).zip(desc_byte_arrays)
        {
            let mut actual = 0_u64;
            let rem = actual.read_sortable_bytes(SortOrder::Asc, &asc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());

            let rem = actual.read_sortable_bytes(SortOrder::Desc, &desc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());
        }
    }

    #[test]
    fn test_varint_signed() {
        let mut numbers = [
            0_i64,
            i8::MIN.into(),
            i8::MAX.into(),
            u8::MAX.into(),
            i16::MIN.into(),
            i16::MAX.into(),
            u16::MAX.into(),
            i32::MIN.into(),
            i32::MAX.into(),
            u32::MAX.into(),
            i64::MIN,
            i64::MAX,
        ];
        let mut asc_byte_arrays = vec![];
        let mut desc_byte_arrays = vec![];

        // Encode into separate buffers
        for i in &numbers {
            let mut buf = vec![];
            i.write_sortable_bytes(SortOrder::Asc, &mut buf);
            asc_byte_arrays.push(buf);

            let mut buf = vec![];
            i.write_sortable_bytes(SortOrder::Desc, &mut buf);
            desc_byte_arrays.push(buf);
        }

        // Sort the buffers and the numbers;
        asc_byte_arrays.sort();
        desc_byte_arrays.sort();
        desc_byte_arrays.reverse();
        numbers.sort();

        assert_eq!(asc_byte_arrays.len(), numbers.len());

        // Decode and make sure we're still in numeric order
        for ((expected, asc_buf), desc_buf) in
            numbers.iter().zip(asc_byte_arrays).zip(desc_byte_arrays)
        {
            let mut actual = 0_i64;
            let rem = actual.read_sortable_bytes(SortOrder::Asc, &asc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());

            let rem = actual.read_sortable_bytes(SortOrder::Desc, &desc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());
        }
    }

    #[test]
    fn test_bytes() {
        let mut strings = [
            "a".as_bytes(),
            "z".as_bytes(),
            "ba".as_bytes(),
            "yz".as_bytes(),
            "abcdefgh".as_bytes(),
            "abcdefgaa".as_bytes(),
            "aaaaaaaaaaaaaaaaaa".as_bytes(),
            "aaaaaaaaaaaaaaaaz".as_bytes(),
        ];
        let mut asc_byte_arrays = vec![];
        let mut desc_byte_arrays = vec![];

        // Encode into separate buffers
        for s in &strings {
            let mut buf = vec![];
            s.write_sortable_bytes(SortOrder::Asc, &mut buf);
            asc_byte_arrays.push(buf);

            let mut buf = vec![];
            s.write_sortable_bytes(SortOrder::Desc, &mut buf);
            desc_byte_arrays.push(buf);
        }

        // Sort the buffers and the strings;
        asc_byte_arrays.sort();
        desc_byte_arrays.sort();
        desc_byte_arrays.reverse();
        strings.sort();

        assert_eq!(asc_byte_arrays.len(), strings.len());

        // Decode and make sure we're still in lex order
        for ((expected, asc_buf), desc_buf) in
            strings.iter().zip(asc_byte_arrays).zip(desc_byte_arrays)
        {
            let mut actual: Vec<u8> = vec![];
            let rem = actual.read_sortable_bytes(SortOrder::Asc, &asc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());

            let rem = actual.read_sortable_bytes(SortOrder::Desc, &desc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());
        }
    }
}
