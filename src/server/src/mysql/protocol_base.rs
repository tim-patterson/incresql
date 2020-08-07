use std::convert::TryInto;

pub fn write_int_1(i: u8, buffer: &mut Vec<u8>) {
    buffer.push(i);
}

pub fn read_int_1<'a>(i: &mut u8, buffer: &'a [u8]) -> &'a [u8] {
    *i = buffer[0];
    &buffer[1..]
}

pub fn write_int_2(i: u16, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&i.to_le_bytes());
}

pub fn read_int_2<'a>(i: &mut u16, buffer: &'a [u8]) -> &'a [u8] {
    *i = u16::from_le_bytes(buffer[..2].as_ref().try_into().unwrap());
    &buffer[2..]
}

pub fn write_int_3(i: u32, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&i.to_le_bytes()[..3]);
}

pub fn read_int_3<'a>(i: &mut u32, buffer: &'a [u8]) -> &'a [u8] {
    let mut buf = [0_u8; 4];
    buf[0] = buffer[0];
    buf[1] = buffer[1];
    buf[2] = buffer[2];
    *i = u32::from_le_bytes(buf);
    &buffer[3..]
}

pub fn write_int_4(i: u32, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&i.to_le_bytes());
}

pub fn read_int_4<'a>(i: &mut u32, buffer: &'a [u8]) -> &'a [u8] {
    *i = u32::from_le_bytes(buffer[..4].as_ref().try_into().unwrap());
    &buffer[4..]
}

pub fn write_enc_int(i: u64, buffer: &mut Vec<u8>) {
    if i < 251 {
        buffer.push(i as u8);
    } else if i >= 251 && i <= 0xFFFF {
        buffer.push(0xFC);
        write_int_2(i as u16, buffer);
    } else if i >= 251 && i <= 0xFFFFFF {
        buffer.push(0xFD);
        write_int_3(i as u32, buffer);
    } else {
        buffer.push(0xFE);
        buffer.extend_from_slice(&i.to_le_bytes());
    }
}

pub fn read_enc_int<'a>(i: &mut u64, buffer: &'a [u8]) -> &'a [u8] {
    match buffer[0] {
        0xfc => {
            let mut i2 = 0;
            let rem = read_int_2(&mut i2, &buffer[1..]);
            *i = i2 as u64;
            rem
        }
        0xfd => {
            // 3 byte
            let mut i2 = 0;
            let rem = read_int_3(&mut i2, &buffer[1..]);
            *i = i2 as u64;
            rem
        }
        0xfe => {
            // 8 byte
            *i = u64::from_le_bytes(buffer[1..9].as_ref().try_into().unwrap());
            &buffer[9..]
        }
        b => {
            *i = b as u64;
            &buffer[1..]
        }
    }
}

pub fn write_null_string<S: AsRef<[u8]>>(s: S, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(s.as_ref());
    buffer.push(0);
}

pub fn read_null_string<'a>(s: &mut String, buffer: &'a [u8]) -> &'a [u8] {
    read_null_bytestring(unsafe { s.as_mut_vec() }, buffer)
}

pub fn read_null_bytestring<'a>(s: &mut Vec<u8>, buffer: &'a [u8]) -> &'a [u8] {
    let mut len = 0_usize;
    for b in buffer {
        if *b == 0 {
            break;
        }
        len += 1;
    }
    s.clear();
    s.extend_from_slice(&buffer[..len]);

    &buffer[(len + 1)..]
}

pub fn write_eof_string<S: AsRef<[u8]>>(s: S, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(s.as_ref());
}

pub fn read_eof_string<'a>(s: &mut String, buffer: &'a [u8]) -> &'a [u8] {
    let vec = unsafe { s.as_mut_vec() };
    vec.clear();
    vec.extend_from_slice(buffer);
    &[]
}

pub fn read_eof_bytestring<'a>(s: &mut Vec<u8>, buffer: &'a [u8]) -> &'a [u8] {
    s.clear();
    s.extend_from_slice(buffer);
    &[]
}

pub fn write_enc_string<S: AsRef<[u8]>>(s: S, buffer: &mut Vec<u8>) {
    write_enc_int(s.as_ref().len() as u64, buffer);
    write_eof_string(s, buffer);
}

pub fn read_enc_string<'a>(s: &mut String, buffer: &'a [u8]) -> &'a [u8] {
    let mut length = 0;
    let rem = read_enc_int(&mut length, buffer);
    read_fixed_length_string(s, length as usize, rem)
}

pub fn read_enc_bytestring<'a>(s: &mut Vec<u8>, buffer: &'a [u8]) -> &'a [u8] {
    let mut length = 0;
    let rem = read_enc_int(&mut length, buffer);
    s.clear();
    s.extend_from_slice(&rem[..length as usize]);
    &rem[(length as usize)..]
}

pub fn read_fixed_length_string<'a>(s: &mut String, length: usize, buffer: &'a [u8]) -> &'a [u8] {
    read_fixed_length_bytestring(unsafe { s.as_mut_vec() }, length, buffer)
}

pub fn read_fixed_length_bytestring<'a>(
    s: &mut Vec<u8>,
    length: usize,
    buffer: &'a [u8],
) -> &'a [u8] {
    s.clear();
    s.extend_from_slice(&buffer[..length]);
    &buffer[length..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_1() {
        let mut buf = vec![];
        write_int_1(234, &mut buf);
        let mut i = 0;
        let rem = read_int_1(&mut i, &buf);
        assert_eq!(i, 234);
        assert!(rem.is_empty());
    }

    #[test]
    fn test_int_2() {
        let mut buf = vec![];
        write_int_2(9123, &mut buf);
        let mut i = 0;
        let rem = read_int_2(&mut i, &buf);
        assert_eq!(i, 9123);
        assert!(rem.is_empty());
    }

    #[test]
    fn test_int_3() {
        let mut buf = vec![];
        write_int_3(7832432, &mut buf);
        let mut i = 0;
        let rem = read_int_3(&mut i, &buf);
        assert_eq!(i, 7832432);
        assert!(rem.is_empty());
    }

    #[test]
    fn test_int_4() {
        let mut buf = vec![];
        write_int_4(3123456789, &mut buf);
        let mut i = 0;
        let rem = read_int_4(&mut i, &buf);
        assert_eq!(i, 3123456789);
        assert!(rem.is_empty());
    }

    #[test]
    fn test_enc_int() {
        let mut buf = vec![];
        write_enc_int(67, &mut buf);
        write_enc_int(9123, &mut buf);
        write_enc_int(7832432, &mut buf);
        write_enc_int(8920398049823, &mut buf);
        let (mut a, mut b, mut c, mut d) = (0, 0, 0, 0);

        let rem = read_enc_int(&mut a, &buf);
        let rem = read_enc_int(&mut b, rem);
        let rem = read_enc_int(&mut c, rem);
        let rem = read_enc_int(&mut d, rem);
        assert_eq!(a, 67);
        assert_eq!(b, 9123);
        assert_eq!(c, 7832432);
        assert_eq!(d, 8920398049823);
        assert!(rem.is_empty());
    }

    #[test]
    fn test_null_string() {
        let mut buf = vec![];
        write_null_string("hello", &mut buf);
        write_null_string("world".as_bytes(), &mut buf);
        let (mut h, mut w) = (String::new(), Vec::new());
        let mut rem = read_null_string(&mut h, &buf);
        rem = read_null_bytestring(&mut w, rem);
        assert_eq!(h, "hello");
        assert_eq!(w, "world".as_bytes());
        assert!(rem.is_empty())
    }

    #[test]
    fn test_eof_string() {
        let mut buf = vec![];
        write_eof_string("hello", &mut buf);
        let mut h = String::new();
        let rem = read_eof_string(&mut h, &buf);
        assert_eq!(h, "hello");
        assert_eq!(buf.len(), "hello".len());
        assert!(rem.is_empty());

        let mut h2 = Vec::new();
        let rem = read_eof_bytestring(&mut h2, &buf);
        assert_eq!(h2, "hello".as_bytes());
        assert!(rem.is_empty())
    }

    #[test]
    fn test_enc_string() {
        let mut buf = vec![];
        write_enc_string("hello", &mut buf);
        write_enc_string("world".as_bytes(), &mut buf);
        let (mut h, mut w) = (String::new(), Vec::new());
        let mut rem = read_enc_string(&mut h, &buf);
        rem = read_enc_bytestring(&mut w, rem);
        assert_eq!(h, "hello");
        assert_eq!(w, "world".as_bytes());
        assert!(rem.is_empty())
    }

    #[test]
    fn test_fixed_length_string() {
        let buf = "helloworld".as_bytes();
        let (mut h, mut w) = (String::new(), Vec::new());
        let mut rem = read_fixed_length_string(&mut h, 5, &buf);
        rem = read_fixed_length_bytestring(&mut w, 5, rem);
        assert_eq!(h, "hello");
        assert_eq!(w, "world".as_bytes());
        assert!(rem.is_empty())
    }
}
