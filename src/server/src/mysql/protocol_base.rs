use std::convert::TryInto;

pub fn write_int_1(i: u8, buffer: &mut Vec<u8>) {
    buffer.push(i);
}

pub fn read_int_1(buffer: &[u8]) -> (u8, &[u8]) {
    (buffer[0], &buffer[1..])
}

pub fn write_int_2(i: u16, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&i.to_le_bytes());
}

pub fn read_int_2(buffer: &[u8]) -> (u16, &[u8]) {
    (
        u16::from_le_bytes(buffer[..2].as_ref().try_into().unwrap()),
        &buffer[2..],
    )
}

pub fn write_int_3(i: u32, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&i.to_le_bytes()[..3]);
}

pub fn read_int_3(buffer: &[u8]) -> (u32, &[u8]) {
    let mut buf = [0_u8; 4];
    buf[0] = buffer[0];
    buf[1] = buffer[1];
    buf[2] = buffer[2];

    (u32::from_le_bytes(buf), &buffer[3..])
}

pub fn write_int_4(i: u32, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&i.to_le_bytes());
}

pub fn read_int_4(buffer: &[u8]) -> (u32, &[u8]) {
    (
        u32::from_le_bytes(buffer[..4].as_ref().try_into().unwrap()),
        &buffer[4..],
    )
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

pub fn read_enc_int(buffer: &[u8]) -> (u64, &[u8]) {
    match buffer[0] {
        0xfc => {
            let (i, rem) = read_int_2(&buffer[1..]);
            (i as u64, rem)
        }
        0xfd => {
            // 3 byte
            let (i, rem) = read_int_3(&buffer[1..]);
            (i as u64, rem)
        }
        0xfe => {
            // 8 byte
            let i = u64::from_le_bytes(buffer[1..9].as_ref().try_into().unwrap());
            (i, &buffer[9..])
        }
        b => (b as u64, &buffer[1..]),
    }
}

pub fn write_null_string<S: AsRef<[u8]>>(s: S, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(s.as_ref());
    buffer.push(0);
}

pub fn read_null_string(buffer: &[u8]) -> (String, &[u8]) {
    let mut len = 0_usize;
    for b in buffer {
        if *b == 0 {
            break;
        }
        len += 1;
    }
    let mut vec = Vec::with_capacity(len);
    vec.extend_from_slice(&buffer[..len]);

    let s = unsafe { String::from_utf8_unchecked(vec) };

    (s, &buffer[(len + 1)..])
}

pub fn write_eof_string<S: AsRef<[u8]>>(s: S, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(s.as_ref());
}

pub fn read_eof_string(buffer: &[u8]) -> (String, &[u8]) {
    let s = unsafe { String::from_utf8_unchecked(buffer.to_vec()) };
    (s, [].as_ref())
}

pub fn write_enc_string<S: AsRef<[u8]>>(s: S, buffer: &mut Vec<u8>) {
    write_enc_int(s.as_ref().len() as u64, buffer);
    write_eof_string(s, buffer);
}

pub fn read_enc_string(buffer: &[u8]) -> (String, &[u8]) {
    let (length, rem) = read_enc_int(buffer);
    read_fixed_length_string(length as usize, rem)
}

pub fn read_fixed_length_string(length: usize, buffer: &[u8]) -> (String, &[u8]) {
    let mut vec = Vec::with_capacity(length);
    vec.extend_from_slice(&buffer[..length]);
    let s = unsafe { String::from_utf8_unchecked(vec) };
    (s, &buffer[length..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_1() {
        let mut buf = vec![];
        write_int_1(234, &mut buf);
        let (i, rem) = read_int_1(&buf);
        assert_eq!(i, 234);
        assert!(rem.is_empty())
    }

    #[test]
    fn test_int_2() {
        let mut buf = vec![];
        write_int_2(9123, &mut buf);
        let (i, rem) = read_int_2(&buf);
        assert_eq!(i, 9123);
        assert!(rem.is_empty())
    }

    #[test]
    fn test_int_3() {
        let mut buf = vec![];
        write_int_3(7832432, &mut buf);
        let (i, rem) = read_int_3(&buf);
        assert_eq!(i, 7832432);
        assert!(rem.is_empty())
    }

    #[test]
    fn test_int_4() {
        let mut buf = vec![];
        write_int_4(3123456789, &mut buf);
        let (i, rem) = read_int_4(&buf);
        assert_eq!(i, 3123456789);
        assert!(rem.is_empty())
    }

    #[test]
    fn test_enc_int() {
        let mut buf = vec![];
        write_enc_int(67, &mut buf);
        write_enc_int(9123, &mut buf);
        write_enc_int(7832432, &mut buf);
        write_enc_int(8920398049823, &mut buf);

        let (a, rem) = read_enc_int(&buf);
        let (b, rem) = read_enc_int(rem);
        let (c, rem) = read_enc_int(rem);
        let (d, _rem) = read_enc_int(rem);
        assert_eq!(a, 67);
        assert_eq!(b, 9123);
        assert_eq!(c, 7832432);
        assert_eq!(d, 8920398049823);
    }

    #[test]
    fn test_null_string() {
        let mut buf = vec![];
        write_null_string("hello", &mut buf);
        write_null_string("world".as_bytes(), &mut buf);

        let (h, rem) = read_null_string(&buf);
        let (w, rem) = read_null_string(rem);
        assert_eq!(h, "hello");
        assert_eq!(w, "world");
        assert!(rem.is_empty())
    }

    #[test]
    fn test_eof_string() {
        let mut buf = vec![];
        write_eof_string("hello", &mut buf);

        let (h, rem) = read_eof_string(&buf);
        assert_eq!(h, "hello");
        assert_eq!(buf.len(), "hello".len());
        assert!(rem.is_empty())
    }

    #[test]
    fn test_enc_string() {
        let mut buf = vec![];
        write_enc_string("hello", &mut buf);
        write_enc_string("world".as_bytes(), &mut buf);

        let (h, rem) = read_enc_string(&buf);
        let (w, rem) = read_enc_string(rem);
        assert_eq!(h, "hello");
        assert_eq!(w, "world");
        assert!(rem.is_empty())
    }
}
