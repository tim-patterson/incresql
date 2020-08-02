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
}
