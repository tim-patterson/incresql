use crate::mysql::constants::*;
use crate::mysql::protocol_base::*;
use data::{DataType, Datum};
use std::collections::HashMap;
use std::fmt::Debug;

pub trait ClientPacket
where
    Self: Sized,
{
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error>;
}

const SERVER_SUPPORTED_CAPABILITIES: u32 = CAPABILITY_CLIENT_LONG_PASSWORD
    | CAPABILITY_CLIENT_FOUND_ROWS
    | CAPABILITY_CLIENT_LONG_FLAG
    | CAPABILITY_CLIENT_CONNECT_WITH_DB
    | CAPABILITY_CLIENT_NO_SCHEMA
    | CAPABILITY_CLIENT_PROTOCOL_41
    | CAPABILITY_CLIENT_SECURE_CONNECTION
    | CAPABILITY_CLIENT_CONNECT_ATTRS
    | CAPABILITY_CLIENT_PLUGIN_AUTH
    | CAPABILITY_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
    | CAPABILITY_CLIENT_DEPRECATE_EOF;

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_connection_phase_packets_protocol_handshake_v10.html
pub fn write_handshake_packet(connection_id: u32, buffer: &mut Vec<u8>) {
    let protocol_version = 10;
    let server_version = "8.0.0-incresql";
    let auth_plugin_data_part_1 = [1, 2, 3, 4, 5, 6, 7, 0];
    let filler = 0;
    let character_set = CHARSET_UTF8_GENERAL_CI;
    let status_flags = 0;
    let auth_plugin_data_len = 20;
    let reserved = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let auth_plugin_data_part_2 = [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 1, 0];
    let auth_plugin_name = "mysql_native_password";

    write_int_1(protocol_version, buffer);
    write_null_string(server_version, buffer);
    write_int_4(connection_id, buffer);
    buffer.extend_from_slice(&auth_plugin_data_part_1);
    write_int_1(filler, buffer);
    write_int_2(SERVER_SUPPORTED_CAPABILITIES as u16, buffer);
    write_int_1(character_set, buffer);
    write_int_2(status_flags, buffer);
    write_int_2((SERVER_SUPPORTED_CAPABILITIES >> 16) as u16, buffer);
    if (SERVER_SUPPORTED_CAPABILITIES & CAPABILITY_CLIENT_PLUGIN_AUTH) != 0 {
        write_int_1(auth_plugin_data_len, buffer);
    } else {
        write_int_1(0, buffer);
    }
    buffer.extend_from_slice(&reserved);
    buffer.extend_from_slice(&auth_plugin_data_part_2);
    if (SERVER_SUPPORTED_CAPABILITIES & CAPABILITY_CLIENT_PLUGIN_AUTH) != 0 {
        write_null_string(auth_plugin_name, buffer);
    }
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_connection_phase_packets_protocol_handshake_response.html
#[derive(Default, Debug, PartialEq)]
pub struct HandshakeResponsePacket {
    pub client_flags: u32,
    pub max_packet_size: u32,
    pub character_set: u8,
    pub username: String,
    pub auth_response: Vec<u8>,
    pub database: String,
    pub client_plugin_name: String,
    pub client_connection_attrs: HashMap<String, String>,
}

impl ClientPacket for HandshakeResponsePacket {
    fn read(mut buffer: &[u8]) -> Result<Self, std::io::Error> {
        let mut packet = Self::default();
        let mut lower_capibilities = 0;
        buffer = read_int_2(&mut lower_capibilities, buffer);
        if (lower_capibilities as u32 & CAPABILITY_CLIENT_PROTOCOL_41) != 0 {
            let mut upper_capibilities = 0;
            buffer = read_int_2(&mut upper_capibilities, buffer);
            packet.client_flags = (lower_capibilities as u32 + ((upper_capibilities as u32) << 16))
                & SERVER_SUPPORTED_CAPABILITIES;
            buffer = read_int_4(&mut packet.max_packet_size, buffer);
            buffer = read_int_1(&mut packet.character_set, buffer);
            buffer = &buffer[23..]; // filler
            buffer = read_null_string(&mut packet.username, buffer);
            if (packet.client_flags & CAPABILITY_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
                buffer = read_enc_bytestring(&mut packet.auth_response, buffer);
            } else {
                let mut len = 0_u8;
                buffer = read_int_1(&mut len, buffer);
                buffer =
                    read_fixed_length_bytestring(&mut packet.auth_response, len as usize, buffer);
            }

            if (packet.client_flags & CAPABILITY_CLIENT_CONNECT_WITH_DB) != 0 {
                buffer = read_null_string(&mut packet.database, buffer);
            }

            if !buffer.is_empty() && (packet.client_flags & CAPABILITY_CLIENT_PLUGIN_AUTH) != 0 {
                buffer = read_null_string(&mut packet.client_plugin_name, buffer);
            }

            if !buffer.is_empty() && (packet.client_flags & CAPABILITY_CLIENT_CONNECT_ATTRS) != 0 {
                let mut len_kvs = 0_u64;
                buffer = read_enc_int(&mut len_kvs, buffer);

                while !buffer.is_empty() {
                    let mut key = String::new();
                    let mut value = String::new();
                    buffer = read_enc_string(&mut key, buffer);
                    buffer = read_enc_string(&mut value, buffer);
                    packet.client_connection_attrs.insert(key, value);
                }
            }
        } else {
            packet.client_flags = lower_capibilities as u32 & SERVER_SUPPORTED_CAPABILITIES;
            buffer = read_int_3(&mut packet.max_packet_size, buffer);
            buffer = read_null_string(&mut packet.username, buffer);

            if (packet.client_flags & CAPABILITY_CLIENT_CONNECT_WITH_DB) != 0 {
                buffer = read_null_bytestring(&mut packet.auth_response, buffer);
                buffer = read_null_string(&mut packet.database, buffer);
            } else {
                buffer = read_eof_bytestring(&mut packet.auth_response, buffer);
            }
        }
        assert!(
            buffer.is_empty(),
            format!("Left over packet bytes {:x?}", buffer)
        );
        Ok(packet)
    }
}

pub fn write_auth_switch_request_packet(buffer: &mut Vec<u8>) {
    let status_tag = 0xFE;
    let plugin_name = "mysql_native_password";
    let plugin_data = [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5];

    write_int_1(status_tag, buffer);
    write_null_string(plugin_name, buffer);
    write_null_string(&plugin_data, buffer);
}

#[derive(Default, Debug, PartialEq)]
pub struct AuthSwitchResponsePacket {
    pub data: Vec<u8>,
}

impl ClientPacket for AuthSwitchResponsePacket {
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error> {
        let mut packet = Self::default();
        read_eof_bytestring(&mut packet.data, buffer);
        Ok(packet)
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct ComInitDbPacket {
    pub schema: String,
}

impl ClientPacket for ComInitDbPacket {
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error> {
        let mut packet = Self::default();
        read_eof_string(&mut packet.schema, buffer);
        Ok(packet)
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct ComQueryPacket {
    pub query: String,
}

impl ClientPacket for ComQueryPacket {
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error> {
        let mut packet = Self::default();
        read_eof_string(&mut packet.query, buffer);
        Ok(packet)
    }
}

pub fn write_err_packet(
    error_code: u16,
    error_message: &str,
    sql_state: &str,
    capabilities: u32,
    buffer: &mut Vec<u8>,
) {
    let header = 0xFF;
    let sql_state_marker = b'#';

    write_int_1(header, buffer);
    write_int_2(error_code, buffer);
    if (capabilities & CAPABILITY_CLIENT_PROTOCOL_41) != 0 {
        write_int_1(sql_state_marker, buffer);
        buffer.extend_from_slice(sql_state.as_bytes());
    }
    write_eof_string(error_message, buffer);
}

pub fn write_err_packet_from_err(err: &MyError, capabilities: u32, buffer: &mut Vec<u8>) {
    write_err_packet(err.code, err.msg, err.sql_state, capabilities, buffer)
}

pub fn write_tuple_packet(tuple: &[Datum], types: &[DataType], buffer: &mut Vec<u8>) {
    for (idx, value) in tuple.iter().enumerate() {
        match value {
            Datum::Null => buffer.push(0xFB),
            Datum::Boolean(b) => write_enc_string(if *b { "1" } else { "0" }, buffer),
            // TODO We could keep a buffer and write into that, then calc the length and copy across
            // to avoid format allocating strings...
            _ => write_enc_string(format!("{}", value.typed_with(types[idx])), buffer),
        }
    }
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_ok_packet.html
pub fn write_ok_packet(eof: bool, affected_rows: u64, capabilities: u32, buffer: &mut Vec<u8>) {
    let header = if eof { 0xFE } else { 0 };
    let last_insert_id = 0;
    let status_flags = STATUS_FLAG_AUTOCOMMIT;
    let warnings = 0;
    let info = "";

    write_int_1(header, buffer);
    write_enc_int(affected_rows, buffer);
    write_enc_int(last_insert_id, buffer);
    if (capabilities & CAPABILITY_CLIENT_PROTOCOL_41) != 0 {
        write_int_2(status_flags, buffer);
        write_int_2(warnings, buffer);
    } else if (capabilities & CAPABILITY_CLIENT_TRANSACTIONS) != 0 {
        write_int_2(status_flags, buffer);
    }

    if (capabilities & CAPABILITY_CLIENT_SESSION_TRACK) != 0 {
        write_null_string(info, buffer);
    } else {
        write_eof_string(info, buffer);
    }
}

pub fn write_eof_packet(capabilities: u32, buffer: &mut Vec<u8>) {
    let header = 0xFE;
    let warnings = 0;
    let status_flags = STATUS_FLAG_AUTOCOMMIT; // Autocommit

    write_int_1(header, buffer);
    if (capabilities & CAPABILITY_CLIENT_PROTOCOL_41) != 0 {
        write_int_2(warnings, buffer);
        write_int_2(status_flags, buffer);
    }
}

pub fn write_resultset_packet(column_count: usize, capabilities: u32, buffer: &mut Vec<u8>) {
    let metadata_follows = 1;
    if (capabilities & CAPABILITY_CLIENT_MULTI_RESULTS) != 0 {
        write_int_1(metadata_follows, buffer);
    }
    write_enc_int(column_count as u64, buffer);
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_com_query_response_text_resultset_column_definition.html
pub fn write_column_packet(
    table: &str,
    name: &str,
    data_type: DataType,
    capabilities: u32,
    buffer: &mut Vec<u8>,
) {
    // 32768 is to be set for number fields, TIMESTAMP_FLAG   1024
    let flags = 0_u16;
    let character_set = CHARSET_UTF8_GENERAL_CI;

    // Just used for client formatting
    // 0x00 for integers and static strings
    // 0x1f for dynamic strings, double, float
    // 0x00 to 0x51 for decimals
    let mut decimals = 0;
    // Used for client formatting, ie varchar(1024)
    let mut column_length = 1024;

    let column_type = match data_type {
        DataType::Null => MYSQL_TYPE_NULL,
        DataType::Text => {
            decimals = 0x1f;
            MYSQL_TYPE_VAR_STRING
        }
        DataType::Integer => MYSQL_TYPE_LONG,
        DataType::Date => MYSQL_TYPE_DATE,
        DataType::BigInt => MYSQL_TYPE_LONGLONG,
        DataType::Boolean => MYSQL_TYPE_TINY,
        DataType::Decimal(precision, scale) => {
            column_length = precision as u32;
            // Assuming this is meant to be the scale...
            decimals = scale;
            MYSQL_TYPE_NEWDECIMAL
        }
        DataType::ByteA => MYSQL_TYPE_BLOB,
        DataType::Json => MYSQL_TYPE_VAR_STRING,
    };

    if (capabilities & CAPABILITY_CLIENT_PROTOCOL_41) != 0 {
        write_enc_string("def", buffer);
        write_enc_string("", buffer);
        write_enc_string(table, buffer);
        write_enc_string("", buffer);
        write_enc_string(name, buffer);
        write_enc_string("", buffer);
        write_enc_int(0x0C, buffer);
        write_int_2(character_set as u16, buffer);
        write_int_4(column_length, buffer);
        write_int_1(column_type, buffer);
        write_int_2(flags, buffer);
        write_int_1(decimals, buffer);
        // These 2 types don't seem to be mentioned in the spec but yet seem to be required to
        // make up the 0x0c length as per the spec
        write_int_2(0, buffer);
    } else {
        write_enc_string(table, buffer);
        write_enc_string(name, buffer);
        write_int_1(1_u8, buffer);
        write_int_1(column_type, buffer);

        if (capabilities & CAPABILITY_CLIENT_LONG_FLAG) != 0 {
            write_enc_int(3, buffer);
            write_int_2(flags, buffer);
            write_int_1(decimals, buffer);
        } else {
            write_enc_int(2, buffer);
            // This doesn't agree with spec but I suspect spec has a typo in it
            write_int_1(flags as u8, buffer);
            write_int_1(decimals, buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_handshake_packet() {
        let mut buf = vec![];
        write_handshake_packet(1, &mut buf);
        assert_eq!(
            buf.as_slice(),
            &[
                10_u8, 56, 46, 48, 46, 48, 45, 105, 110, 99, 114, 101, 115, 113, 108, 0, 1, 0, 0,
                0, 1, 2, 3, 4, 5, 6, 7, 0, 0, 31, 130, 33, 0, 0, 56, 1, 20, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 1, 0, 109, 121, 115, 113, 108, 95, 110, 97,
                116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0
            ] as &[u8]
        );
    }

    #[test]
    fn test_handshake_response_packet() -> Result<(), Box<dyn Error>> {
        // Sample packets from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_connection_phase_packets_protocol_handshake_response.html
        // First 4 bytes removed as they're part of the framing protocol
        let raw_packet = [
            0x85, 0x24, 0x00, 0x00, 0x00, 0x6f, 0x6c, 0x64, 0x00, 0x47, 0x44, 0x53, 0x43, 0x51,
            0x59, 0x52, 0x5f,
        ];
        let packet = HandshakeResponsePacket::read(raw_packet.as_ref())?;
        assert_eq!(packet.username, "old");

        let raw_packet = [
            0x8d, 0xa6, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x70, 0x61, 0x6d, 0x00, 0x14, 0xab, 0x09, 0xee, 0xf6, 0xbc,
            0xb1, 0x32, 0x3e, 0x61, 0x14, 0x38, 0x65, 0xc0, 0x99, 0x1d, 0x95, 0x7d, 0x75, 0xd4,
            0x47, 0x74, 0x65, 0x73, 0x74, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61,
            0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00,
        ];
        let packet = HandshakeResponsePacket::read(raw_packet.as_ref())?;
        assert_eq!(packet.username, "pam");
        assert_eq!(packet.database, "test");
        assert_eq!(packet.client_plugin_name, "mysql_native_password");

        let raw_packet = [
            0x85, 0xa2, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x14, 0x22, 0x50, 0x79, 0xa2,
            0x12, 0xd4, 0xe8, 0x82, 0xe5, 0xb3, 0xf4, 0x1a, 0x97, 0x75, 0x6b, 0xc8, 0xbe, 0xdb,
            0x9f, 0x80, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65,
            0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00, 0x61, 0x03, 0x5f, 0x6f,
            0x73, 0x09, 0x64, 0x65, 0x62, 0x69, 0x61, 0x6e, 0x36, 0x2e, 0x30, 0x0c, 0x5f, 0x63,
            0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x08, 0x6c, 0x69, 0x62,
            0x6d, 0x79, 0x73, 0x71, 0x6c, 0x04, 0x5f, 0x70, 0x69, 0x64, 0x05, 0x32, 0x32, 0x33,
            0x34, 0x34, 0x0f, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72,
            0x73, 0x69, 0x6f, 0x6e, 0x08, 0x35, 0x2e, 0x36, 0x2e, 0x36, 0x2d, 0x6d, 0x39, 0x09,
            0x5f, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x06, 0x78, 0x38, 0x36, 0x5f,
            0x36, 0x34, 0x03, 0x66, 0x6f, 0x6f, 0x03, 0x62, 0x61, 0x72,
        ];
        let packet = HandshakeResponsePacket::read(raw_packet.as_ref())?;
        assert_eq!(packet.username, "root");
        assert_eq!(packet.database, "");
        assert_eq!(packet.client_plugin_name, "mysql_native_password");
        assert_eq!(packet.client_connection_attrs["foo"], "bar");
        assert_eq!(packet.client_connection_attrs["_os"], "debian6.0");
        assert_eq!(packet.client_connection_attrs["_client_name"], "libmysql");
        assert_eq!(packet.client_connection_attrs["_pid"], "22344");
        assert_eq!(
            packet.client_connection_attrs["_client_version"],
            "5.6.6-m9"
        );
        assert_eq!(packet.client_connection_attrs["_platform"], "x86_64");
        Ok(())
    }

    #[test]
    fn test_auth_switch_request_packet() {
        let mut buf = vec![];
        write_auth_switch_request_packet(&mut buf);
        assert_eq!(
            buf.as_slice(),
            &[
                254_u8, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115,
                115, 119, 111, 114, 100, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5,
                5, 5, 0
            ] as &[u8]
        );
    }

    #[test]
    fn test_auth_switch_response_packet() -> Result<(), Box<dyn Error>> {
        let raw_packet = "abcdef".as_bytes();
        let packet = AuthSwitchResponsePacket::read(raw_packet)?;
        assert_eq!(packet.data, "abcdef".as_bytes());
        Ok(())
    }

    #[test]
    fn test_com_init_db_packet() -> Result<(), Box<dyn Error>> {
        let raw_packet = "abcdef".as_bytes();
        let packet = ComInitDbPacket::read(raw_packet)?;
        assert_eq!(packet.schema, "abcdef");
        Ok(())
    }

    #[test]
    fn test_com_query_packet() -> Result<(), Box<dyn Error>> {
        let raw_packet = "Select foo from bar".as_bytes();
        let packet = ComQueryPacket::read(raw_packet)?;
        assert_eq!(packet.query, "Select foo from bar");
        Ok(())
    }

    #[test]
    fn test_err_packet() {
        let mut buf = vec![];
        write_err_packet(
            1096,
            "No tables used",
            "HY000",
            SERVER_SUPPORTED_CAPABILITIES,
            &mut buf,
        );
        // Expected response from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_err_packet.html
        assert_eq!(
            buf.as_slice(),
            [
                0xff, 0x48, 0x04, 0x23, 0x48, 0x59, 0x30, 0x30, 0x30, 0x4e, 0x6f, 0x20, 0x74, 0x61,
                0x62, 0x6c, 0x65, 0x73, 0x20, 0x75, 0x73, 0x65, 0x64
            ]
            .as_ref()
        );
    }

    #[test]
    fn test_err_packet_from_err() {
        let mut buf = vec![];
        write_err_packet_from_err(
            &MYSQL_ER_NO_TABLES_USED,
            SERVER_SUPPORTED_CAPABILITIES,
            &mut buf,
        );
        // Expected response from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_err_packet.html
        assert_eq!(
            buf.as_slice(),
            [
                0xff, 0x48, 0x04, 0x23, 0x48, 0x59, 0x30, 0x30, 0x30, 0x4e, 0x6f, 0x20, 0x74, 0x61,
                0x62, 0x6c, 0x65, 0x73, 0x20, 0x75, 0x73, 0x65, 0x64
            ]
            .as_ref()
        );
    }

    #[test]
    fn test_ok_packet() {
        let mut buf = vec![];
        write_ok_packet(false, 0, SERVER_SUPPORTED_CAPABILITIES, &mut buf);
        // Expected response from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_ok_packet.html
        assert_eq!(
            buf.as_slice(),
            [0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00].as_ref()
        );
    }

    #[test]
    fn test_eof_packet() {
        let mut buf = vec![];
        write_eof_packet(SERVER_SUPPORTED_CAPABILITIES, &mut buf);
        // Expected response from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_eof_packet.html
        assert_eq!(buf.as_slice(), [0xfe, 0x00, 0x00, 0x02, 0x00].as_ref());
    }

    #[test]
    fn test_column_packet() {
        let mut buf = vec![];
        write_column_packet(
            "foo",
            "bar",
            DataType::Integer,
            SERVER_SUPPORTED_CAPABILITIES,
            &mut buf,
        );
        assert_eq!(
            buf.as_slice(),
            [
                3, 100, 101, 102, 0, 3, 102, 111, 111, 0, 3, 98, 97, 114, 0, 12, 33, 0, 0, 4, 0, 0,
                3, 0, 0, 0, 0, 0
            ]
            .as_ref()
        );
    }

    #[test]
    fn test_resultset_packet_packet() {
        let mut buf = vec![];
        write_resultset_packet(4, SERVER_SUPPORTED_CAPABILITIES, &mut buf);
        assert_eq!(buf.as_slice(), [4].as_ref());
    }
}
