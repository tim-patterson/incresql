use crate::mysql::protocol_base::*;
use data::DataType;
use std::collections::HashMap;
use std::fmt::Debug;

pub trait ServerPacket: Debug {
    fn write(&self, capabilities_lower: u16, capabilities_upper: u16, buffer: &mut Vec<u8>);
}

pub trait ClientPacket
where
    Self: Sized,
{
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error>;
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_connection_phase_packets_protocol_handshake_v10.html
#[derive(Debug)]
pub struct HandshakePacket {
    protocol_version: u8,             // int<1> - Always 10
    server_version: String,           // string<NUL> - human readable status information
    connection_id: u32,               // int<4>
    auth_plugin_data_part_1: [u8; 8], // string[8] - first 8 bytes of the plugin provided data (scramble)
    filler: u8, // int<1> - 0x00 byte, terminating the first part of a scramble
    //https://github.com/mysql/mysql-server/blob/8e797a5d6eb3a87f16498edcb7261a75897babae/router/src/mysql_protocol/include/mysqlrouter/mysql_protocol/constants.h#L105
    capability_flags_1: u16, // int<2> - The lower 2 bytes of the Capabilities Flags
    character_set: u8, // int<1> - default server a_protocol_character_set, only the lower 8-bits
    status_flags: u16, // int<2> - SERVER_STATUS_flags_enum
    capability_flags_2: u16, // int<2> - The upper 2 bytes of the Capabilities Flags
    auth_plugin_data_len: u8, // int<1> - length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
    reserved: [u8; 10],       // string[10] - reserved. All 0s.
    auth_plugin_data_part_2: Vec<u8>, // $length - Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
    auth_plugin_name: String, // NULL - name of the auth_method that the auth_plugin_data belongs to
}

impl HandshakePacket {
    pub fn new(connection_id: u32) -> Self {
        HandshakePacket {
            protocol_version: 10,
            server_version: "8.0.0-incresql".to_string(),
            connection_id,
            auth_plugin_data_part_1: [1, 2, 3, 4, 5, 6, 7, 0], // A Nonce
            filler: 0,
            // CLIENT_PROTOCOL_41, CLIENT_CONNECT_WITH_DB, CLIENT_INTERACTIVE, CLIENT_LONG_PASSWORD, CLIENT_ODBC, CLIENT_TRANSACTIONS,
            // CLIENT_SECURE_CONNECTION (needed for jdbc driver v8..)
            capability_flags_1: 512 + 8 + 1024 + 1 + 64 + 8192 + 32768,
            character_set: 33, // utf8_general_ci
            status_flags: 0,
            // CLIENT_CONNECT_ATTRS, CLIENT_DEPRECATE_EOF, CLIENT_MULTI_RESULTS, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA, CLIENT_PLUGIN_AUTH
            capability_flags_2: (1_u16 << 4)
                + (1_u16 << 8)
                + (1_u16 << 1)
                + (1_u16 << 5)
                + (1_u16 << 3),
            auth_plugin_data_len: 20,
            reserved: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            auth_plugin_data_part_2: vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 1, 0],
            auth_plugin_name: "mysql_native_password".to_string(),
        }
    }
}

impl ServerPacket for HandshakePacket {
    /// At this point we haven't established the capabilities so these values are ignored for
    /// this packet
    fn write(&self, _capabilities_lower: u16, _capabilities_upper: u16, buffer: &mut Vec<u8>) {
        write_int_1(self.protocol_version, buffer);
        write_null_string(&self.server_version, buffer);
        write_int_4(self.connection_id, buffer);
        buffer.extend_from_slice(&self.auth_plugin_data_part_1);
        write_int_1(self.filler, buffer);
        write_int_2(self.capability_flags_1, buffer);
        write_int_1(self.character_set, buffer);
        write_int_2(self.status_flags, buffer);
        write_int_2(self.capability_flags_2, buffer);
        write_int_1(self.auth_plugin_data_len, buffer);
        buffer.extend_from_slice(&self.reserved);
        buffer.extend_from_slice(&self.auth_plugin_data_part_2);
        write_null_string(&self.auth_plugin_name, buffer);
    }
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_connection_phase_packets_protocol_handshake_response.html
#[derive(Default, Debug, PartialEq)]
pub struct HandshakeResponsePacket {
    pub client_flag_lower: u16, // int<2>
    pub client_flag_upper: u16, // int<2>
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
        buffer = read_int_2(&mut packet.client_flag_lower, buffer);
        // If client protocol 41
        if (packet.client_flag_lower & 512) != 0 {
            buffer = read_int_2(&mut packet.client_flag_upper, buffer);
            buffer = read_int_4(&mut packet.max_packet_size, buffer);
            buffer = read_int_1(&mut packet.character_set, buffer);
            buffer = &buffer[23..]; // filler
            buffer = read_null_string(&mut packet.username, buffer);
            // CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA (1UL << 21), 21 - 16 = 5...
            if (packet.client_flag_upper & (1_u16 << 5)) != 0 {
                buffer = read_enc_bytestring(&mut packet.auth_response, buffer);
            } else {
                let mut len = 0_u8;
                buffer = read_int_1(&mut len, buffer);
                buffer =
                    read_fixed_length_bytestring(&mut packet.auth_response, len as usize, buffer);
            }

            // CLIENT_CONNECT_WITH_DB (8)
            if (packet.client_flag_lower & 8) != 0 {
                buffer = read_null_string(&mut packet.database, buffer);
            }

            // //  CLIENT_PLUGIN_AUTH (1UL << 19), 19 - 16 = 3...
            if !buffer.is_empty() && (packet.client_flag_upper & (1_u16 << 3)) != 0 {
                buffer = read_null_string(&mut packet.client_plugin_name, buffer);
            }

            //  CLIENT_CONNECT_ATTRS (1UL << 20), 20 - 16 = 4...
            if !buffer.is_empty() && (packet.client_flag_upper & (1_u16 << 4)) != 0 {
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
            buffer = read_int_3(&mut packet.max_packet_size, buffer);
            buffer = read_null_string(&mut packet.username, buffer);
            // CLIENT_CONNECT_WITH_DB
            if (packet.client_flag_lower & 8) != 0 {
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

#[derive(Debug)]
pub struct AuthSwitchRequestPacket {
    status_tag: u8,
    plugin_name: String,
    plugin_data: Vec<u8>,
}

impl AuthSwitchRequestPacket {
    pub fn new() -> Self {
        AuthSwitchRequestPacket {
            status_tag: 0xFE,
            plugin_name: "mysql_native_password".to_string(),
            plugin_data: vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5],
        }
    }
}

impl ServerPacket for AuthSwitchRequestPacket {
    fn write(&self, _capabilities_lower: u16, _capabilities_upper: u16, buffer: &mut Vec<u8>) {
        write_int_1(self.status_tag, buffer);
        write_null_string(&self.plugin_name, buffer);
        write_null_string(&self.plugin_data, buffer);
    }
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

#[derive(Default, Debug, PartialEq)]
pub struct ComFieldListPacket {
    pub table: String,
    pub field_wildcard: Vec<u8>,
}

impl ClientPacket for ComFieldListPacket {
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error> {
        let mut packet = Self::default();
        let rem = read_null_string(&mut packet.table, buffer);
        read_eof_bytestring(&mut packet.field_wildcard, rem);
        Ok(packet)
    }
}

#[derive(Debug)]
pub struct ErrPacket {
    header: u8,
    error_code: u16,
    sql_state_marker: u8,
    sql_state: String,
    error_message: String,
}

impl ErrPacket {
    pub fn new(error_code: u16, error_message: String) -> Self {
        ErrPacket {
            header: 0xFF,
            error_code,
            sql_state_marker: b'#',
            sql_state: "HY000".to_string(),
            error_message,
        }
    }
}

impl ServerPacket for ErrPacket {
    fn write(&self, capabilities_lower: u16, _capabilities_upper: u16, buffer: &mut Vec<u8>) {
        write_int_1(self.header, buffer);
        write_int_2(self.error_code, buffer);
        // CLIENT_PROTOCOL_41
        if (capabilities_lower & 512) != 0 {
            write_int_1(self.sql_state_marker, buffer);
            buffer.extend_from_slice(self.sql_state.as_bytes());
        }
        write_eof_string(&self.error_message, buffer);
    }
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_ok_packet.html
#[derive(Debug)]
pub struct OkPacket {
    header: u8,
    affected_rows: u64,
    last_insert_id: u64,
    status_flags: u16,
    warnings: u16,
    info: String,
    session_state_info: String,
}

impl OkPacket {
    pub fn new(eof: bool, affected_rows: u64) -> Self {
        OkPacket {
            header: if eof { 0xFE } else { 0 },
            affected_rows,
            last_insert_id: 0,
            status_flags: 2, // autocommit
            warnings: 0,
            info: "".to_string(),
            session_state_info: "".to_string(),
        }
    }
}

impl ServerPacket for OkPacket {
    fn write(&self, capabilities_lower: u16, capabilities_upper: u16, buffer: &mut Vec<u8>) {
        write_int_1(self.header, buffer);
        write_enc_int(self.affected_rows, buffer);
        write_enc_int(self.last_insert_id, buffer);
        // CLIENT_PROTOCOL_41
        if (capabilities_lower & 512) != 0 {
            write_int_2(self.status_flags, buffer);
            write_int_2(self.warnings, buffer);
        // CLIENT_TRANSACTIONS
        } else if (capabilities_lower & 8192) != 0 {
            write_int_2(self.status_flags, buffer);
        }

        // // CLIENT_SESSION_TRACK
        if (capabilities_upper & (1u16 << 7)) != 0 {
            write_null_string(&self.info, buffer);
        //self.session_state_info.write_null_string(buffer);
        } else {
            write_eof_string(&self.info, buffer);
        }
        write_eof_string(&self.info, buffer);
    }
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_ok_packet.html
#[derive(Debug)]
pub struct EofPacket {
    header: u8,
    warnings: u16,
    status_flags: u16,
}

impl EofPacket {
    pub fn new() -> Self {
        EofPacket {
            header: 0xFE,
            warnings: 0,
            status_flags: 2, // Autocommit
        }
    }
}

impl ServerPacket for EofPacket {
    fn write(&self, capabilities_lower: u16, _capabilities_upper: u16, buffer: &mut Vec<u8>) {
        write_int_1(self.header, buffer);
        // CLIENT_PROTOCOL_41
        if (capabilities_lower & 512) != 0 {
            write_int_2(self.warnings, buffer);
            write_int_2(self.status_flags, buffer);
        }
    }
}

#[derive(Debug)]
pub struct ResultsetPacket {
    metadata_follows: u8,
    column_count: usize,
}

impl ResultsetPacket {
    pub fn new(column_count: usize) -> Self {
        ResultsetPacket {
            metadata_follows: 1,
            column_count,
        }
    }
}

impl ServerPacket for ResultsetPacket {
    fn write(&self, _capabilities_lower: u16, capabilities_upper: u16, buffer: &mut Vec<u8>) {
        // CLIENT_OPTIONAL_RESULTSET_METADATA
        if (capabilities_upper & (1_u16 << 9)) != 0 {
            write_int_1(self.metadata_follows, buffer);
        }
        write_enc_int(self.column_count as u64, buffer);
    }
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_com_query_response_text_resultset_column_definition.html
#[derive(Debug)]
struct ColumnPacket {
    table: String,
    name: String,
    character_set: u16,
    column_length: u32,
    column_type: u8,
    flags: u16,
    decimals: u8,
}

impl ColumnPacket {
    fn new(table: String, name: String, data_type: DataType) -> Self {
        // 32768 is to be set for number fields, TIMESTAMP_FLAG   1024
        let flags = 0_u16;

        // Just used for client formatting
        // 0x00 for integers and static strings
        // 0x1f for dynamic strings, double, float
        // 0x00 to 0x51 for decimals
        let mut decimals = 0;
        // Used for client formatting, ie varchar(1024)
        let mut column_length = 1024;

        // https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
        let column_type = match data_type {
            DataType::Text => {
                decimals = 0x1f;
                15
            }
            DataType::Integer => 8,
            DataType::Boolean => 1, // Mysql tinyint
            DataType::Decimal(precision, scale) => {
                column_length = precision as u32;
                // Assuming this is meant to be the scale...
                decimals = scale;
                // New Decimal
                22
            }
        };

        ColumnPacket {
            table,
            name,
            character_set: 33, // UTF8
            column_length,
            column_type,
            flags,
            decimals,
        }
    }
}

impl ServerPacket for ColumnPacket {
    fn write(&self, capabilities_lower: u16, _capabilities_upper: u16, buffer: &mut Vec<u8>) {
        // CLIENT_PROTOCOL_41
        if (capabilities_lower & 512) != 0 {
            write_enc_string("def", buffer);
            write_enc_string("", buffer);
            write_enc_string(&self.table, buffer);
            write_enc_string("", buffer);
            write_enc_string(&self.name, buffer);
            write_enc_string("", buffer);
            write_enc_int(0x0C, buffer);
            write_int_2(self.character_set, buffer);
            write_int_4(self.column_length, buffer);
            write_int_1(self.column_type, buffer);
            write_int_2(self.flags, buffer);
            write_int_1(self.decimals, buffer);
            // These 2 types don't seem to be mentioned in the spec but yet seem to be required to
            // make up the 0x0c length as per the spec
            write_int_2(0, buffer);
        } else {
            write_enc_string(&self.table, buffer);
            write_enc_string(&self.name, buffer);
            write_int_1(1_u8, buffer);
            write_int_1(self.column_type, buffer);
            // CLIENT_LONG_FLAG
            if (capabilities_lower & 4) != 0 {
                write_enc_int(3, buffer);
                write_int_2(self.flags, buffer);
                write_int_1(self.decimals, buffer);
            } else {
                write_enc_int(2, buffer);
                // This doesn't agree with spec but I suspect spec has a typo in it
                write_int_1(self.flags as u8, buffer);
                write_int_1(self.decimals, buffer);
            }
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
        HandshakePacket::new(1).write(0, 0, &mut buf);
        assert_eq!(
            buf.as_slice(),
            [
                10, 56, 46, 48, 46, 48, 45, 105, 110, 99, 114, 101, 115, 113, 108, 0, 1, 0, 0, 0,
                1, 2, 3, 4, 5, 6, 7, 0, 0, 73, 166, 33, 0, 0, 58, 1, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 1, 0, 109, 121, 115, 113, 108, 95, 110, 97,
                116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0
            ]
            .as_ref()
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
        AuthSwitchRequestPacket::new().write(0, 0, &mut buf);
        assert_eq!(
            buf.as_slice(),
            [
                254, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115,
                115, 119, 111, 114, 100, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5,
                5, 5, 0
            ]
            .as_ref()
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
    fn test_com_field_list_packet() -> Result<(), Box<dyn Error>> {
        // Test packet from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_com_field_list.html
        let raw_packet = [
            0x03, 0x64, 0x65, 0x66, 0x04, 0x74, 0x65, 0x73, 0x74, 0x09, 0x66, 0x69, 0x65, 0x6c,
            0x64, 0x6c, 0x69, 0x73, 0x74, 0x09, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x6c, 0x69, 0x73,
            0x74, 0x02, 0x69, 0x64, 0x02, 0x69, 0x64, 0x0c, 0x3f, 0x00, 0x0b, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfb, 0x05, 0x00, 0x00, 0x02, 0xfe, 0x00, 0x00,
            0x02, 0x00,
        ]
        .as_ref();
        let packet = ComFieldListPacket::read(raw_packet)?;
        assert_eq!(
            packet.table,
            "\u{3}def\u{4}test\tfieldlist\tfieldlist\u{2}id\u{2}id\u{c}?"
        );
        assert_eq!(
            packet.field_wildcard,
            [11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 251, 5, 0, 0, 2, 254, 0, 0, 2, 0].as_ref()
        );
        Ok(())
    }

    #[test]
    fn test_err_packet() {
        let mut buf = vec![];
        ErrPacket::new(1096, String::from("No tables used")).write(0xffff, 0, &mut buf);
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
        OkPacket::new(false, 0).write(0xffff, 0, &mut buf);
        // Expected response from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_ok_packet.html
        assert_eq!(
            buf.as_slice(),
            [0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00].as_ref()
        );
    }

    #[test]
    fn test_eof_packet() {
        let mut buf = vec![];
        EofPacket::new().write(0xffff, 0, &mut buf);
        // Expected response from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_eof_packet.html
        assert_eq!(buf.as_slice(), [0xfe, 0x00, 0x00, 0x02, 0x00].as_ref());
    }

    #[test]
    fn test_column_packet() {
        let mut buf = vec![];
        ColumnPacket::new("foo".to_string(), "bar".to_string(), DataType::Integer)
            .write(0xffff, 0, &mut buf);
        assert_eq!(
            buf.as_slice(),
            [
                3, 100, 101, 102, 0, 3, 102, 111, 111, 0, 3, 98, 97, 114, 0, 12, 33, 0, 0, 4, 0, 0,
                8, 0, 0, 0, 0, 0
            ]
            .as_ref()
        );
    }

    #[test]
    fn test_resultset_packet_packet() {
        let mut buf = vec![];
        ResultsetPacket::new(4).write(0xffff, 0, &mut buf);
        assert_eq!(buf.as_slice(), [4].as_ref());
    }
}
