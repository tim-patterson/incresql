use crate::mysql::protocol_base::*;
use std::collections::HashMap;
use std::fmt::Debug;

trait ServerPacket: Debug {
    fn write(&self, capabilities_lower: u16, capabilities_upper: u16, buffer: &mut Vec<u8>);
}

trait ClientPacket
where
    Self: Sized,
{
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error>;
}

/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_connection_phase_packets_protocol_handshake_v10.html
#[derive(Debug)]
struct HandshakePacket {
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
    fn new(connection_id: u32) -> Self {
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
#[derive(Default, Debug)]
struct HandshakeResponsePacket {
    client_flag_lower: u16, // int<2>
    client_flag_upper: u16, // int<2>
    max_packet_size: u32,
    character_set: u8,
    username: String,
    auth_response: Vec<u8>,
    database: String,
    client_plugin_name: String,
    client_connection_attrs: HashMap<String, String>,
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
}
