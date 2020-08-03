use crate::mysql::packets::{ClientPacket, ComFieldListPacket, ComInitDbPacket, ComQueryPacket};
use std::net::TcpStream;
use std::num::Wrapping;

mod packets;
mod protocol_base;

pub struct Connection {
    stream: TcpStream,
    packet_header_buf: Vec<u8>,
    packet_buf: Vec<u8>,
    capabilities_lower: u16,
    capabilities_upper: u16,
    sequence_id: Wrapping<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            packet_header_buf: Vec::new(),
            packet_buf: Vec::new(),
            capabilities_lower: 0,
            capabilities_upper: 0,
            sequence_id: Wrapping(0),
        }
    }
}

#[derive(Debug, PartialEq)]
enum CommandPacket {
    ComQuit,
    ComInitDb(ComInitDbPacket),
    ComQuery(ComQueryPacket),
    ComFieldList(ComFieldListPacket),
    ComPing,
}

impl ClientPacket for CommandPacket {
    fn read(buffer: &[u8]) -> Result<Self, std::io::Error> {
        let packet = match buffer[0] {
            0x01 => CommandPacket::ComQuit,
            0x02 => CommandPacket::ComInitDb(ComInitDbPacket::read(&buffer[1..])?),
            0x03 => CommandPacket::ComQuery(ComQueryPacket::read(&buffer[1..])?),
            0x04 => CommandPacket::ComFieldList(ComFieldListPacket::read(&buffer[1..])?),
            0x0E => CommandPacket::ComPing,
            _ => panic!("Unknown packet {:?}", buffer[0]),
        };

        Ok(packet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_command_packet() -> Result<(), Box<dyn Error>> {
        // Test case from https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_com_init_db.html
        let buf = [0x02, 0x74, 0x65, 0x73, 0x74].as_ref();
        let packet = CommandPacket::read(&buf)?;
        assert_eq!(
            packet,
            CommandPacket::ComInitDb(ComInitDbPacket {
                schema: "test".to_string()
            })
        );
        Ok(())
    }
}
