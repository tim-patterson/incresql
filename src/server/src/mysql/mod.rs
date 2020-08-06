use crate::mysql::packets::{
    write_auth_switch_request_packet, write_handshake_packet, write_ok_packet,
    AuthSwitchResponsePacket, ClientPacket, ComFieldListPacket, ComInitDbPacket, ComQueryPacket,
    HandshakeResponsePacket,
};
use crate::mysql::protocol_base::{read_int_1, read_int_3, write_int_3};
use data::Session;
use std::cmp::min;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::num::Wrapping;

mod constants;
mod packets;
mod protocol_base;

pub struct Connection {
    stream: TcpStream,
    packet_header_buf: Vec<u8>,
    packet_buf: Vec<u8>,
    capabilities: u32,
    sequence_id: Wrapping<u8>,
    session: Session,
}

impl Connection {
    pub fn new(stream: TcpStream, session: Session) -> Self {
        Connection {
            stream,
            packet_header_buf: Vec::new(),
            packet_buf: Vec::new(),
            capabilities: 0,
            sequence_id: Wrapping(0),
            session,
        }
    }

    pub fn connect(&mut self) -> Result<(), std::io::Error> {
        self.handshake()?;
        let connection_id = self.session.connection_id;
        let capabilities = self.capabilities;

        loop {
            match self.receive_packet::<CommandPacket>() {
                Ok(command) => {
                    match command {
                        CommandPacket::ComQuit => {
                            break;
                        }
                        CommandPacket::ComPing => {
                            self.send_packet(|buf| write_ok_packet(false, 0, capabilities, buf))?;
                        }
                        CommandPacket::ComInitDb(_com_init_db) => {
                            self.send_packet(|buf| write_ok_packet(false, 0, capabilities, buf))?;
                        }
                        CommandPacket::ComQuery(com_query) => {
                            dbg!(&com_query.query);
                            //self.process_query_command(com_query.query)?;
                        }
                        CommandPacket::ComFieldList(_com_field_list) => panic!(),
                    }
                }
                Err(io_error) => {
                    println!("Connection {} bailed due to {}", connection_id, io_error);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Set up the initial handshake with the server
    fn handshake(&mut self) -> Result<(), std::io::Error> {
        // Note that these handshake packets in the connection phase don't have the command byte.
        let connection_id = self.session.connection_id;

        self.send_packet(|buf| write_handshake_packet(connection_id, buf))?;

        // Receive response
        let handshake_response = self.receive_packet::<HandshakeResponsePacket>()?;
        let capabilities = handshake_response.client_flags;
        self.capabilities = self.capabilities;
        self.session.user = handshake_response.username;

        // Ask for user's password
        self.send_packet(write_auth_switch_request_packet)?;

        // Get back the user's password
        let _auth_response = self.receive_packet::<AuthSwitchResponsePacket>()?;

        // Reply with Ok.
        self.send_packet(|buf| write_ok_packet(false, 0, capabilities, buf))
    }

    /// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet
    /// Takes a closure as input, that closure must populate the bytes for the packet being sent
    fn send_packet<F: Fn(&mut Vec<u8>)>(&mut self, f: F) -> Result<(), std::io::Error> {
        self.packet_buf.clear();
        f(&mut self.packet_buf);

        let mut payload_length = self.packet_buf.len() as u32;
        let mut payload_byte = self.packet_buf.as_slice();

        while payload_length > 0 {
            let packet_length = min(payload_length, 0xffffff) as u32;
            self.packet_header_buf.clear();

            write_int_3(packet_length, &mut self.packet_header_buf);
            self.packet_header_buf.push(self.sequence_id.0);

            self.stream.write_all(&self.packet_header_buf)?;

            self.stream
                .write_all(&payload_byte[..(packet_length as usize)])?;

            payload_length -= packet_length;
            payload_byte = &payload_byte[(packet_length as usize)..];
            self.sequence_id += Wrapping(1u8);
        }
        self.stream.flush()?;
        Ok(())
    }

    fn receive_packet<P: ClientPacket>(&mut self) -> Result<P, std::io::Error> {
        self.stream
            .read_exact(self.packet_header_buf.as_mut_slice())?;
        let mut header_bytes = self.packet_header_buf.as_slice();
        let mut packet_length = 0_u32;
        header_bytes = read_int_3(&mut packet_length, header_bytes);

        read_int_1(&mut self.sequence_id.0, header_bytes);
        self.sequence_id += Wrapping(1_u8);

        // A bit yuk..., sizing vec so we can just use the stream.read_exact method
        self.packet_buf.clear();
        for _ in 0..packet_length {
            self.packet_buf.push(0);
        }
        self.stream.read_exact(&mut self.packet_buf)?;
        let packet = P::read(&self.packet_buf)?;
        Ok(packet)
    }
}

#[allow(clippy::enum_variant_names)]
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

    #[test]
    fn test_connection() -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
