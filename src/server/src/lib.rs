use crate::mysql::Connection;
use data::Session;
use std::net::TcpListener;
use std::thread;

// Something to do with the infinite loop for the listen loop means that we trip up rusts deadcode
// detection, we'll just make mysql public to get around it even though there's probably no use for
// it outside of the server
pub mod mysql;

pub struct Server {}

impl Server {
    pub fn new() -> Self {
        Server {}
    }

    pub fn listen(&mut self, addr: &str) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(addr)?;
        let mut connection_id = 1;
        loop {
            if let Ok((stream, _)) = listener.accept() {
                let session = Session::new(connection_id);
                connection_id += 1;
                thread::spawn(move || {
                    Connection::new(stream, session);
                });
            }
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Server::new()
    }
}
