use crate::mysql::MysqlConnection;
use runtime::Runtime;
use scoped_threadpool::Pool;
use std::net::TcpListener;
use std::panic::{catch_unwind, AssertUnwindSafe};

// Something to do with the infinite loop for the listen loop means that we trip up rusts deadcode
// detection, we'll just make mysql public to get around it even though there's probably no use for
// it outside of the server
pub mod mysql;

/// Implements a tcp server that accepts mysql connections
pub struct Server {
    runtime: Runtime,
}

impl Server {
    pub fn new(runtime: Runtime) -> Self {
        Server { runtime }
    }

    /// Starts listening for mysql connections. This method doesn't normally terminate.
    pub fn listen(&mut self, addr: &str) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(addr)?;
        let mut pool = Pool::new(500);

        loop {
            if let Ok((stream, _)) = listener.accept() {
                pool.scoped(|scope| {
                    let connection = self.runtime.new_connection();
                    let connection_id = connection.connection_id;
                    scope.execute(move || {
                        if let Err(err) = catch_unwind(AssertUnwindSafe(|| {
                            let mut mysql_connection = MysqlConnection::new(stream, connection);
                            if let Err(err) = mysql_connection.connect() {
                                eprintln!("IO Error for {}\n {:?}", connection_id, err);
                            }
                        })) {
                            eprintln!("Thread panic for connection {}\n {:?}", connection_id, err);
                        }
                    });
                })
            }
        }
    }
}
