pub mod connection;
mod error;

pub use error::QueryError;

use crate::connection::Connection;
use data::Session;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock, Weak};

/// Wraps all the runtime services of incresql.
/// connections are created from a runtime and then sql can then be run against a connection.
#[derive(Debug)]
pub struct Runtime {
    connections_state: RwLock<ConnectionsState>,
}

#[derive(Debug)]
struct ConnectionsState {
    connection_id_counter: u32,
    connections: HashMap<u32, Weak<Connection<'static>>>,
}

impl Runtime {
    /// Create a new runtime
    pub fn new() -> Self {
        let connections_state = RwLock::from(ConnectionsState {
            connection_id_counter: 0,
            connections: HashMap::new(),
        });

        Runtime { connections_state }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    /// Returns a new connection on which to execute sql commands
    pub fn new_connection(&self) -> Arc<Connection<'_>> {
        let mut connection_state = self.connections_state.write().unwrap();
        connection_state.connection_id_counter += 1;
        let connection_id = connection_state.connection_id_counter;
        let session = Arc::new(Session::new(connection_id));
        let connection = Arc::from(Connection {
            connection_id,
            session,
            runtime: &self,
        });

        connection_state.connections.insert(
            connection_id,
            Arc::downgrade(unsafe { std::mem::transmute(&connection) }),
        );

        connection
    }

    /// Marks the connection_id passed as killed, its then up to the executors to bail out.
    pub fn kill_connection(&self, connection_id: u32) {
        let mut connection_state = self.connections_state.write().unwrap();
        connection_state
            .connections
            .get_mut(&connection_id)
            .map(|connection| {
                connection
                    .upgrade()
                    .map(|connection| connection.session.kill_flag.store(true, Ordering::Relaxed))
            });
    }

    /// Used by connections when they're dropped to clean up any state
    fn remove_connection(&self, connection_id: u32) {
        let mut connection_state = self.connections_state.write().unwrap();
        connection_state.connections.remove(&connection_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_connection() {
        let runtime = Runtime::new();
        let connection_1 = runtime.new_connection();
        let connection_2 = runtime.new_connection();

        assert_ne!(connection_1.connection_id, connection_2.connection_id);

        assert_eq!(
            connection_2.connection_id,
            connection_2.session.connection_id
        );
    }

    #[test]
    fn test_connection_kill() {
        let runtime = Runtime::new();
        let connection_1 = runtime.new_connection();

        assert_eq!(
            connection_1.session.kill_flag.load(Ordering::Acquire),
            false
        );

        runtime.kill_connection(connection_1.connection_id);

        assert_eq!(connection_1.session.kill_flag.load(Ordering::Acquire), true);
    }

    #[test]
    fn test_connection_drop() {
        let runtime = Runtime::new();
        let connection_1 = runtime.new_connection();
        let connection_2 = runtime.new_connection();

        assert_eq!(
            runtime.connections_state.read().unwrap().connections.len(),
            2
        );

        std::mem::drop(connection_1);
        std::mem::drop(connection_2);

        assert_eq!(
            runtime.connections_state.read().unwrap().connections.len(),
            0
        );
    }
}
