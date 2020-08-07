use std::sync::atomic::AtomicBool;
use std::sync::RwLock;

/// Stores any and all session variables.
#[derive(Debug)]
pub struct Session {
    pub user: RwLock<String>,
    pub current_database: RwLock<String>,
    pub connection_id: u32,
    pub kill_flag: AtomicBool,
}

impl Session {
    pub fn new(connection_id: u32) -> Self {
        Session {
            user: RwLock::from(String::new()),
            current_database: RwLock::from(String::from("default")),
            connection_id,
            kill_flag: AtomicBool::from(false),
        }
    }
}
