/// Stores any and all session variables.
pub struct Session {
    pub user: String,
    pub connection_id: u32,
}

impl Session {
    pub fn new(connection_id: u32) -> Self {
        Session {
            user: String::new(),
            connection_id,
        }
    }
}
