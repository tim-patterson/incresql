use rocksdb::DB;
use std::sync::Arc;

#[allow(dead_code)]
pub struct Table {
    db: Arc<DB>,
    id: u32,
}

impl Table {
    pub(crate) fn new(db: Arc<DB>, id: u32) -> Self {
        Table { db, id }
    }

    pub fn id(&self) -> u32 {
        self.id
    }
}
