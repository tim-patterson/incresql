use crate::registry::Registry;

mod database;

pub fn register_builtins(registry: &mut Registry) {
    database::register_builtins(registry);
}
