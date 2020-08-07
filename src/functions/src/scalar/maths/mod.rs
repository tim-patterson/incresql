use crate::registry::Registry;

mod add;

pub fn register_builtins(registry: &mut Registry) {
    add::register_builtins(registry)
}
