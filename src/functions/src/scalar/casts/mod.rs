use crate::registry::Registry;

mod to_bool;
mod to_int;

pub fn register_builtins(registry: &mut Registry) {
    to_bool::register_builtins(registry);
    to_int::register_builtins(registry);
}
