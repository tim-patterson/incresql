use crate::registry::Registry;

mod to_bigint;
mod to_bool;
mod to_int;
mod to_text;

pub fn register_builtins(registry: &mut Registry) {
    to_bigint::register_builtins(registry);
    to_bool::register_builtins(registry);
    to_int::register_builtins(registry);
    to_text::register_builtins(registry);
}
