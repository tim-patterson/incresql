use crate::registry::Registry;

mod eq;

pub fn register_builtins(registry: &mut Registry) {
    eq::register_builtins(registry);
}
