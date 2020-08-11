use crate::registry::Registry;

mod eq;
mod ne;

pub fn register_builtins(registry: &mut Registry) {
    eq::register_builtins(registry);
    ne::register_builtins(registry);
}
