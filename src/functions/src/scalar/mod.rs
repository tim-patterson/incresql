use crate::registry::Registry;

mod maths;

pub fn register_builtins(registry: &mut Registry) {
    maths::register_builtins(registry)
}
