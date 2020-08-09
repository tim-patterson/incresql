use crate::registry::Registry;
mod casts;
mod maths;

pub fn register_builtins(registry: &mut Registry) {
    casts::register_builtins(registry);
    maths::register_builtins(registry);
}
