use crate::registry::Registry;
mod bool;
mod casts;
mod maths;

pub fn register_builtins(registry: &mut Registry) {
    bool::register_builtins(registry);
    casts::register_builtins(registry);
    maths::register_builtins(registry);
}
