use crate::registry::Registry;

mod maths;
mod misc;

pub fn register_builtins(registry: &mut Registry) {
    maths::register_builtins(registry);
    misc::register_builtins(registry);
}
