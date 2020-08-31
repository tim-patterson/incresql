use crate::registry::Registry;
mod bool;
mod casts;
mod date;
mod json;
mod maths;
mod misc;
mod session;

pub fn register_builtins(registry: &mut Registry) {
    bool::register_builtins(registry);
    casts::register_builtins(registry);
    date::register_builtins(registry);
    json::register_builtins(registry);
    maths::register_builtins(registry);
    misc::register_builtins(registry);
    session::register_builtins(registry);
}
