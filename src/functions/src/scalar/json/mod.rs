use crate::registry::Registry;
mod json_extract;

pub fn register_builtins(registry: &mut Registry) {
    json_extract::register_builtins(registry);
}
