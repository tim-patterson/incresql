use crate::registry::Registry;
mod json_extract;
mod json_unquote;

pub fn register_builtins(registry: &mut Registry) {
    json_extract::register_builtins(registry);
    json_unquote::register_builtins(registry);
}
