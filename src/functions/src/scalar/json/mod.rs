use crate::registry::Registry;
mod compile_jsonpath;
mod json_extract;
mod json_extract_unquote;
mod json_unquote;

pub fn register_builtins(registry: &mut Registry) {
    compile_jsonpath::register_builtins(registry);
    json_extract::register_builtins(registry);
    json_extract_unquote::register_builtins(registry);
    json_unquote::register_builtins(registry);
}
