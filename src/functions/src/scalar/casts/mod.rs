use crate::registry::Registry;

mod to_bigint;
mod to_bool;
mod to_date;
mod to_decimal;
mod to_int;
mod to_json;
mod to_jsonpath;
mod to_text;
mod to_timestamp;
mod type_of;

pub fn register_builtins(registry: &mut Registry) {
    to_bigint::register_builtins(registry);
    to_bool::register_builtins(registry);
    to_date::register_builtins(registry);
    to_decimal::register_builtins(registry);
    to_int::register_builtins(registry);
    to_json::register_builtins(registry);
    to_jsonpath::register_builtins(registry);
    to_text::register_builtins(registry);
    to_timestamp::register_builtins(registry);
    type_of::register_builtins(registry);
}
