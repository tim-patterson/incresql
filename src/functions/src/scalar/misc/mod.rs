use crate::registry::Registry;

mod coalesce;
mod if_fn;

pub fn register_builtins(registry: &mut Registry) {
    coalesce::register_builtins(registry);
    if_fn::register_builtins(registry);
}
