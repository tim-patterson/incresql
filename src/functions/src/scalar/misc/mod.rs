use crate::registry::Registry;

mod coalesce;

pub fn register_builtins(registry: &mut Registry) {
    coalesce::register_builtins(registry);
}
