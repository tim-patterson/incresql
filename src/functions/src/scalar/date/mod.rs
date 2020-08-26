use crate::registry::Registry;
mod date_sub;

pub fn register_builtins(registry: &mut Registry) {
    date_sub::register_builtins(registry);
}
