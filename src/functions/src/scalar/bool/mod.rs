use crate::registry::Registry;

mod eq;
mod gt;
mod gte;
mod lt;
mod lte;
mod ne;

pub fn register_builtins(registry: &mut Registry) {
    eq::register_builtins(registry);
    gt::register_builtins(registry);
    gte::register_builtins(registry);
    lt::register_builtins(registry);
    lte::register_builtins(registry);
    ne::register_builtins(registry);
}
