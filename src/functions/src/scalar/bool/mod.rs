use crate::registry::Registry;

mod between;
mod eq;
mod gt;
mod gte;
mod lt;
mod lte;
mod ne;

pub fn register_builtins(registry: &mut Registry) {
    between::register_builtins(registry);
    eq::register_builtins(registry);
    gt::register_builtins(registry);
    gte::register_builtins(registry);
    lt::register_builtins(registry);
    lte::register_builtins(registry);
    ne::register_builtins(registry);
}
