use crate::registry::Registry;

mod and;
mod between;
mod eq;
mod gt;
mod gte;
mod is_false;
mod is_null;
mod is_true;
mod lt;
mod lte;
mod ne;
mod not;
mod or;

pub fn register_builtins(registry: &mut Registry) {
    and::register_builtins(registry);
    between::register_builtins(registry);
    eq::register_builtins(registry);
    gt::register_builtins(registry);
    gte::register_builtins(registry);
    is_false::register_builtins(registry);
    is_null::register_builtins(registry);
    is_true::register_builtins(registry);
    lt::register_builtins(registry);
    lte::register_builtins(registry);
    ne::register_builtins(registry);
    not::register_builtins(registry);
    or::register_builtins(registry);
}
