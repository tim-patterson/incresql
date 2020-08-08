use crate::registry::Registry;

mod add;
mod divide;
mod multiply;
mod subtract;

pub fn register_builtins(registry: &mut Registry) {
    add::register_builtins(registry);
    divide::register_builtins(registry);
    multiply::register_builtins(registry);
    subtract::register_builtins(registry);
}
