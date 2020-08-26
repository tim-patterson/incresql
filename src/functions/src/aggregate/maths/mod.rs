mod avg;
mod sum;

use crate::registry::Registry;

pub fn register_builtins(registry: &mut Registry) {
    avg::register_builtins(registry);
    sum::register_builtins(registry);
}
