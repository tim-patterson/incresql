mod sum;

use crate::registry::Registry;

pub fn register_builtins(registry: &mut Registry) {
    sum::register_builtins(registry);
}
