mod count;

use crate::registry::Registry;

pub fn register_builtins(registry: &mut Registry) {
    count::register_builtins(registry);
}
