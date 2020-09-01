use std::error::Error;

#[cfg(not(windows))]
use jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod _tpch;

fn main() -> Result<(), Box<dyn Error>> {
    _tpch::main()
}
