use runtime::Runtime;
use server::Server;
use std::error::Error;

#[cfg(not(windows))]
use jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn Error>> {
    let listen_address = "0.0.0.0:3307";
    let path = "target/test_db";
    eprintln!("Initializing Runtime");
    let runtime = Runtime::new(path)?;
    eprintln!("Initializing Server");
    let mut server = Server::new(runtime);
    eprintln!("Server Running");
    server.listen(listen_address)?;
    Ok(())
}
