use runtime::Runtime;
use server::Server;
use std::error::Error;

use clap::{App, Arg};
#[cfg(not(windows))]
use jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("TPCH")
        .arg(
            Arg::with_name("directory")
                .short("d")
                .long("directory")
                .default_value("target/test_db"),
        )
        .get_matches();
    let listen_address = "0.0.0.0:3307";
    let path = matches.value_of("directory").unwrap();
    eprintln!("Initializing Runtime");
    let runtime = Runtime::new(path)?;
    eprintln!("Initializing Server");
    let mut server = Server::new(runtime);
    eprintln!("Server Running");
    server.listen(listen_address)?;
    Ok(())
}
