use runtime::Runtime;
use server::Server;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let listen_address = "0.0.0.0:3306";
    let path = "target/test_db";
    println!("Initializing Runtime");
    let runtime = Runtime::new(path)?;
    println!("Initializing Server");
    let mut server = Server::new(runtime);
    println!("Server Running");
    server.listen(listen_address)?;
    Ok(())
}
