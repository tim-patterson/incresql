use server::Server;

fn main() -> Result<(), std::io::Error> {
    let listen_address = "0.0.0.0:3306";
    println!("Starting Server");
    let mut server = Server::new();
    println!("Server Started, listening");
    server.listen(listen_address)?;
    Ok(())
}
