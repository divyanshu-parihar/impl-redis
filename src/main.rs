mod server;
mod store;

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use server::handle_socket;
use store::Record;

fn main() {
    println!("[REDIS] server started");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    // our database
    let map: HashMap<String, Record> = HashMap::new();
    let db = Arc::new(Mutex::new(map));

    for stream in listener.incoming() {
        match stream {
            Ok(mut tcp_stream) => {
                println!("Handling the socket connection");
                let db_clone = Arc::clone(&db);
                std::thread::spawn(move || handle_socket(&mut tcp_stream, db_clone));
            }
            Err(_) => {
                println!("Error")
            }
        }
    }
}
