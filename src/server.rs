use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::store::{Record, RecordDataType};

pub fn handle_socket(socket: &mut TcpStream, db: Arc<std::sync::Mutex<HashMap<String, Record>>>) {
    loop {
        let mut buffer = [0; 1024];
        let data = socket.read(&mut buffer);
        match data {
            Ok(0) => {
                break;
            }
            Ok(x) => {
                println!(
                    "Received data from client: {:?}",
                    String::from_utf8(buffer[..x].to_vec())
                );

                match String::from_utf8(buffer[..x].to_vec()) {
                    Ok(str) => {
                        let mut values: Vec<&str> = str.split("\r\n").collect();
                        println!("Values: {:?}", values);
                        if values.len() < 3 {
                            println!("Invalid command received {}", str);
                            let _ = socket.write(b"+PONG\r\n");
                            continue;
                        }
                        values.pop();
                        let command = values[2].to_uppercase();
                        if command == "ECHO" {
                            let size = values.len();
                            let response =
                                format!("${}\r\n{}\r\n", values[size - 1].len(), values[size - 1]);
                            match socket.write(response.as_bytes()) {
                                Ok(data) => {
                                    if data == 0 {
                                        println!("Client disconnected");
                                        break;
                                    }
                                    println!("PONG sent to client {}", data);
                                }
                                Err(err) => {
                                    println!("Error sending PONG to client {}", err);
                                }
                            }
                        } else if command == "PING" {
                            match socket.write("+PONG\r\n".as_bytes()) {
                                Ok(data) => {
                                    if data == 0 {
                                        println!("Client disconnected");
                                        break;
                                    }
                                    println!("PONG sent to client {}", data);
                                }
                                Err(err) => {
                                    println!("Error sending PONG to client {}", err);
                                }
                            }
                        } else if command == "SET" {
                            let key = values[4];
                            let value = values[6];
                            let map = &mut db.lock().unwrap();
                            if values.len() > 7 {
                                if values[8].to_uppercase() == "PX" {
                                    let milli_seconds = values[10].parse::<u64>().unwrap_or(0);
                                    let expiration_time =
                                        Instant::now() + Duration::from_millis(milli_seconds);
                                    let _ = map.insert(
                                        key.to_string(),
                                        Record {
                                            data: RecordDataType::String(value.to_string()),
                                            expiration_time: Some(expiration_time),
                                        },
                                    );
                                    println!(
                                        "SET command executed with expiration: key='{}', value='{}', expires in {} seconds",
                                        key,
                                        value,
                                        milli_seconds / 1000
                                    );
                                    match socket.write("+OK\r\n".as_bytes()) {
                                        Ok(data) => {
                                            if data == 0 {
                                                println!("Client disconnected");
                                                break;
                                            }
                                            println!("OK sent to client {}", data);
                                        }
                                        Err(err) => {
                                            println!("Error sending PONG to client {}", err);
                                        }
                                    }
                                    continue;
                                }
                            }
                            let _ = map.insert(
                                key.to_string(),
                                Record {
                                    data: RecordDataType::String(value.to_string()),
                                    expiration_time: None,
                                },
                            );
                            println!("SET command executed: key='{}', value='{}'", key, value);
                            match socket.write("+OK\r\n".as_bytes()) {
                                Ok(data) => {
                                    if data == 0 {
                                        println!("Client disconnected");
                                        break;
                                    }
                                    println!("OK sent to client {}", data);
                                }
                                Err(err) => {
                                    println!("Error sending PONG to client {}", err);
                                }
                            }
                        } else if command == "GET" {
                            let key = values[4];
                            let mut map = db.lock().unwrap();

                            if let Some(record) = map.get(key) {
                                if let Some(expiration) = record.expiration_time {
                                    if Instant::now() > expiration {
                                        map.remove(key);

                                        socket.write(b"$-1\r\n").unwrap();
                                        continue;
                                    }
                                }

                                if let RecordDataType::String(value) = &record.data {
                                    let response = format!("${}\r\n{}\r\n", value.len(), value);
                                    socket.write(response.as_bytes()).unwrap();
                                } else {
                                    socket.write_all(b"$-1\r\n").unwrap();
                                }
                            } else {
                                socket.write_all(b"$-1\r\n").unwrap();
                            }
                        } else if command == "LPUSH" || command == "RPUSH" {
                            let key = values[4];
                            let mut map = db.lock().unwrap();
                            let mut list_of_values = Vec::new();
                            let mut index = 6;
                            while index < values.len() {
                                list_of_values.push(values[index].to_string());
                                index += 2;
                            }
                            match map.get_mut(key) {
                                Some(r) => match &mut r.data {
                                    RecordDataType::List(list) => {
                                        for el in list_of_values {
                                            if command == "LPUSH" {
                                                list.insert(0, el.to_string());
                                            } else {
                                                list.push(el.to_string());
                                            }
                                        }
                                        let response = format!(":{}\r\n", list.len());
                                        let _ = socket.write_all(response.as_bytes());
                                    }
                                    _ => {
                                        let response = format!(
                                            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                                        );
                                        let _ = socket.write_all(response.as_bytes());
                                    }
                                },
                                None => {
                                    let size = list_of_values.len();
                                    if command == "LPUSH" {
                                        list_of_values.reverse();
                                    }

                                    map.insert(
                                        key.to_string(),
                                        Record {
                                            data: RecordDataType::List(list_of_values),
                                            expiration_time: None,
                                        },
                                    );
                                    let response = format!(":{}\r\n", size);
                                    let _ = socket.write_all(response.as_bytes());
                                }
                            }
                        } else if command == "LRANGE" {
                            let size = values.len();
                            if size != 9 {
                                let _ = socket.write_all(b"-1\r\n");
                            }

                            let key = values[4];
                            let mut map = db.lock().unwrap();

                            match map.get(key) {
                                Some(record) => match &record.data {
                                    RecordDataType::List(list) => {
                                        let size = list.len() as isize;

                                        let mut start = values[6].parse::<isize>().unwrap_or(0);

                                        let mut end = values[8].parse::<isize>().unwrap_or(0);
                                        if start < 0 {
                                            start = start + size
                                        }
                                        if end < 0 {
                                            end = end + size
                                        }
                                        let actual_start = std::cmp::min(start, size);
                                        let actual_end = std::cmp::min(end, size - 1);

                                        if actual_start > actual_end || actual_start >= size {
                                            // Return an empty Redis Array
                                            socket.write_all(b"*0\r\n").unwrap();
                                        } else {
                                            let count = actual_end - actual_start + 1;
                                            let mut response = format!("*{}\r\n", count);

                                            for i in actual_start..=actual_end {
                                                let item = &list[i as usize];
                                                println!("I + {} {}", i, item);
                                                response.push_str(&format!(
                                                    "${}\r\n{}\r\n",
                                                    item.len(),
                                                    item
                                                ));
                                            }

                                            socket.write_all(response.as_bytes()).unwrap();
                                        }
                                    }
                                    RecordDataType::String(_) => {
                                        let _ = socket.write_all(b"0\r\n");
                                    }
                                },
                                None => {}
                            }
                        } else {
                            println!("Received unhandled command: {}", command);
                            let error_response = format!("-ERR unknown command '{}'\r\n", command);
                            let _ = socket.write(error_response.as_bytes());
                        }
                    }
                    Err(err) => {
                        println!("Error converting bytes to string: {}", err);
                    }
                }
            }
            Err(_) => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    // Helper function to setup our mock server and return a client stream
    fn setup_test_server() -> TcpStream {
        // Bind to port 0, which lets the OS pick a random free port
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_address = listener.local_addr().unwrap();

        let db = Arc::new(Mutex::new(HashMap::new()));

        // Start the server handler in a background thread
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                handle_socket(&mut stream, db);
            }
        });

        // Connect our test client to the server we just started
        TcpStream::connect(server_address).unwrap()
    }

    // Helper function to send a command and get the string response
    fn send_command(stream: &mut TcpStream, command: &str) -> String {
        stream.write_all(command.as_bytes()).unwrap();

        // Wait a tiny bit for the server to process and respond
        thread::sleep(Duration::from_millis(10));

        let mut buffer = [0; 1024];
        let size = stream.read(&mut buffer).unwrap();
        String::from_utf8(buffer[..size].to_vec()).unwrap()
    }

    #[test]
    fn test_ping_command() {
        let mut client = setup_test_server();
        // Redis RESP format for PING
        let cmd = "*1\r\n$4\r\nPING\r\n";
        let response = send_command(&mut client, cmd);
        assert_eq!(response, "+PONG\r\n");
    }

    #[test]
    fn test_echo_command() {
        let mut client = setup_test_server();
        // Redis RESP format for ECHO "hello"
        let cmd = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let response = send_command(&mut client, cmd);
        assert_eq!(response, "$5\r\nhello\r\n");
    }

    #[test]
    fn test_set_and_get_command() {
        let mut client = setup_test_server();

        // 1. SET user "Alice"
        let set_cmd = "*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nAlice\r\n";
        let set_response = send_command(&mut client, set_cmd);
        assert_eq!(set_response, "+OK\r\n");

        // 2. GET user
        let get_cmd = "*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n";
        let get_response = send_command(&mut client, get_cmd);
        assert_eq!(get_response, "$5\r\nAlice\r\n");
    }

    #[test]
    fn test_get_nonexistent_key() {
        let mut client = setup_test_server();

        // GET missing_key
        let get_cmd = "*2\r\n$3\r\nGET\r\n$11\r\nmissing_key\r\n";
        let response = send_command(&mut client, get_cmd);

        // Should return Null Bulk String
        assert_eq!(response, "$-1\r\n");
    }

    #[test]
    fn test_set_with_expiration_px() {
        let mut client = setup_test_server();

        // SET key val PX 100 (Expires in 100 milliseconds)
        let set_cmd = "*5\r\n$3\r\nSET\r\n$4\r\ntemp\r\n$4\r\ndata\r\n$2\r\nPX\r\n$3\r\n100\r\n";
        assert_eq!(send_command(&mut client, set_cmd), "+OK\r\n");

        // Immediately GET the key (should exist)
        let get_cmd = "*2\r\n$3\r\nGET\r\n$4\r\ntemp\r\n";
        assert_eq!(send_command(&mut client, get_cmd), "$4\r\ndata\r\n");

        // Wait for 150 milliseconds for the key to expire
        thread::sleep(Duration::from_millis(150));

        // GET the key again (should be expired and deleted)
        assert_eq!(send_command(&mut client, get_cmd), "$-1\r\n");
    }
    #[test]
    fn test_rpush_and_lpush_commands() {
        let mut client = setup_test_server();

        // 1. RPUSH queue "task1" (Creates the list, appends to right)
        let rpush_cmd1 = "*3\r\n$5\r\nRPUSH\r\n$5\r\nqueue\r\n$5\r\ntask1\r\n";
        let rpush_response1 = send_command(&mut client, rpush_cmd1);
        assert_eq!(
            rpush_response1, ":1\r\n",
            "First RPUSH should return length 1"
        );

        // 2. RPUSH queue "task2" (Appends to right)
        let rpush_cmd2 = "*3\r\n$5\r\nRPUSH\r\n$5\r\nqueue\r\n$5\r\ntask2\r\n";
        let rpush_response2 = send_command(&mut client, rpush_cmd2);
        assert_eq!(
            rpush_response2, ":2\r\n",
            "Second RPUSH should return length 2"
        );

        // 3. LPUSH queue "priority_task" (Prepends to left)
        let lpush_cmd = "*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$13\r\npriority_task\r\n";
        let lpush_response = send_command(&mut client, lpush_cmd);
        assert_eq!(lpush_response, ":3\r\n", "LPUSH should return length 3");
    }

    #[test]
    fn test_wrongtype_error() {
        let mut client = setup_test_server();

        // 1. SET a standard string key
        let set_cmd = "*3\r\n$3\r\nSET\r\n$6\r\nconfig\r\n$4\r\nfast\r\n";
        send_command(&mut client, set_cmd);

        // 2. Try to LPUSH onto that string key
        let lpush_cmd = "*3\r\n$5\r\nLPUSH\r\n$6\r\nconfig\r\n$4\r\ndata\r\n";
        let error_response = send_command(&mut client, lpush_cmd);

        assert_eq!(
            error_response,
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }
    #[test]
    fn test_rpush_multiple_elements() {
        let mut client = setup_test_server();

        // RPUSH queue "a" "b" "c" (3 elements)
        let rpush_cmd = "*5\r\n$5\r\nRPUSH\r\n$5\r\nqueue\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
        let response = send_command(&mut client, rpush_cmd);

        // Should return length 3
        assert_eq!(
            response, ":3\r\n",
            "RPUSH with 3 elements should return length 3"
        );
    }

    #[test]
    fn test_lpush_multiple_elements() {
        let mut client = setup_test_server();

        // LPUSH queue "x" "y" (2 elements)
        let lpush_cmd = "*4\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$1\r\nx\r\n$1\r\ny\r\n";
        let response = send_command(&mut client, lpush_cmd);

        // Should return length 2
        assert_eq!(
            response, ":2\r\n",
            "LPUSH with 2 elements should return length 2"
        );

        // LPUSH queue "z" (1 element, total should now be 3)
        let lpush_cmd_single = "*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$1\r\nz\r\n";
        let response_single = send_command(&mut client, lpush_cmd_single);

        assert_eq!(
            response_single, ":3\r\n",
            "Subsequent LPUSH should return length 3"
        );
    }
    #[test]
    fn test_lrange_command() {
        let mut client = setup_test_server();

        // Setup: RPUSH list "one" "two" "three" "four"
        let rpush_cmd = "*6\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nfour\r\n";
        send_command(&mut client, rpush_cmd);

        // Test 1: LRANGE positive (0 to 2) -> "one", "two", "three"
        let lrange_pos = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n0\r\n$1\r\n2\r\n";
        let res_pos = send_command(&mut client, lrange_pos);
        assert_eq!(res_pos, "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n");

        // Test 2: LRANGE negative (0 to -1) -> All elements
        let lrange_neg = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
        let res_neg = send_command(&mut client, lrange_neg);
        assert_eq!(
            res_neg,
            "*4\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nfour\r\n"
        );

        // Test 3: LRANGE out of bounds (1 to 100) -> "two", "three", "four"
        let lrange_oob = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n1\r\n$3\r\n100\r\n";
        let res_oob = send_command(&mut client, lrange_oob);
        assert_eq!(res_oob, "*3\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nfour\r\n");
    }
}
