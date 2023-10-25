use std::io::{Read, Write};
use serde_json::json;
use std::net::TcpStream;
use std::thread;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum Request {
    Write { key: String, value: String },
    Read { key: String },
}

fn perform_operations(thread_id: usize, num_ops: usize) {
    let mut stream = TcpStream::connect("127.0.0.1:8080").expect("Could not connect to server");
    let mut last_written_value = String::new();

    for i in 0..num_ops {
        let key = format!("key_{}", thread_id);
        let value = format!("value_{}_{}", thread_id, i);

        // Perform write
        let write_payload = json!({
            "type": "Write",
            "key": key.clone(),
            "value": value.clone()
        })
            .to_string();

        stream
            .write_all(format!("{}\n", write_payload).as_bytes())
            .expect("Failed to write to server");
        stream.flush().expect("Failed to flush write buffer");  // Ensure all data is sent
        last_written_value = value.clone();

        // Perform read
        let read_payload = json!({
            "type": "Read",
            "key": key
        })
            .to_string();

        stream
            .write_all(format!("{}\n", read_payload).as_bytes())
            .expect("Failed to write to server");
        stream.flush().expect("Failed to flush read buffer");  // Ensure all data is sent

        let mut buffer = vec![0; 1024];
        let n = stream
            .read(&mut buffer)
            .expect("Failed to read from server");

        let server_response = String::from_utf8_lossy(&buffer[..n]).to_string();
        if server_response != last_written_value {
            println!(
                "Guarantees not met for thread {}: expected {}, got {}",
                thread_id, last_written_value, server_response
            );
        } else {
            println!("Guarantees met for thread {}", thread_id);
        }
    }
}

pub fn run_client() {
    let num_threads = 10;
    let num_ops_per_thread = 100;
    let mut handles = vec![];

    for i in 0..num_threads {
        let handle = thread::spawn(move || {
            perform_operations(i, num_ops_per_thread);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
