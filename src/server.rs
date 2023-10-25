use std::collections::hash_map::DefaultHasher;
use std::collections::{Bound, BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, mpsc, Mutex, MutexGuard};
use std::sync::atomic::AtomicUsize;
use std::thread;
use rustc_hash::FxHashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, Sender};
use serde::{Serialize, Deserialize};

#[derive(Clone)]
struct Data {
    value: String,
    version: HashMap<usize, u64>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Request {
    Write { key: String, value: String },
    Read { key: String },
    Print {}
}

enum Message {
    ClientRequest { client_id: usize, request: Request },
    Replicate { key: String, value: String, version: HashMap<usize, u64> },
}

type Database = HashMap<String, Data>;

static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(0);
fn init_nodes(num_nodes: usize, response_sender: Sender<(usize, String)>, ring: Arc<Mutex<FxHashMap<u64, usize>>>) -> Vec<Sender<Message>> {
    println!("Initializing {} nodes", num_nodes);
    let shared_senders = Arc::new(Mutex::new(vec![]));
    let mut senders = vec![];
    for id in 0..num_nodes {
        let (tx, rx) = mpsc::channel();
        let version_vector = Arc::new(Mutex::new(HashMap::new()));

        {
            let mut senders_lock = shared_senders.lock().unwrap();
            senders_lock.push(tx.clone());
        }

        let tx_clone = tx.clone();
        senders.push(tx_clone);
        let senders_clone = shared_senders.clone();
        let ring_clone = ring.clone();
        let response_sender_clone = response_sender.clone();

        thread::spawn(move || {
            node(id, rx, senders_clone, response_sender_clone, version_vector, ring_clone);
        });
    }

    senders
}
fn node(
    id: usize,
    receiver: Receiver<Message>,
    senders: Arc<Mutex<Vec<Sender<Message>>>>,
    response_sender: Sender<(usize, String)>,
    version_vector: Arc<Mutex<HashMap<usize, u64>>>,
    ring: Arc<Mutex<FxHashMap<u64, usize>>>,
) {
    let mut local_db: Database = HashMap::new();

    {
        let mut cons_hash_ring = ring.lock().unwrap();
        update_ring(&mut cons_hash_ring, id, 3);
    }


    while let Ok(message) = receiver.recv() {
        match message {
            Message::ClientRequest { client_id, request } => {
                let mut new_version_vector = version_vector.lock().unwrap().clone();
                match request {
                    Request::Write { key, value } => {
                        let new_version = new_version_vector.entry(id).or_insert(0);
                        *new_version += 1;

                        local_db.insert(
                            key.clone(),
                            Data {
                                value: value.clone(),
                                version: new_version_vector.clone(),
                            },
                        );

                        {
                            let mut sent_count = 1;

                            let mut senders = senders.lock().unwrap();
                            let quorum_size = senders.len() / 2 + 1;
                            while sent_count <= quorum_size {
                                senders[(id + sent_count) % senders.len()].send(Message::Replicate {
                                        key: key.clone(),
                                        value: value.clone(),
                                        version: new_version_vector.clone(),
                                    }).unwrap();

                                sent_count += 1;

                            }
                        }
                    },
                    Request::Read { key } => {
                        match local_db.get(&key) {
                            Some(data) => {
                                let response = format!("{}", data.value);
                                response_sender.send((client_id, response)).unwrap();
                            },
                            None => {
                                let response = format!("Key {} not found", key);
                                response_sender.send((client_id, response)).unwrap();
                            },
                        }
                    },
                    Request::Print {} => {
                        print_local_state(id, local_db.clone());
                    }
                }
            },
            Message::Replicate { key, value, version } => {
                if let Some(existing) = local_db.get(&key) {
                    if should_update(&existing.version, &version) {
                        local_db.insert(key, Data { value, version });
                    } else {
                        println!("Node {} ignoring update for key {}", id, key)
                    }
                } else {
                    local_db.insert(key, Data { value, version });
                }

            },
        }
    }
}

fn print_local_state(id: usize, local_db: Database) {
    println!("Local state on node {}:", id);
    for (key, value) in local_db.iter() {
        println!("{}: {}", key, value.value);
    }
    println!();
}

fn should_update(existing_version: &HashMap<usize, u64>, incoming_version: &HashMap<usize, u64>) -> bool {
    let mut is_concurrent = false;
    let mut is_causally_before = true;

    for (node, existing_version_num) in existing_version.iter() {
        let incoming_version_num = incoming_version.get(node).unwrap_or(&0);

        if existing_version_num > incoming_version_num {
            is_causally_before = false;
        }
        if existing_version_num < incoming_version_num {
            is_concurrent = true;
        }
    }

    for (node, incoming_version_num) in incoming_version.iter() {
        let existing_version_num = existing_version.get(node).unwrap_or(&0);

        if incoming_version_num > existing_version_num {
            is_concurrent = true;
        }
        if incoming_version_num < existing_version_num {
            is_causally_before = false;
        }
    }

    is_concurrent || is_causally_before
}

// Inside server.rs

pub fn run_server() {
    let ring: Arc<Mutex<FxHashMap<u64, usize>>> = Arc::new(Mutex::new(FxHashMap::default()));
    let client_streams: Arc<Mutex<HashMap<usize, TcpStream>>> = Arc::new(Mutex::new(HashMap::new()));
    let (response_tx, response_rx) = mpsc::channel();

    let senders = Arc::new(init_nodes(5, response_tx.clone(), ring.clone()));

    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();


    let client_streams_clone = Arc::clone(&client_streams);
    thread::spawn( move || {
        while let Ok((client_id, response)) = response_rx.recv() {
            let map = client_streams_clone.lock().unwrap();

            if let Some(mut stream) = map.get(&client_id) {
                let _ = stream.write_all(response.as_bytes()).unwrap();  // Be more careful in real code
            } else {
                println!("Stream for client {} not found", client_id);
            }
        }
    });

    loop {
        let (stream, addr) = listener.accept().unwrap();
        let client_id = extract_client_id(&stream);

        let stream_clone = stream.try_clone().expect("Failed to clone stream");
        {
            let mut map = client_streams.lock().unwrap();
            map.insert(client_id, stream);
        }

        let ring = ring.clone();
        let senders_clone = Arc::clone(&senders);
        thread::spawn(move || {
            handle(stream_clone, ring, senders_clone.clone());
        });
    }
}

impl Request {
    fn key(&self) -> &str {
        match self {
            Request::Read { key } => key,
            Request::Write { key, value: _ } => key,
            Request::Print {} => "",
        }
    }
}


fn handle(
    mut stream: TcpStream,
    ring: Arc<Mutex<FxHashMap<u64, usize>>>,
    senders: Arc<Vec<Sender<Message>>>,
) {
    loop {
        let reader = BufReader::new(stream.try_clone().expect("Failed to clone stream"));
        let client_id = extract_client_id(&stream);
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    let json: Result<Request, _> = serde_json::from_str(&line);

                    match json {
                        Ok(req) => {
                            if req.key() == "" {
                                for sender in &(*senders) {
                                    sender.send(Message::ClientRequest {
                                        client_id,
                                        request: req.clone(),
                                    }).unwrap();
                                }
                            } else {
                                let key_hash = calculate_hash(&req.key());
                                let node_id;
                                {
                                    let ring = ring.lock().unwrap();
                                    node_id = ring.get_node(key_hash);
                                }

                                // Send the message to the node responsible for this key.
                                let sender = &(*senders)[node_id].clone();
                                sender.send(Message::ClientRequest {
                                    client_id,
                                    request: req.clone(),
                                }).unwrap();
                            }
                        },
                        Err(err) => {
                            println!("Failed to parse JSON: {}", err);
                        },
                    }
                },
                Err(err) => {
                    println!("Failed to read line: {}", err);
                    break;
                }
            }
        }
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

fn combine_hashes(a: u64, b: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    a.hash(&mut hasher);
    b.hash(&mut hasher);
    hasher.finish()
}

fn update_ring(ring: &mut MutexGuard<FxHashMap<u64, usize>>, node_id: usize, num_replicas: usize) {
    for i in 0..num_replicas {
        let hash_a = calculate_hash(&node_id);
        let hash_b = calculate_hash(&i);
        let combined_hash = combine_hashes(hash_a, hash_b);
        ring.insert(combined_hash, node_id);
    }
}

pub trait HashRingTrait {
    fn get_node(&self, hash: u64) -> usize;
}

fn sorted_ring(ring: &FxHashMap<u64, usize>) -> BTreeMap<u64, usize> {
    let mut sorted = BTreeMap::new();
    for (key, value) in ring.iter() {
        sorted.insert(*key, *value);
    }
    sorted
}

impl HashRingTrait for FxHashMap<u64, usize> {
    fn get_node(&self, hash: u64) -> usize {
        let sorted_ring = sorted_ring(self);

        // Find the first node that has a hash greater than the key's hash
        for (_, &node_id) in sorted_ring.range((Bound::Excluded(hash), Bound::Unbounded)) {
            return node_id;
        }

        // If we get here, it means that no nodes in the ring have a hash greater
        // than the key's hash. In this case, the key should wrap around to the
        // first node in the ring.
        *sorted_ring.values().next().expect("Ring is empty")
    }
}

fn extract_client_id(stream: &TcpStream) -> usize {
    let peer_addr = stream.peer_addr().expect("0.0.0.0"); // Getting the peer address as a string

    // Combine the peer address and the counter to create a "unique" client_id
    let client_id = format!("{}", peer_addr)
        .as_bytes()
        .iter()
        .map(|&byte| byte as usize)
        .sum();

    client_id
}
