# eazydb
Distributed KV store build by Empowered Coders

## Requirements

- Eventually consistent key-value store in RUST
- Each entry in the key-value store will be a pair of binary strings.
- Clients will be able to perform the following operations:
  - Put a new entry into the store.
  - Get the value associated with a key.
- Eazy DB should allow the following guarantees:
  - Read Your Own Write
  - Monotonic Reads
- The evolution of EazyDB should incorporate multiple masters.


## Performance Benchmarks

- EazyDB must be benchmarked against the following -
    For the median of 5 runs -
     - 5 request/sec
     - 20 request/sec
     - 40 request/sec
