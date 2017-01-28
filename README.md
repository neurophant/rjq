# Redis Job Queue

Simple redis job queue

[![crates.io](https://img.shields.io/crates/v/rjq.svg)](https://crates.io/crates/rjq)
[![Build Status](https://travis-ci.org/embali/rjq.svg?branch=master)](https://travis-ci.org/embali/rjq)


## Enqueue jobs

```rust
extern crate rjq;

use std::time::Duration;
use std::thread::sleep;

use rjq::{Queue, Status};


fn main() {
    let queue = Queue::new("redis://localhost/", "rjq");
    let mut uuids = Vec::new();
    for _ in 0..10 {
        sleep(Duration::from_millis(100));
        uuids.push(queue.enqueue(vec![], 30).unwrap());
    }
    sleep(Duration::from_millis(10000));
    for uuid in uuids.iter() {
        println!("{} {:?} {}",
                 uuid,
                 queue.status(uuid).unwrap_or(Status::FAILED),
                 queue.result(uuid).unwrap_or("".to_string()));
    }
}
```


## Queue worker

```rust
extern crate rjq;

use std::time::Duration;
use std::thread::sleep;
use std::error::Error;

use rjq::Queue;


fn main() {
    fn process(uuid: String, _: Vec<String>) -> Result<String, Box<Error>> {
        sleep(Duration::from_millis(1000));
        println!("{}", uuid);
        Ok(format!("hi from {}", uuid))
    }

    let queue = Queue::new("redis://localhost/", "rjq");
    queue.work(1, process, 5, 10, 30, false, true).unwrap();
}
```


## Job status

QUEUED - job queued for further processing

RUNNING - job is running by worker

LOST - job has not been finished in time

FINISHED - job has been successfully finished

FAILED - job has been failed due to some errors


## Queue functions

### Init queue

```rust
fn new(url: &str, name: &str) -> Queue;
```

url - redis URL

name - queue name

Returns queue

### Drop queue jobs

```rust
fn drop(&self) -> Result<(), Box<Error>>;
```

### Enqueue job

```rust
fn enqueue(&self, args: Vec<String>, expire: usize) -> Result<String, Box<Error>>;
```

args - job arguments

expire - if job has not been started by worker in this time (in seconds), it will expire

Returns job UUID

### Get job status

```rust
fn status(&self, uuid: &str) -> Result<Status, Box<Error>>;
```

uuid - job unique identifier

Returns job status

### Work on queue

```rust
fn work<F: Fn(String, Vec<String>) -> Result<String, Box<Error>> + Send + Sync + 'static>
    (&self,
     wait: usize,
     fun: F,
     timeout: usize,
     freq: usize,
     expire: usize,
     fall: bool,
     infinite: bool)
     -> Result<(), Box<Error>>;
```

wait - time to wait for BLPOP

fun - worker function

timeout - worker function should finish in timeout (in seconds)

freq - job status check frequency (times per second)

expire - job result will expire in this time (in seconds)

fall - panics to terminate process if the job has been lost

infinite - process jobs infinitely one after another, otherwise only one job will be processed

### Get job result

```rust
fn result(&self, uuid: &str) -> Result<String, Box<Error>>;
```

uuid - job unique identifier

Returns job result


## Run tests

```bash
cargo test
```
