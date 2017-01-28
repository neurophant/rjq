extern crate rjq;

use std::time::Duration;
use std::thread::sleep;

use rjq::{Queue, Queues, Status};


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
