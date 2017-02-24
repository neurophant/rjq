//! Redis job queue and worker crate.
//!
//! # Enqueue jobs
//!
//! ```rust,ignore
//! extern crate rjq;
//!
//! use std::time::Duration;
//! use std::thread::sleep;
//! use rjq::{Queue, Status};
//!
//! let queue = Queue::new("redis://localhost/", "rjq");
//! let mut uuids = Vec::new();
//!
//! for _ in 0..10 {
//!     sleep(Duration::from_millis(100));
//!     uuids.push(queue.enqueue(vec![], 30)?);
//! }
//!
//! sleep(Duration::from_millis(10000));
//!
//! for uuid in uuids.iter() {
//!     let status = queue.status(uuid).unwrap_or(Status::FAILED);
//!     let result = queue.result(uuid).unwrap_or("".to_string());
//!     println!("{} {:?} {}", uuid, status, result);
//! }
//! ```
//!
//! # Work on jobs
//!
//! ```rust,ignore
//! extern crate rjq;
//!
//! use std::time::Duration;
//! use std::thread::sleep;
//! use std::error::Error;
//! use rjq::Queue;
//!
//! fn process(uuid: String, _: Vec<String>) -> Result<String, Box<Error>> {
//!     sleep(Duration::from_millis(1000));
//!     println!("{}", uuid);
//!     Ok(format!("hi from {}", uuid))
//! }
//!
//! let queue = Queue::new("redis://localhost/", "rjq");
//! queue.work(1, process, 5, 10, 30, false, true)?;
//! ```

#![deny(missing_docs)]

extern crate redis;
extern crate rustc_serialize;
extern crate uuid;

use std::error::Error;
use std::thread;
use std::sync::mpsc::channel;
use std::time::Duration;
use std::thread::sleep;
use std::marker::{Send, Sync};
use std::sync::Arc;
use redis::{Commands, Client};
use rustc_serialize::json::{encode, decode};
use uuid::Uuid;

/// Job status
#[derive(RustcEncodable, RustcDecodable, Debug, PartialEq)]
pub enum Status {
    /// Job is queued
    QUEUED,
    /// Job is running
    RUNNING,
    /// Job was lost - timeout exceeded
    LOST,
    /// Job finished successfully
    FINISHED,
    /// Job failed
    FAILED,
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
struct Job {
    uuid: String,
    status: Status,
    args: Vec<String>,
    result: String,
}

impl Job {
    fn new(args: Vec<String>) -> Job {
        Job {
            uuid: Uuid::new_v4().to_string(),
            status: Status::QUEUED,
            args: args,
            result: "".to_string(),
        }
    }
}

/// Queue 
pub struct Queue {
    /// Redis url
    url: String,
    /// Queue name
    name: String,
}

impl Queue {
    /// Init new queue object
    pub fn new(url: &str, name: &str) -> Queue {
        Queue {
            url: url.to_string(),
            name: name.to_string(),
        }
    }

    /// Delete enqueued jobs
    pub fn drop(&self) -> Result<(), Box<Error>> {
        let client = Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        conn.del(format!("{}:uuids", self.name))?;

        Ok(())
    }

    /// Enqueue new job
    pub fn enqueue(&self, args: Vec<String>, expire: usize) -> Result<String, Box<Error>> {
        let client = Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        let job = Job::new(args);

        conn.set_ex(format!("{}:{}", self.name, job.uuid), encode(&job)?, expire)?;
        conn.rpush(format!("{}:uuids", self.name), &job.uuid)?;

        Ok(job.uuid)
    }

    /// Get job status
    pub fn status(&self, uuid: &str) -> Result<Status, Box<Error>> {
        let client = redis::Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        let json: String = conn.get(format!("{}:{}", self.name, uuid))?;
        let job: Job = decode(&json)?;

        Ok(job.status)
    }

    /// Work on queue, process enqueued jobs
    pub fn work<F: Fn(String, Vec<String>) -> Result<String, Box<Error>> + Send + Sync + 'static>
        (&self,
         wait: usize,
         fun: F,
         timeout: usize,
         freq: usize,
         expire: usize,
         fall: bool,
         infinite: bool)
         -> Result<(), Box<Error>> {
        let client = redis::Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        let a_fun = Arc::new(fun);
        let uuids_key = format!("{}:uuids", self.name);
        loop {
            let uuids: Vec<String> = conn.blpop(&uuids_key, wait)?;
            if uuids.len() < 2 {
                if !infinite {
                    break;
                }
                continue;
            }

            let uuid = (&uuids[1]).to_string();
            let key = format!("{}:{}", self.name, uuid);
            let json: String = conn.get(&key).unwrap_or("".to_string());

            if json == "" {
                if !infinite {
                    break;
                }
                continue;
            }

            let mut job: Job = decode(&json)?;

            job.status = Status::RUNNING;
            conn.set_ex(&key, encode(&job)?, timeout + expire)?;

            let (tx, rx) = channel();
            let ca_fun = a_fun.clone();
            let cuuid = uuid.clone();
            let args = job.args.clone();
            thread::spawn(move || {
                match ca_fun(cuuid, args) {
                    Ok(res) => {
                        tx.send((Status::FINISHED, res)).unwrap_or(());
                    }
                    Err(_) => {
                        tx.send((Status::FAILED, "".to_string())).unwrap_or(());
                    }
                }
            });

            for _ in 0..(timeout * freq) {
                let (status, result) = rx.try_recv().unwrap_or((Status::RUNNING, "".to_string()));
                job.status = status;
                job.result = result;
                if job.status != Status::RUNNING {
                    break;
                }
                sleep(Duration::from_millis(1000 / freq as u64));
            }
            if job.status == Status::RUNNING {
                job.status = Status::LOST;
            }
            conn.set_ex(&key, encode(&job)?, expire)?;

            if fall && job.status == Status::LOST {
                panic!("LOST");
            }

            if !infinite {
                break;
            }
        }

        Ok(())
    }

    /// Get job result
    pub fn result(&self, uuid: &str) -> Result<String, Box<Error>> {
        let client = redis::Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        let json: String = conn.get(format!("{}:{}", self.name, uuid))?;
        let job: Job = decode(&json)?;

        Ok(job.result)
    }
}
