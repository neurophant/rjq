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
//!     let status = queue.status(uuid)?;
//!     let result = queue.result(uuid)?.unwrap();
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

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate redis;
extern crate uuid;

use std::error::Error;
use std::thread;
use std::sync::mpsc::channel;
use std::time::Duration;
use std::thread::sleep;
use std::marker::{Send, Sync};
use std::sync::Arc;
use redis::{Commands, Client};
use uuid::Uuid;

/// Job status
#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    uuid: String,
    status: Status,
    args: Vec<String>,
    result: Option<String>,
}

impl Job {
    fn new(args: Vec<String>) -> Job {
        Job {
            uuid: Uuid::new_v4().to_string(),
            status: Status::QUEUED,
            args: args,
            result: None,
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
    ///
    /// `url` - redis url to connect
    ///
    /// `name` - queue name
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
    ///
    /// `args` - job arguments
    ///
    /// `expire` - job expiration time in seconds, if hasn't started during this time it will be
    /// removed
    ///
    /// Returns unique job identifier
    pub fn enqueue(&self, args: Vec<String>, expire: usize) -> Result<String, Box<Error>> {
        let client = Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        let job = Job::new(args);

        conn.set_ex(format!("{}:{}", self.name, job.uuid),
                    serde_json::to_string(&job)?,
                    expire)?;
        conn.rpush(format!("{}:uuids", self.name), &job.uuid)?;

        Ok(job.uuid)
    }

    /// Get job status
    ///
    /// `uuid` - unique job identifier
    ///
    /// Returns job status
    pub fn status(&self, uuid: &str) -> Result<Status, Box<Error>> {
        let client = redis::Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        let json: String = conn.get(format!("{}:{}", self.name, uuid))?;
        let job: Job = serde_json::from_str(&json)?;

        Ok(job.status)
    }

    /// Work on queue, process enqueued jobs
    ///
    /// `wait` - time to wait in single iteration to pop next job, set 1-10 if not sure about that
    ///
    /// `fun` - function that would work on jobs
    ///
    /// `timeout` - timeout in seconds, if job hasn't been completed during this time, it will be
    /// marked as lost
    ///
    /// `freq` - frequency of checking job status while counting on timeout, number of checks per
    /// second, recommended values from 1 to 50, if not sure set to 10
    ///
    /// `expire` - job result expiration time in seconds
    ///
    /// `fall` - if set to true then worker will panic if job was lost
    ///
    /// `infinite` - if set to false then worker will process one job and quit
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

        let afun = Arc::new(fun);
        let uuids_key = format!("{}:uuids", self.name);
        loop {
            let uuids: Vec<String> = conn.blpop(&uuids_key, wait)?;
            if uuids.len() < 2 {
                if !infinite {
                    break;
                }
                continue;
            }

            let uuid = &uuids[1].to_string();
            let key = format!("{}:{}", self.name, uuid);
            let json: String = match conn.get(&key) {
                Ok(o) => o,
                Err(_) => {
                    if !infinite {
                        break;
                    }
                    continue;
                }
            };

            let mut job: Job = serde_json::from_str(&json)?;

            job.status = Status::RUNNING;
            conn.set_ex(&key, serde_json::to_string(&job)?, timeout + expire)?;

            let (tx, rx) = channel();
            let cafun = afun.clone();
            let cuuid = uuid.clone();
            let cargs = job.args.clone();
            thread::spawn(move || {
                let r = match cafun(cuuid, cargs) {
                    Ok(o) => (Status::FINISHED, Some(o)),
                    Err(_) => (Status::FAILED, None),
                };
                tx.send(r).unwrap_or(())
            });

            for _ in 0..(timeout * freq) {
                let (status, result) = rx.try_recv().unwrap_or((Status::RUNNING, None));
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
            conn.set_ex(&key, serde_json::to_string(&job)?, expire)?;

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
    ///
    /// `uuid` - unique job identifier
    ///
    /// Returns job result
    pub fn result(&self, uuid: &str) -> Result<Option<String>, Box<Error>> {
        let client = redis::Client::open(self.url.as_str())?;
        let conn = client.get_connection()?;

        let json: String = conn.get(format!("{}:{}", self.name, uuid))?;
        let job: Job = serde_json::from_str(&json)?;

        Ok(job.result)
    }
}
