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


#[derive(RustcEncodable, RustcDecodable, Debug, PartialEq)]
pub enum Status {
    QUEUED,
    RUNNING,
    LOST,
    FINISHED,
    FAILED,
}


#[derive(RustcEncodable, RustcDecodable, Debug)]
struct Job {
    uuid: String,
    status: Status,
    args: Vec<String>,
    result: String,
}


trait Jobs {
    fn new(args: Vec<String>) -> Job;
}


impl Jobs for Job {
    fn new(args: Vec<String>) -> Job {
        Job {
            uuid: Uuid::new_v4().to_string(),
            status: Status::QUEUED,
            args: args,
            result: "".to_string(),
        }
    }
}


pub struct Queue {
    url: String,
    name: String,
}


pub trait Queues {
    fn new(url: &str, name: &str) -> Queue;
    fn drop(&self) -> Result<(), Box<Error>>;
    fn enqueue(&self, args: Vec<String>, expire: usize) -> Result<String, Box<Error>>;
    fn status(&self, uuid: &str) -> Result<Status, Box<Error>>;
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
    fn result(&self, uuid: &str) -> Result<String, Box<Error>>;
}


impl Queues for Queue {
    fn new(url: &str, name: &str) -> Queue {
        Queue {
            url: url.to_string(),
            name: name.to_string(),
        }
    }

    fn drop(&self) -> Result<(), Box<Error>> {
        let client = try!(Client::open(self.url.as_str()));
        let conn = try!(client.get_connection());

        try!(conn.del(format!("{}:uuids", self.name)));

        Ok(())
    }

    fn enqueue(&self, args: Vec<String>, expire: usize) -> Result<String, Box<Error>> {
        let client = try!(Client::open(self.url.as_str()));
        let conn = try!(client.get_connection());

        let job = Job::new(args);

        try!(conn.set_ex(format!("{}:{}", self.name, job.uuid),
                         try!(encode(&job)),
                         expire));
        try!(conn.rpush(format!("{}:uuids", self.name), &job.uuid));

        Ok(job.uuid)
    }

    fn status(&self, uuid: &str) -> Result<Status, Box<Error>> {
        let client = try!(redis::Client::open(self.url.as_str()));
        let conn = try!(client.get_connection());

        let json: String = try!(conn.get(format!("{}:{}", self.name, uuid)));
        let job: Job = try!(decode(&json));

        Ok(job.status)
    }

    fn work<F: Fn(String, Vec<String>) -> Result<String, Box<Error>> + Send + Sync + 'static>
        (&self,
         wait: usize,
         fun: F,
         timeout: usize,
         freq: usize,
         expire: usize,
         fall: bool,
         infinite: bool)
         -> Result<(), Box<Error>> {
        let client = try!(redis::Client::open(self.url.as_str()));
        let conn = try!(client.get_connection());

        let a_fun = Arc::new(fun);
        let uuids_key = format!("{}:uuids", self.name);
        loop {
            let uuids: Vec<String> = try!(conn.blpop(&uuids_key, wait));
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

            let mut job: Job = try!(decode(&json));

            job.status = Status::RUNNING;
            try!(conn.set_ex(&key, try!(encode(&job)), timeout + expire));

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
            try!(conn.set_ex(&key, try!(encode(&job)), expire));

            if fall && job.status == Status::LOST {
                panic!("LOST");
            }

            if !infinite {
                break;
            }
        }

        Ok(())
    }

    fn result(&self, uuid: &str) -> Result<String, Box<Error>> {
        let client = try!(redis::Client::open(self.url.as_str()));
        let conn = try!(client.get_connection());

        let json: String = try!(conn.get(format!("{}:{}", self.name, uuid)));
        let job: Job = try!(decode(&json));

        Ok(job.result)
    }
}
