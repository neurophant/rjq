#[cfg(test)]

extern crate rjq;

use std::time::Duration;
use std::thread::sleep;
use std::error::Error;
use rjq::{Status, Queue};

#[test]
fn test_job_queued() {
    let queue = Queue::new("redis://localhost/", "test-queued");
    queue.drop().unwrap();

    let uuid = queue.enqueue(vec![], 5).unwrap();

    let status = queue.status(&uuid).unwrap();
    assert!(status == Status::QUEUED);
}

#[test]
#[should_panic]
fn test_job_expired() {
    let queue = Queue::new("redis://localhost/", "test-expired");
    queue.drop().unwrap();

    let uuid = queue.enqueue(vec![], 1).unwrap();
    sleep(Duration::from_millis(2000));

    queue.status(&uuid).unwrap();
}

#[test]
fn test_job_finished() {
    fn fn_ok(_: String, _: Vec<String>) -> Result<String, Box<Error>> {
        sleep(Duration::from_millis(1000));
        Ok("ok".to_string())
    }

    let queue = Queue::new("redis://localhost/", "test-finished");
    queue.drop().unwrap();

    let uuid = queue.enqueue(vec![], 10).unwrap();
    queue.work(1, fn_ok, 5, 1, 5, false, false).unwrap();

    let status = queue.status(&uuid).unwrap();
    assert!(status == Status::FINISHED);
}

#[test]
fn test_job_result() {
    fn fn_ok(_: String, _: Vec<String>) -> Result<String, Box<Error>> {
        sleep(Duration::from_millis(1000));
        Ok("ok".to_string())
    }

    let queue = Queue::new("redis://localhost/", "test-result");
    queue.drop().unwrap();

    let uuid = queue.enqueue(vec![], 10).unwrap();
    queue.work(1, fn_ok, 5, 1, 5, false, false).unwrap();

    let res = queue.result(&uuid).unwrap();
    assert!(res == "ok");
}

#[test]
fn test_job_failed() {
    fn fn_err(_: String, _: Vec<String>) -> Result<String, Box<Error>> {
        sleep(Duration::from_millis(1000));
        Err(From::from("err"))
    }

    let queue = Queue::new("redis://localhost/", "test-failed");
    queue.drop().unwrap();

    let uuid = queue.enqueue(vec![], 10).unwrap();
    queue.work(1, fn_err, 5, 1, 5, false, false).unwrap();

    let status = queue.status(&uuid).unwrap();
    assert!(status == Status::FAILED);
}

#[test]
fn test_job_lost() {
    fn fn_ok(_: String, _: Vec<String>) -> Result<String, Box<Error>> {
        sleep(Duration::from_millis(10000));
        Ok("ok".to_string())
    }

    let queue = Queue::new("redis://localhost/", "test-lost");
    queue.drop().unwrap();

    let uuid = queue.enqueue(vec![], 10).unwrap();
    queue.work(1, fn_ok, 5, 1, 5, false, false).unwrap();

    let status = queue.status(&uuid).unwrap();
    assert!(status == Status::LOST);
}
