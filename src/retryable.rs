use std::error::Error;

pub trait Retryable: Error {
    fn retryable(&self) -> bool;
}
