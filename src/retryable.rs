use std::error::Error;

/// Retryable is used to indicate whether the error is a result of an operation that may succeed
/// if it is tried again. If it is not known whether the operation would succeed if retried then
/// default to true.
/// Put another way, retryable should only be false if the error indicates that retrying the
/// operation will always result in an error.
pub trait Retryable: Error {
  fn retryable(&self) -> bool;
}
