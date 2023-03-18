use thiserror::Error;

#[derive(Error, Debug)]
#[error("trnsparent")]
pub struct LockAbortError {
    #[from]
    source: anyhow::Error,
}
