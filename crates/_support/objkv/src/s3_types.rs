#[derive(Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Written,
    /// The key already existed; `If-None-Match: *` was refused.
    AlreadyExists,
}

#[derive(Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
}
