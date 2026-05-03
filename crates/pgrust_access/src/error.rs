use std::fmt;

pub type AccessResult<T> = Result<T, AccessError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessError {
    Corrupt(&'static str),
    Scalar(String),
    Unsupported(String),
}

impl fmt::Display for AccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corrupt(message) => write!(f, "{message}"),
            Self::Scalar(message) | Self::Unsupported(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for AccessError {}
