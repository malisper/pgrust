use std::fmt;

use pgrust_core::InterruptReason;

pub type AccessResult<T> = Result<T, AccessError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessError {
    Corrupt(&'static str),
    Io(String),
    Interrupted(InterruptReason),
    Scalar(String),
    UniqueViolation(String),
    Unsupported(String),
}

impl fmt::Display for AccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corrupt(message) => write!(f, "{message}"),
            Self::Io(message) => write!(f, "{message}"),
            Self::Interrupted(reason) => write!(f, "interrupted: {reason:?}"),
            Self::Scalar(message) | Self::UniqueViolation(message) | Self::Unsupported(message) => {
                write!(f, "{message}")
            }
        }
    }
}

impl std::error::Error for AccessError {}
