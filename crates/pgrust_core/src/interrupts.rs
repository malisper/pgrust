#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum InterruptReason {
    StatementTimeout = 1,
    QueryCancel = 2,
}

impl InterruptReason {
    pub fn message(self) -> &'static str {
        match self {
            Self::StatementTimeout => "canceling statement due to statement timeout",
            Self::QueryCancel => "canceling statement due to user request",
        }
    }

    pub fn sqlstate(self) -> &'static str {
        match self {
            Self::StatementTimeout => "57014",
            Self::QueryCancel => "57014",
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::StatementTimeout),
            2 => Some(Self::QueryCancel),
            _ => None,
        }
    }
}
