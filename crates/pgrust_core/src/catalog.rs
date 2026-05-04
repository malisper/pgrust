pub const DEFAULT_COLLATION_OID: u32 = 100;
pub const GLOBAL_TABLESPACE_OID: u32 = 1664;
pub const RECORD_TYPE_OID: u32 = 2249;
pub const XID8_TYPE_OID: u32 = 5069;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

impl PolicyCommand {
    pub fn as_char(self) -> char {
        match self {
            Self::All => '*',
            Self::Select => 'r',
            Self::Insert => 'a',
            Self::Update => 'w',
            Self::Delete => 'd',
        }
    }

    pub fn from_char(value: char) -> Option<Self> {
        match value {
            '*' => Some(Self::All),
            'r' => Some(Self::Select),
            'a' => Some(Self::Insert),
            'w' => Some(Self::Update),
            'd' => Some(Self::Delete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeCanonicalization {
    Discrete,
    Continuous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgInheritsRow {
    pub inhrelid: u32,
    pub inhparent: u32,
    pub inhseqno: i32,
    pub inhdetachpending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgPartitionedTableRow {
    pub partrelid: u32,
    pub partstrat: char,
    pub partnatts: i16,
    pub partdefid: u32,
    pub partattrs: Vec<i16>,
    pub partclass: Vec<u32>,
    pub partcollation: Vec<u32>,
    pub partexprs: Option<String>,
}
