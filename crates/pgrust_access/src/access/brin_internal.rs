use crate::access::brin_tuple::BrinTuple;
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;

pub const BRIN_PROCNUM_OPCINFO: i16 = 1;
pub const BRIN_PROCNUM_ADDVALUE: i16 = 2;
pub const BRIN_PROCNUM_CONSISTENT: i16 = 3;
pub const BRIN_PROCNUM_UNION: i16 = 4;
pub const BRIN_MANDATORY_NPROCS: i16 = 4;
pub const BRIN_PROCNUM_OPTIONS: i16 = 5;
pub const BRIN_FIRST_OPTIONAL_PROCNUM: i16 = 11;
pub const BRIN_LAST_OPTIONAL_PROCNUM: i16 = 15;

pub const BRIN_PROCNUM_STRATEGY_LT: i16 = 11;
pub const BRIN_PROCNUM_STRATEGY_LE: i16 = 12;
pub const BRIN_PROCNUM_STRATEGY_GE: i16 = 13;
pub const BRIN_PROCNUM_STRATEGY_GT: i16 = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrinOpcInfo {
    pub nstored: usize,
    pub regular_nulls: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrinDesc {
    pub tupdesc: RelationDesc,
    pub total_stored: usize,
    pub info: Vec<BrinOpcInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrinValues {
    pub attno: usize,
    pub has_nulls: bool,
    pub all_nulls: bool,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrinMemTuple {
    pub placeholder: bool,
    pub empty_range: bool,
    pub blkno: u32,
    pub columns: Vec<BrinValues>,
}

impl BrinMemTuple {
    pub fn new(desc: &BrinDesc, blkno: u32) -> Self {
        Self {
            placeholder: false,
            empty_range: true,
            blkno,
            columns: desc
                .info
                .iter()
                .enumerate()
                .map(|(index, info)| BrinValues {
                    attno: index + 1,
                    has_nulls: false,
                    all_nulls: true,
                    values: vec![Value::Null; info.nstored],
                })
                .collect(),
        }
    }

    pub fn placeholder(desc: &BrinDesc, blkno: u32) -> Self {
        let mut tuple = Self::new(desc, blkno);
        tuple.placeholder = true;
        tuple
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrinTupleLocation {
    pub block: u32,
    pub offset: u16,
}

impl BrinTupleLocation {
    pub fn invalid() -> Self {
        Self {
            block: 0,
            offset: 0,
        }
    }

    pub fn is_valid(self) -> bool {
        self.offset != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrinTupleBytes {
    pub header: BrinTuple,
    pub bytes: Vec<u8>,
}
