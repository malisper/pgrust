use ::mcx::{PgString, PgVec};
use ::types_core::{AttrNumber, InvalidAttrNumber, Oid};

// FormData_pg_index trimmed to the fields ports consume (the decode-once
// rd_index projection of an index's relcache entry).
#[derive(Debug)]
pub struct FormData_pg_index<'mcx> {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisvalid: bool,
    pub indisready: bool,
    // indkey.values[0..indnatts]; InvalidAttrNumber marks an expression key.
    pub indkey: PgVec<'mcx, AttrNumber>,
    // !heap_attisnull(rd_indextuple, Anum_pg_index_indpred): partial index.
    pub has_indpred: bool,
    // C caches parsed trees (rd_indexprs/rd_indpred) and copyObjects them out
    // per call; here the nodeToString sources are cached and callers re-parse
    // per open — same O(tree) per-statement cost shape, no copyObject port.
    pub indexprs_src: Option<PgString<'mcx>>,
    pub indpred_src: Option<PgString<'mcx>>,
}

impl FormData_pg_index<'_> {
    #[inline]
    pub fn indkey0(&self) -> AttrNumber {
        self.indkey.first().copied().unwrap_or(InvalidAttrNumber)
    }
}
