use core::cell::Cell;

use ::datum::Datum;
use ::mcx::{PgBox, PgString, PgVec};
use ::types_core::{AttrNumber, Oid, NAMEDATALEN};

pub const TYPALIGN_CHAR: i8 = b'c' as i8;
pub const TYPALIGN_SHORT: i8 = b's' as i8;
pub const TYPALIGN_INT: i8 = b'i' as i8;
pub const TYPALIGN_DOUBLE: i8 = b'd' as i8;
pub const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
pub const TYPSTORAGE_EXTERNAL: i8 = b'e' as i8;
pub const TYPSTORAGE_MAIN: i8 = b'm' as i8;
pub const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;

pub const InvalidCompressionMethod: i8 = 0;

pub const ATTNULLABLE_UNRESTRICTED: i8 = b'f' as i8;
pub const ATTNULLABLE_UNKNOWN: i8 = b'u' as i8;
pub const ATTNULLABLE_VALID: i8 = b'v' as i8;
pub const ATTNULLABLE_INVALID: i8 = b'i' as i8;

pub const ALIGNOF_SHORT: u8 = 2;
pub const ALIGNOF_INT: u8 = 4;
pub const ALIGNOF_DOUBLE: u8 = 8;
pub const MAXIMUM_ALIGNOF: usize = 8;

#[inline]
pub const fn MAXALIGN(len: usize) -> usize {
    (len + MAXIMUM_ALIGNOF - 1) & !(MAXIMUM_ALIGNOF - 1)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameData {
    pub data: [u8; NAMEDATALEN as usize],
}

impl NameData {
    pub fn name_str(&self) -> &[u8] {
        let len = self
            .data
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.data.len());
        &self.data[..len]
    }

    pub fn namestrcpy(&mut self, src: &str) {
        self.data.fill(0);
        let bytes = src.as_bytes();
        let len = bytes.len().min(NAMEDATALEN as usize - 1);
        self.data[..len].copy_from_slice(&bytes[..len]);
    }
}

impl Default for NameData {
    fn default() -> Self {
        Self {
            data: [0; NAMEDATALEN as usize],
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FormData_pg_attribute {
    pub attrelid: Oid,
    pub attname: NameData,
    pub atttypid: Oid,
    pub attlen: i16,
    pub attnum: i16,
    pub atttypmod: i32,
    pub attndims: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
    pub attcompression: i8,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: i8,
    pub attgenerated: i8,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attinhcount: i16,
    pub attcollation: Oid,
}

// attcacheoff: C writes this cache through a shared TupleDesc; Cell = plain-store cost.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactAttribute {
    pub attcacheoff: Cell<i32>,
    pub attlen: i16,
    pub attbyval: bool,
    pub attispackable: bool,
    pub atthasmissing: bool,
    pub attisdropped: bool,
    pub attgenerated: bool,
    pub attnullability: i8,
    pub attalignby: u8,
}

const _: () = assert!(core::mem::size_of::<CompactAttribute>() == 16);

impl CompactAttribute {
    // populate_compact_attribute_internal minus IsCatalogRelationOid (catalog
    // columns report ATTNULLABLE_VALID; arrives with the tupdesc unit).
    pub fn populate_from(src: &FormData_pg_attribute) -> CompactAttribute {
        CompactAttribute {
            attcacheoff: Cell::new(-1),
            attlen: src.attlen,
            attbyval: src.attbyval,
            attispackable: src.attstorage != TYPSTORAGE_PLAIN,
            atthasmissing: src.atthasmissing,
            attisdropped: src.attisdropped,
            attgenerated: src.attgenerated != 0,
            attnullability: if !src.attnotnull {
                ATTNULLABLE_UNRESTRICTED
            } else {
                ATTNULLABLE_UNKNOWN
            },
            attalignby: match src.attalign {
                TYPALIGN_INT => ALIGNOF_INT,
                TYPALIGN_CHAR => 1,
                TYPALIGN_DOUBLE => ALIGNOF_DOUBLE,
                TYPALIGN_SHORT => ALIGNOF_SHORT,
                _ => panic!("invalid attalign value: {}", src.attalign),
            },
        }
    }
}

#[derive(Debug)]
pub struct AttrDefault<'mcx> {
    pub adnum: AttrNumber,
    pub adbin: Option<PgString<'mcx>>,
}

#[derive(Debug)]
pub struct ConstrCheck<'mcx> {
    pub ccname: Option<PgString<'mcx>>,
    pub ccbin: Option<PgString<'mcx>>,
    pub ccenforced: bool,
    pub ccvalid: bool,
    pub ccnoinherit: bool,
}

// By-ref am_value points into descriptor-owned bytes; borrows are bounded by
// the descriptor's lifetime (heaptuple.c's missing_cache dissolves).
#[derive(Clone, Copy, Debug)]
pub struct AttrMissing {
    pub am_present: bool,
    pub am_value: Datum,
}

#[derive(Debug)]
pub struct TupleConstr<'mcx> {
    pub defval: PgVec<'mcx, AttrDefault<'mcx>>,
    pub check: PgVec<'mcx, ConstrCheck<'mcx>>,
    pub missing: PgVec<'mcx, AttrMissing>,
    pub num_defval: u16,
    pub num_check: u16,
    pub has_not_null: bool,
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

#[derive(Debug)]
pub struct TupleDescData<'mcx> {
    pub natts: i32,
    pub tdtypeid: Oid,
    pub tdtypmod: i32,
    pub tdrefcount: i32,
    pub constr: Option<PgBox<'mcx, TupleConstr<'mcx>>>,
    pub compact_attrs: PgVec<'mcx, CompactAttribute>,
    // C's trailing Form_pg_attribute array (TupleDescAttr), one entry per attribute.
    pub attrs: PgVec<'mcx, FormData_pg_attribute>,
}

pub type TupleDesc<'a, 'mcx> = &'a TupleDescData<'mcx>;

// SearchSysCache1(TYPEOID) projected to the fields TupleDescInitEntry reads.
#[derive(Clone, Copy, Debug)]
pub struct PgTypeShape {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: i8,
    pub typstorage: i8,
    pub typcollation: Oid,
}

impl<'mcx> TupleDescData<'mcx> {
    #[inline]
    pub fn attr(&self, i: usize) -> &FormData_pg_attribute {
        &self.attrs[i]
    }

    #[inline]
    pub fn attr_mut(&mut self, i: usize) -> &mut FormData_pg_attribute {
        &mut self.attrs[i]
    }

    #[inline]
    pub fn compact_attr(&self, i: usize) -> &CompactAttribute {
        &self.compact_attrs[i]
    }

    pub fn populate_compact_attribute(&mut self, attnum: usize) {
        self.compact_attrs[attnum] = CompactAttribute::populate_from(&self.attrs[attnum]);
    }
}
