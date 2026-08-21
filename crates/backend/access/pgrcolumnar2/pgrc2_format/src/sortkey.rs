//! Sort-key record (spec §9): parts record their sort key; ordering is a
//! format property feeding sort elision and aggregation-in-order.

use crate::class::CollationClass;
use crate::wire::{put_u16, put_u32, put_u8, Cur};
use crate::{FormatError, FormatResult};

pub const SORT_KEY_ENTRY_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SortDir {
    Asc = 0,
    Desc = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NullsOrder {
    First = 0,
    Last = 1,
}

/// One sort-key column (8 B). Wire order == declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SortKeyEntry {
    pub attno: u32,
    pub dir: u8,
    pub nulls: u8,
    pub collation_class: u8,
    pub pad: u8,
}

/// The SortKey section body: `{ nkeys: u16, flags: u16, pad: u32 }` +
/// entries. `nkeys == 0` = unordered part.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SortKeyRecord {
    pub keys: Vec<SortKeyEntry>,
}

impl SortKeyRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.keys.len() * SORT_KEY_ENTRY_LEN);
        put_u16(&mut out, self.keys.len() as u16);
        put_u16(&mut out, 0);
        put_u32(&mut out, 0);
        for k in &self.keys {
            put_u32(&mut out, k.attno);
            put_u8(&mut out, k.dir);
            put_u8(&mut out, k.nulls);
            put_u8(&mut out, k.collation_class);
            put_u8(&mut out, k.pad);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<SortKeyRecord> {
        let mut c = Cur::new(bytes);
        let nkeys = c.u16("SortKeyRecord")?;
        let _flags = c.u16("SortKeyRecord")?;
        let _pad = c.u32("SortKeyRecord")?;
        if c.remaining() != nkeys as usize * SORT_KEY_ENTRY_LEN {
            return Err(FormatError::Corrupt {
                at: "SortKeyRecord length",
            });
        }
        let mut keys = Vec::with_capacity(nkeys as usize);
        for _ in 0..nkeys {
            let e = SortKeyEntry {
                attno: c.u32("SortKeyEntry")?,
                dir: c.u8("SortKeyEntry")?,
                nulls: c.u8("SortKeyEntry")?,
                collation_class: c.u8("SortKeyEntry")?,
                pad: c.u8("SortKeyEntry")?,
            };
            if e.dir > 1 || e.nulls > 1 {
                return Err(FormatError::Corrupt {
                    at: "SortKeyEntry dir/nulls",
                });
            }
            CollationClass::from_u8(e.collation_class)?;
            keys.push(e);
        }
        Ok(SortKeyRecord { keys })
    }
}
