// reltrigger.h Trigger/TriggerDesc + pg_trigger.h/trigger.h bit vocabulary.
// Leaf crate so types_rel (rd_trigdesc) and the executor can both name these.
#![allow(non_snake_case, non_upper_case_globals)]

use mcx::{PgString, PgVec};
use types_core::Oid;

pub const TRIGGER_TYPE_ROW: i16 = 1 << 0;
pub const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
pub const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
pub const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
pub const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
pub const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
pub const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;
pub const TRIGGER_TYPE_LEVEL_MASK: i16 = TRIGGER_TYPE_ROW;
pub const TRIGGER_TYPE_STATEMENT: i16 = 0;
pub const TRIGGER_TYPE_TIMING_MASK: i16 = TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD;
pub const TRIGGER_TYPE_AFTER: i16 = 0;
pub const TRIGGER_TYPE_EVENT_MASK: i16 =
    TRIGGER_TYPE_INSERT | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_TRUNCATE;

pub const TRIGGER_EVENT_INSERT: u32 = 0x0000_0000;
pub const TRIGGER_EVENT_DELETE: u32 = 0x0000_0001;
pub const TRIGGER_EVENT_UPDATE: u32 = 0x0000_0002;
pub const TRIGGER_EVENT_TRUNCATE: u32 = 0x0000_0003;
pub const TRIGGER_EVENT_OPMASK: u32 = 0x0000_0003;
pub const TRIGGER_EVENT_ROW: u32 = 0x0000_0004;
pub const TRIGGER_EVENT_BEFORE: u32 = 0x0000_0008;
pub const TRIGGER_EVENT_AFTER: u32 = 0x0000_0000;
pub const TRIGGER_EVENT_INSTEAD: u32 = 0x0000_0010;
pub const TRIGGER_EVENT_TIMINGMASK: u32 = 0x0000_0018;
pub const AFTER_TRIGGER_DEFERRABLE: u32 = 0x0000_0020;
pub const AFTER_TRIGGER_INITDEFERRED: u32 = 0x0000_0040;

pub const TRIGGER_FIRES_ON_ORIGIN: i8 = b'O' as i8;
pub const TRIGGER_FIRES_ALWAYS: i8 = b'A' as i8;
pub const TRIGGER_FIRES_ON_REPLICA: i8 = b'R' as i8;
pub const TRIGGER_DISABLED: i8 = b'D' as i8;

pub const RI_TRIGGER_PK: i32 = 1;
pub const RI_TRIGGER_FK: i32 = 2;
pub const RI_TRIGGER_NONE: i32 = 0;

#[inline]
pub fn TRIGGER_FOR_ROW(t: i16) -> bool {
    t & TRIGGER_TYPE_ROW != 0
}
#[inline]
pub fn TRIGGER_FOR_BEFORE(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_BEFORE
}
#[inline]
pub fn TRIGGER_FOR_AFTER(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_AFTER
}
#[inline]
pub fn TRIGGER_FOR_INSTEAD(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_INSTEAD
}
#[inline]
pub fn TRIGGER_FOR_INSERT(t: i16) -> bool {
    t & TRIGGER_TYPE_INSERT != 0
}
#[inline]
pub fn TRIGGER_FOR_DELETE(t: i16) -> bool {
    t & TRIGGER_TYPE_DELETE != 0
}
#[inline]
pub fn TRIGGER_FOR_UPDATE(t: i16) -> bool {
    t & TRIGGER_TYPE_UPDATE != 0
}
#[inline]
pub fn TRIGGER_FIRED_BY_INSERT(e: u32) -> bool {
    e & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_INSERT
}
#[inline]
pub fn TRIGGER_FIRED_BY_DELETE(e: u32) -> bool {
    e & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_DELETE
}
#[inline]
pub fn TRIGGER_FIRED_BY_UPDATE(e: u32) -> bool {
    e & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_UPDATE
}
#[inline]
pub fn TRIGGER_FIRED_FOR_ROW(e: u32) -> bool {
    e & TRIGGER_EVENT_ROW != 0
}
#[inline]
pub fn TRIGGER_FIRED_AFTER(e: u32) -> bool {
    e & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_AFTER
}

#[derive(Debug)]
pub struct Trigger<'mcx> {
    pub tgoid: Oid,
    pub tgname: PgString<'mcx>,
    pub tgfoid: Oid,
    pub tgtype: i16,
    pub tgenabled: i8,
    pub tgisinternal: bool,
    pub tgisclone: bool,
    pub tgconstrrelid: Oid,
    pub tgconstrindid: Oid,
    pub tgconstraint: Oid,
    pub tgdeferrable: bool,
    pub tginitdeferred: bool,
    pub tgnargs: i16,
    pub tgnattr: i16,
    pub tgattr: PgVec<'mcx, i16>,
    pub tgargs: PgVec<'mcx, PgString<'mcx>>,
    pub tgqual: Option<PgString<'mcx>>,
    pub tgoldtable: Option<PgString<'mcx>>,
    pub tgnewtable: Option<PgString<'mcx>>,
}

#[derive(Debug)]
pub struct TriggerDesc<'mcx> {
    pub triggers: PgVec<'mcx, Trigger<'mcx>>,
    pub trig_insert_before_row: bool,
    pub trig_insert_after_row: bool,
    pub trig_insert_instead_row: bool,
    pub trig_insert_before_statement: bool,
    pub trig_insert_after_statement: bool,
    pub trig_update_before_row: bool,
    pub trig_update_after_row: bool,
    pub trig_update_instead_row: bool,
    pub trig_update_before_statement: bool,
    pub trig_update_after_statement: bool,
    pub trig_delete_before_row: bool,
    pub trig_delete_after_row: bool,
    pub trig_delete_instead_row: bool,
    pub trig_delete_before_statement: bool,
    pub trig_delete_after_statement: bool,
    pub trig_truncate_before_statement: bool,
    pub trig_truncate_after_statement: bool,
    pub trig_insert_new_table: bool,
    pub trig_update_old_table: bool,
    pub trig_update_new_table: bool,
    pub trig_delete_old_table: bool,
}

impl<'mcx> TriggerDesc<'mcx> {
    pub fn numtriggers(&self) -> usize {
        self.triggers.len()
    }
}
