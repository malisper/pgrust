//! Idiomatic port of `src/backend/utils/adt/ri_triggers.c` (PostgreSQL 18.3) —
//! the generic trigger procedures that enforce FOREIGN KEY referential
//! integrity: the per-event check/restrict/cascade/setnull/setdefault triggers,
//! the bulk `RI_Initial_Check` (ALTER TABLE ADD FK validate) and
//! `RI_PartitionRemove_Check`, plus `RI_FKey_trigger_type` and the
//! process-local constraint-info / prepared-plan caches.
//!
//! The caches are process-local (`HTAB`s in C), modelled as `thread_local!`
//! per-backend tables. The constraint info and FK queries are built here with
//! byte-identical text/SQLSTATEs; everything that crosses a subsystem boundary
//! (SPI, the trigger manager, table access, the catalog/syscache, the fmgr/Datum
//! value layer, GUC, snapshots, security context) routes through the owner's
//! seam crate and panics loudly until that owner lands.

#![allow(clippy::result_large_err)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::too_many_arguments)]

pub mod cache;
pub mod checks;
pub mod fmgr_builtins;
pub mod querybuild;
pub mod triggers;

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_rel::Relation;
use types_ri_triggers::TriggerDataRef;
use types_tuple::heaptuple::NameData;

// ---------------------------------------------------------------------------
// RelSide: the two sources of a "Relation" in this unit
//
// In C `fk_rel`/`pk_rel` are both `Relation`, whether opened here via
// `table_open` or handed in as the trigger relation. In this port the opened
// relation is a real `types_rel::Relation` we own (RAII close), while the
// trigger relation is owned by the trigger manager and reached by handle. The
// field/attribute readers dispatch on which side they are given.
// ---------------------------------------------------------------------------

/// A relation as the RI logic sees it: either one we opened (`table_open`,
/// owned `Relation`) or the trigger manager's trigger relation (by handle).
#[derive(Clone, Copy)]
pub enum RelSide<'a, 'mcx> {
    /// `table_open(...)` result we own.
    Open(&'a Relation<'mcx>),
    /// `trigdata->tg_relation`, owned by the trigger manager.
    Trigger(TriggerDataRef),
}

impl<'a, 'mcx> RelSide<'a, 'mcx> {
    /// `RelationGetRelid(rel)`.
    pub fn oid(&self) -> Oid {
        match self {
            RelSide::Open(r) => r.rd_id,
            RelSide::Trigger(t) => backend_commands_trigger_seams::tg_relation_oid::call(*t),
        }
    }

    /// `RelationGetRelationName(rel)` — raw server-encoded name bytes.
    pub fn name_bytes(&self, mcx: Mcx<'_>) -> PgResult<Vec<u8>> {
        match self {
            RelSide::Open(r) => Ok(r.rd_rel.relname.as_str().as_bytes().to_vec()),
            RelSide::Trigger(t) => {
                Ok(backend_commands_trigger_seams::tg_relation_name::call(mcx, *t)?.to_vec())
            }
        }
    }

    /// `RelationGetNamespace(rel)` — `rd_rel->relnamespace`.
    pub fn namespace(&self) -> Oid {
        match self {
            RelSide::Open(r) => r.rd_rel.relnamespace,
            RelSide::Trigger(t) => backend_commands_trigger_seams::tg_relation_namespace::call(*t),
        }
    }

    /// `rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE`.
    pub fn is_partitioned(&self) -> bool {
        match self {
            RelSide::Open(r) => r.rd_rel.relkind == types_tuple::access::RELKIND_PARTITIONED_TABLE,
            RelSide::Trigger(t) => {
                backend_commands_trigger_seams::tg_relation_is_partitioned::call(*t)
            }
        }
    }

    /// `RelationGetForm(rel)->relowner`.
    pub fn owner(&self) -> Oid {
        match self {
            RelSide::Open(r) => r.rd_rel.relowner,
            RelSide::Trigger(t) => backend_commands_trigger_seams::tg_relation_owner::call(*t),
        }
    }

    /// `RIAttName(rel, attnum)` = `NameStr(*attnumAttName(rel, attnum))` — raw
    /// name bytes. RI only ever passes user-column attnums (`> 0`).
    pub fn att_name(&self, mcx: Mcx<'_>, attnum: i16) -> PgResult<Vec<u8>> {
        match self {
            RelSide::Open(r) => {
                debug_assert!(attnum > 0, "RI passes only user attnums");
                let att = r.rd_att.attr((attnum - 1) as usize);
                Ok(att.attname.name_str().to_vec())
            }
            RelSide::Trigger(t) => Ok(
                backend_commands_trigger_seams::tg_relation_att_name::call(mcx, *t, attnum)?
                    .to_vec(),
            ),
        }
    }

    /// `RIAttType(rel, attnum)` = `attnumTypeId(rel, attnum)`.
    pub fn att_type(&self, attnum: i16) -> Oid {
        match self {
            RelSide::Open(r) => r.rd_att.attr((attnum - 1) as usize).atttypid,
            RelSide::Trigger(t) => {
                backend_commands_trigger_seams::tg_relation_att_type::call(*t, attnum)
            }
        }
    }

    /// `RIAttCollation(rel, attnum)` = `attnumCollationId(rel, attnum)`.
    pub fn att_collation(&self, attnum: i16) -> Oid {
        match self {
            RelSide::Open(r) => r.rd_att.attr((attnum - 1) as usize).attcollation,
            RelSide::Trigger(t) => {
                backend_commands_trigger_seams::tg_relation_att_collation::call(*t, attnum)
            }
        }
    }
}

extern crate alloc;
use alloc::vec::Vec;

// ---------------------------------------------------------------------------
// Local definitions (ri_triggers.c head-of-file #defines)
// ---------------------------------------------------------------------------

/// `RI_MAX_NUMKEYS` == `INDEX_MAX_KEYS` (32).
pub const RI_MAX_NUMKEYS: usize = types_core::fmgr::INDEX_MAX_KEYS as usize;

/// `RI_INIT_CONSTRAINTHASHSIZE`.
pub const RI_INIT_CONSTRAINTHASHSIZE: usize = 64;
/// `RI_INIT_QUERYHASHSIZE`.
pub const RI_INIT_QUERYHASHSIZE: usize = RI_INIT_CONSTRAINTHASHSIZE * 4;

/// `ri_NullCheck` result: every key column is NULL.
pub const RI_KEYS_ALL_NULL: i32 = 0;
/// `ri_NullCheck` result: some (but not all) key columns are NULL.
pub const RI_KEYS_SOME_NULL: i32 = 1;
/// `ri_NullCheck` result: no key column is NULL.
pub const RI_KEYS_NONE_NULL: i32 = 2;

// RI query type codes (RI_PLAN_XXX) -----------------------------------------
pub const RI_PLAN_CHECK_LOOKUPPK: i32 = 1;
pub const RI_PLAN_CHECK_LOOKUPPK_FROM_PK: i32 = 2;
pub const RI_PLAN_LAST_ON_PK: i32 = RI_PLAN_CHECK_LOOKUPPK_FROM_PK;
pub const RI_PLAN_CASCADE_ONDELETE: i32 = 3;
pub const RI_PLAN_CASCADE_ONUPDATE: i32 = 4;
pub const RI_PLAN_NO_ACTION: i32 = 5;
pub const RI_PLAN_RESTRICT: i32 = 6;
pub const RI_PLAN_SETNULL_ONDELETE: i32 = 7;
pub const RI_PLAN_SETNULL_ONUPDATE: i32 = 8;
pub const RI_PLAN_SETDEFAULT_ONDELETE: i32 = 9;
pub const RI_PLAN_SETDEFAULT_ONUPDATE: i32 = 10;

/// `MAX_QUOTED_NAME_LEN` == `NAMEDATALEN*2+3`.
pub const MAX_QUOTED_NAME_LEN: usize = (types_core::fmgr::NAMEDATALEN as usize) * 2 + 3;
/// `MAX_QUOTED_REL_NAME_LEN` == `MAX_QUOTED_NAME_LEN*2`.
pub const MAX_QUOTED_REL_NAME_LEN: usize = MAX_QUOTED_NAME_LEN * 2;

pub const RI_TRIGTYPE_INSERT: i32 = 1;
pub const RI_TRIGTYPE_UPDATE: i32 = 2;
pub const RI_TRIGTYPE_DELETE: i32 = 3;

// FK constraint match-type chars (nodes/parsenodes.h) -----------------------
pub const FKCONSTR_MATCH_FULL: i8 = b'f' as i8;
pub const FKCONSTR_MATCH_PARTIAL: i8 = b'p' as i8;
pub const FKCONSTR_MATCH_SIMPLE: i8 = b's' as i8;

/// `CONSTRAINT_FOREIGN` (catalog/pg_constraint.h) — `'f'`.
pub const CONSTRAINT_FOREIGN: i8 = b'f' as i8;

// RI trigger classification (commands/trigger.h) ----------------------------
pub const RI_TRIGGER_PK: i32 = 1;
pub const RI_TRIGGER_FK: i32 = 2;
pub const RI_TRIGGER_NONE: i32 = 0;

// Built-in function OIDs of the RI trigger procs (fmgroids.h) ---------------
pub const F_RI_FKEY_CHECK_INS: Oid = 1644;
pub const F_RI_FKEY_CHECK_UPD: Oid = 1645;
pub const F_RI_FKEY_CASCADE_DEL: Oid = 1646;
pub const F_RI_FKEY_CASCADE_UPD: Oid = 1647;
pub const F_RI_FKEY_RESTRICT_DEL: Oid = 1648;
pub const F_RI_FKEY_RESTRICT_UPD: Oid = 1649;
pub const F_RI_FKEY_SETNULL_DEL: Oid = 1650;
pub const F_RI_FKEY_SETNULL_UPD: Oid = 1651;
pub const F_RI_FKEY_SETDEFAULT_DEL: Oid = 1652;
pub const F_RI_FKEY_SETDEFAULT_UPD: Oid = 1653;
pub const F_RI_FKEY_NOACTION_DEL: Oid = 1654;
pub const F_RI_FKEY_NOACTION_UPD: Oid = 1655;

// Security-context flags (utils/acl.h / miscadmin.h) ------------------------
pub const SECURITY_LOCAL_USERID_CHANGE: i32 = 0x0001;
pub const SECURITY_NOFORCE_RLS: i32 = 0x0004;

// SPI status codes (executor/spi.h) -----------------------------------------
pub const SPI_OK_FINISH: i32 = 2;
pub const SPI_OK_SELECT: i32 = 5;
pub const SPI_OK_DELETE: i32 = 8;
pub const SPI_OK_UPDATE: i32 = 9;

// Trigger-event bit tests (commands/trigger.h) ------------------------------
pub const TRIGGER_EVENT_INSERT: u32 = 0x0000;
pub const TRIGGER_EVENT_DELETE: u32 = 0x0001;
pub const TRIGGER_EVENT_UPDATE: u32 = 0x0002;
pub const TRIGGER_EVENT_TRUNCATE: u32 = 0x0003;
pub const TRIGGER_EVENT_OPMASK: u32 = 0x0003;
pub const TRIGGER_EVENT_ROW: u32 = 0x0004;
pub const TRIGGER_EVENT_BEFORE: u32 = 0x0008;
pub const TRIGGER_EVENT_AFTER: u32 = 0x0000;
pub const TRIGGER_EVENT_INSTEAD: u32 = 0x0010;
pub const TRIGGER_EVENT_TIMINGMASK: u32 = 0x0018;

/// `ANYMULTIRANGEOID` (`catalog/pg_type.h`) — pseudotype OID for the temporal
/// FK `range_agg` parameter.
pub const ANYMULTIRANGEOID: Oid = 4537;

pub fn trigger_fired_by_insert(event: u32) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_INSERT
}
pub fn trigger_fired_by_update(event: u32) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_UPDATE
}
pub fn trigger_fired_by_delete(event: u32) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_DELETE
}
pub fn trigger_fired_after(event: u32) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_AFTER
}
pub fn trigger_fired_for_row(event: u32) -> bool {
    (event & TRIGGER_EVENT_ROW) != 0
}

// ---------------------------------------------------------------------------
// RI_ConstraintInfo  (matches ri_triggers.c's typedef)
// ---------------------------------------------------------------------------

/// Information extracted from an FK `pg_constraint` entry; cached in
/// `ri_constraint_cache`. Field-for-field equivalent to `RI_ConstraintInfo`.
#[derive(Clone, Copy, Debug)]
pub struct RiConstraintInfo {
    pub constraint_id: Oid,
    pub valid: bool,
    pub constraint_root_id: Oid,
    pub oid_hash_value: u32,
    pub root_hash_value: u32,
    pub conname: NameData,
    pub pk_relid: Oid,
    pub fk_relid: Oid,
    pub confupdtype: i8,
    pub confdeltype: i8,
    pub ndelsetcols: i32,
    pub confdelsetcols: [i16; RI_MAX_NUMKEYS],
    pub confmatchtype: i8,
    pub hasperiod: bool,
    pub nkeys: i32,
    pub pk_attnums: [i16; RI_MAX_NUMKEYS],
    pub fk_attnums: [i16; RI_MAX_NUMKEYS],
    pub pf_eq_oprs: [Oid; RI_MAX_NUMKEYS],
    pub pp_eq_oprs: [Oid; RI_MAX_NUMKEYS],
    pub ff_eq_oprs: [Oid; RI_MAX_NUMKEYS],
    pub period_contained_by_oper: Oid,
    pub agged_period_contained_by_oper: Oid,
    pub period_intersect_oper: Oid,
}

impl RiConstraintInfo {
    pub fn new(constraint_id: Oid) -> Self {
        RiConstraintInfo {
            constraint_id,
            valid: false,
            constraint_root_id: InvalidOid,
            oid_hash_value: 0,
            root_hash_value: 0,
            conname: NameData::default(),
            pk_relid: InvalidOid,
            fk_relid: InvalidOid,
            confupdtype: 0,
            confdeltype: 0,
            ndelsetcols: 0,
            confdelsetcols: [0; RI_MAX_NUMKEYS],
            confmatchtype: 0,
            hasperiod: false,
            nkeys: 0,
            pk_attnums: [0; RI_MAX_NUMKEYS],
            fk_attnums: [0; RI_MAX_NUMKEYS],
            pf_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
            pp_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
            ff_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
            period_contained_by_oper: InvalidOid,
            agged_period_contained_by_oper: InvalidOid,
            period_intersect_oper: InvalidOid,
        }
    }

    /// `NameStr(riinfo->conname)` decoded UTF-8-lossy for message formatting.
    pub fn conname_str(&self) -> alloc::string::String {
        alloc::string::String::from_utf8_lossy(self.conname.name_str()).into_owned()
    }
}

/// `RI_QueryKey` — the key identifying a prepared SPI plan in `ri_query_cache`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RiQueryKey {
    pub constr_id: Oid,
    pub constr_queryno: i32,
}

/// Build a [`NameData`] from raw name bytes (truncating/padding to NAMEDATALEN),
/// mirroring `memcpy(&riinfo->conname, &conForm->conname, sizeof(NameData))`.
pub(crate) fn name_data_from_bytes(bytes: &[u8]) -> NameData {
    let mut nd = NameData::default();
    let n = bytes.len().min(nd.data.len());
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// Install every seam this crate owns: the `pg_constraint` invalidation
/// callback the catalog-cache owner fires through `-seams`.
pub fn init_seams() {
    backend_utils_adt_ri_triggers_seams::invalidate_constraint_cache_callback::set(
        cache::invalidate_constraint_cache_callback,
    );
    // Register the RI trigger procs into the fmgr builtin table so that the
    // FK-enforcing triggers dispatch to their bodies (C: their `fmgr_builtins[]`
    // rows). Must run before any FK trigger fires.
    fmgr_builtins::register_ri_builtins();
}
