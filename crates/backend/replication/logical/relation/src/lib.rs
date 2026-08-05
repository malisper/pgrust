// relation.c (replication/logical): the remote-relation map — remote relid ->
// local relation, attribute-name attrmap, replica-identity updatability, and
// the usable local index for UPDATE/DELETE key lookup.
//
// Renderings vs C:
// - The map is a thread-local HashMap (C: HTAB in a process-private context);
//   an apply worker is one thread, so per-worker state is thread-local.
// - Entries hold metadata only; the local Relation is opened per
//   logicalrep_rel_open call and returned to the caller (C caches the open
//   Relation pointer in the entry; the pgrust Relation is an arena-lifetime
//   handle that cannot live in a 'static map).
// - Invalidation: a relcache callback marks entries invalid by local reloid
//   (logicalrep_relmap_invalidate_cb), C's granularity.
// - Partitioned targets refuse loudly; FindUsableIndexForReplicaIdentityFull
//   is out of scope, so REPLICA IDENTITY FULL uses the sequential-scan path
//   (which C also falls back to).
#![allow(non_snake_case)]

use std::cell::RefCell;
use std::collections::HashMap;

use datum::Datum;
use elog::ereport;
use logicalproto::{LogicalRepRelId, LogicalRepRelation};
use mcx::Mcx;
use types_core::{InvalidOid, InvalidXLogRecPtr, Oid, XLogRecPtr};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_rel::{Relation, LOCKMODE};

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

pub const SUBREL_STATE_READY: u8 = b'r';

// LogicalRepRelMapEntry (logicalrelation.h), metadata half.
#[derive(Clone)]
pub struct LogicalRepRelMapEntry {
    pub remoterel: LogicalRepRelation,
    pub localreloid: Oid,
    pub localrelvalid: bool,
    // Local attribute offset (0-based) -> remote column index, or -1.
    pub attrmap: Vec<i16>,
    pub updatable: bool,
    // Replica-identity (or PK) index for FindReplTupleInLocalRel;
    // InvalidOid = sequential scan.
    pub localindexoid: Oid,
    pub state: u8,
    pub statelsn: XLogRecPtr,
}

thread_local! {
    static REL_MAP: RefCell<HashMap<LogicalRepRelId, LogicalRepRelMapEntry>> =
        RefCell::new(HashMap::new());
}

// logicalrep_relmap_update (relation.c:164).
pub fn logicalrep_relmap_update(remoterel: &LogicalRepRelation) {
    REL_MAP.with(|m| {
        m.borrow_mut().insert(
            remoterel.remoteid,
            LogicalRepRelMapEntry {
                remoterel: remoterel.clone(),
                localreloid: InvalidOid,
                localrelvalid: false,
                attrmap: Vec::new(),
                updatable: false,
                localindexoid: InvalidOid,
                state: 0,
                statelsn: InvalidXLogRecPtr,
            },
        );
    });
}

// logicalrep_relmap_invalidate_cb (relation.c:64): InvalidOid = all entries.
fn relmap_invalidate_cb(_arg: Datum, reloid: Oid) {
    REL_MAP.with(|m| {
        for e in m.borrow_mut().values_mut() {
            if reloid == InvalidOid || e.localreloid == reloid {
                e.localrelvalid = false;
            }
        }
    });
}

// logicalrep_rel_att_by_name (relation.c:250).
fn rel_att_by_name(remoterel: &LogicalRepRelation, attname: &str) -> i16 {
    for (i, name) in remoterel.attnames.iter().enumerate() {
        if name == attname {
            return i as i16;
        }
    }
    -1
}

// logicalrep_rel_mark_updatable (relation.c:296).
fn mark_updatable(entry: &mut LogicalRepRelMapEntry) -> PgResult<()> {
    const REPLICA_IDENTITY_FULL: u8 = b'f';

    entry.updatable = true;

    let bitmaps = relcache::indexattr::RelationGetIndexAttrBitmap(entry.localreloid)?;
    let idkey: &[i16] = if !bitmaps.identity.is_empty() {
        &bitmaps.identity
    } else if !bitmaps.pk.is_empty() {
        // Fall back to PK if no replica identity.
        &bitmaps.pk
    } else {
        // Without a replica-identity index or PK, the published table must
        // have replica identity FULL to be updatable.
        if entry.remoterel.replident != REPLICA_IDENTITY_FULL {
            entry.updatable = false;
        }
        return Ok(());
    };

    for &attnum in idkey {
        // pgrust bitmaps carry plain user attnums (1-based).
        let off = (attnum - 1) as usize;
        let remote = entry.attrmap.get(off).copied().unwrap_or(-1);
        if remote < 0
            || !entry.remoterel.attkeys.get(remote as usize).copied().unwrap_or(false)
        {
            entry.updatable = false;
            break;
        }
    }
    Ok(())
}

// FindLogicalRepLocalIndex (relation.c:832), replident/PK subset.
fn find_local_index(rel: &Relation<'_>, remoterel: &LogicalRepRelation) -> Oid {
    const REPLICA_IDENTITY_FULL: u8 = b'f';
    if remoterel.replident == REPLICA_IDENTITY_FULL {
        return InvalidOid;
    }
    let (pk, replident) = rel
        .rd_indexlist
        .borrow()
        .as_ref()
        .map(|l| (l.pkindex, l.replidindex))
        .unwrap_or((InvalidOid, InvalidOid));
    if replident != InvalidOid {
        replident
    } else {
        pk
    }
}

fn check_relkind(relkind: u8, nspname: &str, relname: &str) -> PgResult<()> {
    if relkind == b'p' {
        panic!("unported: partitioned apply target \"{nspname}.{relname}\"");
    }
    if relkind != b'r' {
        ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot use relation \"{nspname}.{relname}\" as logical replication target"
            ))
            .finish(loc("logicalrep_rel_open"))?;
    }
    Ok(())
}

// logicalrep_rel_open (relation.c:349): returns the (possibly rebuilt) entry
// metadata plus the opened+locked local relation.
pub fn logicalrep_rel_open<'mcx>(
    mcx: Mcx<'mcx>,
    remoteid: LogicalRepRelId,
    lockmode: LOCKMODE,
    subid: Oid,
) -> PgResult<(LogicalRepRelMapEntry, Relation<'mcx>)> {
    let Some(mut entry) = REL_MAP.with(|m| m.borrow().get(&remoteid).cloned()) else {
        return Err(Box::new(PgError::error(format!(
            "no relation map entry for remote relation ID {remoteid}"
        ))));
    };

    let mut localrel: Option<Relation<'mcx>> = None;

    // Valid entry: reopen by OID; pending invalidations may flip validity.
    if entry.localrelvalid {
        match table::try_table_open(mcx, entry.localreloid, lockmode)? {
            Some(rel) => {
                let still_valid = REL_MAP
                    .with(|m| m.borrow().get(&remoteid).map(|e| e.localrelvalid))
                    .unwrap_or(false);
                if still_valid {
                    localrel = Some(rel);
                } else {
                    // Note: release the no-longer-useful lock here.
                    table::table_close(rel, lockmode)?;
                    entry.localrelvalid = false;
                }
            }
            None => entry.localrelvalid = false, // renamed or dropped
        }
    }

    if !entry.localrelvalid {
        let remoterel = entry.remoterel.clone();
        let rv = rel_vocab::RangeVar {
            catalogname: None,
            schemaname: Some(remoterel.nspname.as_str()),
            relname: remoterel.relname.as_str(),
            inh: true,
            relpersistence: b'p',
            location: -1,
        };
        let relid = catalog_namespace::RangeVarGetRelid(&rv, lockmode, true)?;
        if relid == InvalidOid {
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "logical replication target relation \"{}.{}\" does not exist",
                    remoterel.nspname, remoterel.relname
                ))
                .finish(loc("logicalrep_rel_open"))?;
            unreachable!();
        }
        let rel = table::table_open(mcx, relid, types_rel::NoLock)?;
        entry.localreloid = relid;

        check_relkind(rel.rd_rel.relkind as u8, &remoterel.nspname, &remoterel.relname)?;

        // Local-offset -> remote-column attrmap by column name; track remote
        // columns with no local counterpart and local generated columns
        // targeted by the remote side (both are errors, relation.c:207).
        let desc = &rel.rd_att;
        let natts = desc.natts as usize;
        entry.attrmap = vec![-1i16; natts];
        let mut missing: Vec<bool> = vec![true; remoterel.natts];
        let mut generated_hit: Vec<String> = Vec::new();
        for i in 0..natts {
            let attr = desc.attr(i);
            if attr.attisdropped {
                continue;
            }
            let attname = std::str::from_utf8(attr.attname.name_str())
                .expect("attname utf8")
                .to_string();
            let m = rel_att_by_name(&remoterel, &attname);
            entry.attrmap[i] = m;
            if m >= 0 {
                if attr.attgenerated != 0 {
                    generated_hit.push(attname);
                }
                missing[m as usize] = false;
            }
        }

        let missing_names: Vec<&str> = missing
            .iter()
            .enumerate()
            .filter(|(_, &miss)| miss)
            .map(|(i, _)| remoterel.attnames[i].as_str())
            .collect();
        if !missing_names.is_empty() || !generated_hit.is_empty() {
            let mut parts = Vec::new();
            if !missing_names.is_empty() {
                parts.push(format!("missing replicated columns: ({})", missing_names.join(", ")));
            }
            if !generated_hit.is_empty() {
                parts.push(format!("generated columns: ({})", generated_hit.join(", ")));
            }
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "logical replication target relation \"{}.{}\" is misconfigured: {}",
                    remoterel.nspname,
                    remoterel.relname,
                    parts.join("; ")
                ))
                .finish(loc("logicalrep_report_missing_or_gen_attrs"))?;
            unreachable!();
        }

        mark_updatable(&mut entry)?;
        entry.localindexoid = find_local_index(&rel, &remoterel);
        entry.localrelvalid = true;
        localrel = Some(rel);
    }

    if entry.state != SUBREL_STATE_READY {
        let (state, lsn) =
            pg_subscription::GetSubscriptionRelState(mcx, subid, entry.localreloid)?;
        entry.state = state;
        entry.statelsn = lsn;
    }

    REL_MAP.with(|m| {
        m.borrow_mut().insert(remoteid, entry.clone());
    });

    Ok((entry, localrel.expect("logicalrep_rel_open produced a relation")))
}

// logicalrep_rel_close (relation.c:504).
pub fn logicalrep_rel_close(rel: Relation<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    table::table_close(rel, lockmode)
}

// logicalrep_relmap_init's callback registration (relation.c:117); called once
// per apply worker before the loop.
pub fn logicalrep_relmap_init() -> PgResult<()> {
    inval::invalidate::CacheRegisterRelcacheCallback(relmap_invalidate_cb, Datum::null())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remoterel() -> LogicalRepRelation {
        LogicalRepRelation {
            remoteid: 42,
            nspname: "public".into(),
            relname: "t".into(),
            natts: 2,
            attnames: vec!["a".into(), "b".into()],
            atttyps: vec![23, 25],
            replident: b'd',
            relkind: b'r',
            attkeys: vec![true, false],
        }
    }

    #[test]
    fn relmap_update_and_invalidate() {
        logicalrep_relmap_update(&remoterel());
        REL_MAP.with(|m| {
            let mut b = m.borrow_mut();
            let e = b.get_mut(&42).unwrap();
            assert!(!e.localrelvalid);
            e.localrelvalid = true;
            e.localreloid = 1000;
        });
        relmap_invalidate_cb(Datum::null(), 999); // different rel: untouched
        REL_MAP.with(|m| assert!(m.borrow()[&42].localrelvalid));
        relmap_invalidate_cb(Datum::null(), 1000);
        REL_MAP.with(|m| assert!(!m.borrow()[&42].localrelvalid));
        relmap_invalidate_cb(Datum::null(), InvalidOid); // all
        REL_MAP.with(|m| assert!(!m.borrow()[&42].localrelvalid));
    }

    #[test]
    fn att_by_name_maps_and_misses() {
        let r = remoterel();
        assert_eq!(rel_att_by_name(&r, "a"), 0);
        assert_eq!(rel_att_by_name(&r, "b"), 1);
        assert_eq!(rel_att_by_name(&r, "c"), -1);
    }
}

// pg_get_replica_identity_index (misc.c:1101): oid of the replica identity
// index, NULL when none. Publisher-side dependency of tablesync's
// fetch_remote_table_info query.
pub fn fc_pg_get_replica_identity_index(
    _flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let reloid = fcinfo.arg(0).as_oid();
    let mcx_owned = mcx::MemoryContext::new("pg_get_replica_identity_index");
    let mcx = mcx_owned.mcx();
    let rel = table::table_open(mcx, reloid, types_rel::AccessShareLock)?;
    // Populate rd_indexlist (computes replidindex incl. the DEFAULT->pkey rule).
    let _ = relcache::RelationGetIndexList(mcx, reloid)?;
    let idxoid = rel
        .rd_indexlist
        .borrow()
        .as_ref()
        .map(|l| l.replidindex)
        .unwrap_or(types_core::InvalidOid);
    rel.close(types_rel::AccessShareLock)?;
    if idxoid != types_core::InvalidOid {
        Ok(datum::Datum::from_oid(idxoid))
    } else {
        Ok(fcinfo.return_null())
    }
}

pub const LOGICALRELATION_BUILTINS: &[types_fmgr::FmgrBuiltin] = &[types_fmgr::FmgrBuiltin {
    foid: 6120,
    name: "pg_get_replica_identity_index",
    nargs: 1,
    strict: true,
    retset: false,
    func: fc_pg_get_replica_identity_index,
}];
