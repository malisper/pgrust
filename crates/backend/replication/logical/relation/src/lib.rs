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
// - logicalrep_partition_open builds the per-partition entry on every call
//   (C caches it in LogicalRepPartMap); correctness-identical, the cache is
//   a later perf item.
// - FindUsableIndexForReplicaIdentityFull's amgettuple requirement is read
//   off this port's index AM set: btree and hash serve index_getnext_slot;
//   every other AM here is bitmap-only (C's gist/spgist amgettuple are not
//   scan-capable in this port), so those indexes are not picked.
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

// GetRelationIdentityOrPK (relation.c:891): the replica identity index, else
// the primary key through RelationGetPrimaryKeyIndex(rel, false)
// (relcache.c:5049), which refuses a DEFERRABLE primary key.
pub fn get_relation_identity_or_pk(mcx: Mcx<'_>, rel: &Relation<'_>) -> PgResult<Oid> {
    relcache::indexlist::RelationGetIdentityOrPkIndex(mcx, rel.rd_id)
}

// FindUsableIndexForReplicaIdentityFull (relation.c:776): the first index of
// the local relation IsIndexUsableForReplicaIdentityFull accepts, else
// InvalidOid. Called for a REPLICA IDENTITY FULL remote relation.
fn find_usable_index_for_replica_identity_full(
    mcx: Mcx<'_>,
    localrel: &Relation<'_>,
    attrmap: &[i16],
) -> PgResult<Oid> {
    let idxlist = relcache::indexlist::RelationGetIndexList(mcx, localrel.rd_id)?;
    for &idxoid in idxlist.iter() {
        let idxrel = indexam::index_open(mcx, idxoid, types_rel::AccessShareLock)?;
        let usable = is_index_usable_for_replica_identity_full(&idxrel, attrmap)?;
        indexam::index_close(idxrel, types_rel::AccessShareLock)?;

        // Return the first eligible index found.
        if usable {
            return Ok(idxoid);
        }
    }
    Ok(InvalidOid)
}

// IsIndexUsableForReplicaIdentityFull (relation.c:814): the index must have
// an equal strategy for each key column, be non-partial, and the leftmost
// field must be a column (not an expression) that references a remote
// relation column; every index attribute type must have a type-cache
// equality operator (tuples_equal rechecks non-PK/RI matches); and the AM
// must implement amgettuple.
pub fn is_index_usable_for_replica_identity_full(
    idxrel: &Relation<'_>,
    attrmap: &[i16],
) -> PgResult<bool> {
    let form = idxrel.rd_index.as_ref().expect("index form");

    // The index must not be a partial index.
    if form.has_indpred {
        return Ok(false);
    }

    debug_assert!(form.indnatts >= 1);

    // Ensure that the index has a valid equal strategy for each key column
    // (rd_opfamily[i] is get_opclass_family(indclass->values[i])).
    for i in 0..form.indnkeyatts as usize {
        let opfamily = idxrel.rd_opfamily[i];
        if amapi::IndexAmTranslateCompareType(
            lsyscache::COMPARE_EQ,
            idxrel.rd_rel.relam,
            opfamily,
            true,
        )? == types_scan::scankey::InvalidStrategy
        {
            return Ok(false);
        }
    }

    // For indexes other than PK and REPLICA IDENTITY, we need to match the
    // local and remote tuples. The equality routine tuples_equal() cannot
    // accept a data type where the type cache cannot provide an equality
    // operator.
    for i in 0..idxrel.rd_att.natts as usize {
        let typentry = typcache::lookup_type_cache(
            idxrel.rd_att.attr(i).atttypid,
            typcache::TYPECACHE_EQ_OPR_FINFO,
        )?;
        if typentry.eq_opr_finfo().fn_oid == InvalidOid {
            return Ok(false);
        }
    }

    // The leftmost index field must not be an expression.
    let keycol = form.indkey0();
    if keycol == types_core::InvalidAttrNumber {
        return Ok(false);
    }

    // And the leftmost index field must reference the remote relation
    // column: if it doesn't, the sequential scan is favorable over the index
    // scan in most cases.
    let off = (keycol - 1) as usize;
    if attrmap.len() <= off || attrmap[off] < 0 {
        return Ok(false);
    }

    // The given index access method must implement "amgettuple", which will
    // be used later to fetch the tuples (RelationFindReplTupleByIndex): in
    // this port that is the btree and hash AMs (indexam::am_gettuple).
    let kind = amapi::GetIndexAmRoutineByAmId(idxrel.rd_rel.relam, false)?
        .expect("noerror=false returned Some");
    if !matches!(kind, types_relscan::IndexAmKind::Btree | types_relscan::IndexAmKind::Hash) {
        return Ok(false);
    }

    Ok(true)
}

// FindLogicalRepLocalIndex (relation.c:908). A partitioned table never needs
// an index (the leaf partition's is used); otherwise the local replica
// identity or (non-deferrable) primary key is used whatever the remote
// replica identity is, and a REPLICA IDENTITY FULL publisher gets one more
// opportunity: any usable index of the local relation
// (FindUsableIndexForReplicaIdentityFull). Without one the apply worker
// sequential-scans.
fn find_local_index(
    mcx: Mcx<'_>,
    localrel: &Relation<'_>,
    remoterel: &LogicalRepRelation,
    attrmap: &[i16],
) -> PgResult<Oid> {
    const REPLICA_IDENTITY_FULL: u8 = b'f';

    // We never need index oid for partitioned tables, always rely on leaf
    // partition's index.
    if localrel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        return Ok(InvalidOid);
    }

    // Simple case, we already have a primary key or a replica identity index.
    let idxoid = get_relation_identity_or_pk(mcx, localrel)?;
    if idxoid != InvalidOid {
        return Ok(idxoid);
    }

    if remoterel.replident == REPLICA_IDENTITY_FULL {
        // We are looking for one more opportunity for using an index. If
        // there are any indexes defined on the local relation, try to pick a
        // suitable index. The index selection safely assumes that all the
        // columns are going to be available for the index scan given that
        // remote relation has replica identity full.
        return find_usable_index_for_replica_identity_full(mcx, localrel, attrmap);
    }

    Ok(InvalidOid)
}

// logicalrep_get_attrs_str (relation.c:227): the named remote columns,
// double-quoted, comma-separated.
fn get_attrs_str(remoterel: &LogicalRepRelation, atts: &[usize]) -> String {
    atts.iter()
        .map(|&i| format!("\"{}\"", remoterel.attnames[i]))
        .collect::<Vec<_>>()
        .join(", ")
}

// logicalrep_report_missing_or_gen_attrs (relation.c:255): `missing` and
// `generated` are remote column indexes; missing columns are reported first
// (errmsg_plural on the count).
fn report_missing_or_gen_attrs(
    remoterel: &LogicalRepRelation,
    missing: &[usize],
    generated: &[usize],
) -> PgResult<()> {
    if !missing.is_empty() {
        let column = if missing.len() == 1 { "column" } else { "columns" };
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "logical replication target relation \"{}.{}\" is missing replicated {column}: {}",
                remoterel.nspname,
                remoterel.relname,
                get_attrs_str(remoterel, missing)
            ))
            .finish(loc("logicalrep_report_missing_or_gen_attrs"))?;
        unreachable!();
    }
    if !generated.is_empty() {
        let column = if generated.len() == 1 { "column" } else { "columns" };
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "logical replication target relation \"{}.{}\" has incompatible generated {column}: {}",
                remoterel.nspname,
                remoterel.relname,
                get_attrs_str(remoterel, generated)
            ))
            .finish(loc("logicalrep_report_missing_or_gen_attrs"))?;
        unreachable!();
    }
    Ok(())
}

// CheckSubscriptionRelkind (execReplication.c:877-886): plain and partitioned
// tables are valid logical replication targets; the DETAIL is
// errdetail_relkind_not_supported(relkind).
pub fn check_relkind(relkind: u8, nspname: &str, relname: &str) -> PgResult<()> {
    if relkind != b'r' && relkind != b'p' {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
        ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot use relation \"{nspname}.{relname}\" as logical replication target"
            ))
            .errdetail(detail)
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
        let mut generated_hit: Vec<usize> = Vec::new();
        for i in 0..natts {
            let attr = desc.attr(i);
            if attr.attisdropped {
                continue;
            }
            // C logicalrep_rel_open treats NameStr(attname) as opaque bytes
            // (strcmp), never requiring UTF-8; a SQL_ASCII subscriber catalog
            // can hold non-UTF-8 attnames, so decode lossily instead of aborting
            // the apply worker (which would crash-loop the whole instance).
            let attname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
            let m = rel_att_by_name(&remoterel, &attname);
            entry.attrmap[i] = m;
            if m >= 0 {
                if attr.attgenerated != 0 {
                    generated_hit.push(m as usize);
                }
                missing[m as usize] = false;
            }
        }

        let missing_idx: Vec<usize> = missing
            .iter()
            .enumerate()
            .filter(|(_, &miss)| miss)
            .map(|(i, _)| i)
            .collect();
        report_missing_or_gen_attrs(&remoterel, &missing_idx, &generated_hit)?;

        mark_updatable(&mut entry)?;
        entry.localindexoid = find_local_index(mcx, &rel, &entry.remoterel, &entry.attrmap)?;
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

// logicalrep_partition_open (relation.c:633): the tuple-routing entry for a
// leaf partition of a partitioned apply target. `map` is the root->partition
// attrmap from the routing machinery (attnums indexed by 0-based partition
// attno holding the 1-based root attno, 0 = dropped); the produced entry maps
// 0-based partition attnos to remote column indexes like every other entry.
// Rendering: built per call — C caches entries in LogicalRepPartMap, a perf
// difference only (see module header).
pub fn logicalrep_partition_open(
    mcx: Mcx<'_>,
    root: &LogicalRepRelMapEntry,
    partrel: &Relation<'_>,
    map: Option<&[i16]>,
) -> PgResult<LogicalRepRelMapEntry> {
    let remoterel = root.remoterel.clone();
    let attrmap = match map {
        Some(map) => map
            .iter()
            .map(|&root_attno| {
                if root_attno == 0 {
                    -1
                } else {
                    root.attrmap.get((root_attno - 1) as usize).copied().unwrap_or(-1)
                }
            })
            .collect(),
        None => root.attrmap.clone(),
    };
    let mut entry = LogicalRepRelMapEntry {
        remoterel,
        localreloid: partrel.rd_id,
        localrelvalid: true,
        attrmap,
        updatable: false,
        localindexoid: InvalidOid,
        // state/statelsn stay 0 as in C.
        state: 0,
        statelsn: InvalidXLogRecPtr,
    };
    mark_updatable(&mut entry)?;
    entry.localindexoid = find_local_index(mcx, partrel, &entry.remoterel, &entry.attrmap)?;
    Ok(entry)
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

    // ---- audit-remediation b060 witnesses -----------------------------------

    // CheckSubscriptionRelkind (execReplication.c:877): the refusal carries
    // errdetail_relkind_not_supported (row
    // a186-candidate-fp-logical-relation-3716aede9a0956c83125-1).
    // errdetail_relkind_not_supported lives behind pg_class_seams (bound by
    // pg_class at boot); the unit test binds a C-shaped fake once.
    fn fake_errdetail_relkind_not_supported(relkind: u8) -> PgResult<String> {
        let kind = match relkind {
            b'v' => "views",
            b'f' => "foreign tables",
            other => panic!("unexpected relkind {other:?}"),
        };
        Ok(format!("This operation is not supported for {kind}."))
    }

    #[test]
    fn relkind_refusal_carries_c_detail() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            pg_class_seams::errdetail_relkind_not_supported::set(
                fake_errdetail_relkind_not_supported,
            );
        });
        let err = check_relkind(b'v', "public", "t").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
        assert_eq!(
            err.message(),
            "cannot use relation \"public.t\" as logical replication target"
        );
        assert_eq!(err.detail(), Some("This operation is not supported for views."));
        let err = check_relkind(b'f', "s", "ft").unwrap_err();
        assert_eq!(err.detail(), Some("This operation is not supported for foreign tables."));
        assert!(check_relkind(b'r', "public", "t").is_ok());
        assert!(check_relkind(b'p', "public", "t").is_ok());
    }

    // logicalrep_report_missing_or_gen_attrs (relation.c:255): errmsg_plural
    // forms, double-quoted names from logicalrep_get_attrs_str, missing
    // reported before generated (row
    // a186-candidate-fp-logical-relation-3e173f87971875b5f605-1).
    #[test]
    fn missing_and_generated_attrs_messages_match_c() {
        let r = remoterel();
        assert!(report_missing_or_gen_attrs(&r, &[], &[]).is_ok());
        let err = report_missing_or_gen_attrs(&r, &[1], &[]).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        assert_eq!(
            err.message(),
            "logical replication target relation \"public.t\" is missing replicated column: \"b\""
        );
        let err = report_missing_or_gen_attrs(&r, &[0, 1], &[]).unwrap_err();
        assert_eq!(
            err.message(),
            "logical replication target relation \"public.t\" is missing replicated columns: \"a\", \"b\""
        );
        let err = report_missing_or_gen_attrs(&r, &[], &[0]).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        assert_eq!(
            err.message(),
            "logical replication target relation \"public.t\" has incompatible generated column: \"a\""
        );
        let err = report_missing_or_gen_attrs(&r, &[], &[0, 1]).unwrap_err();
        assert_eq!(
            err.message(),
            "logical replication target relation \"public.t\" has incompatible generated columns: \"a\", \"b\""
        );
        // Missing columns are reported first when both occur.
        let err = report_missing_or_gen_attrs(&r, &[1], &[0]).unwrap_err();
        assert_eq!(
            err.message(),
            "logical replication target relation \"public.t\" is missing replicated column: \"b\""
        );
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
