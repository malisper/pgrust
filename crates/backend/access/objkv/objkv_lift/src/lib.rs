//! One-time and one direction. C's initdb built these catalogs; afterwards the
//! bucket holds them and the local files are cache.
//!
//! One database's catalogs are one commit object -- a single conditional PUT
//! that exists whole or not at all -- so this only has to stage, and the crash
//! machinery is the one already built. Nothing local is written until the last
//! step: catalogs are read, never modified, and the only file produced is the
//! marker, by rename. So a crash here can leave the bucket wanting attention
//! and can never stop the database starting, and rolling back is deleting that
//! file. The window closes at the first write after the flip.
//!
//! A SQL function rather than a tool because it needs a backend with working
//! catalog access. It refuses to run with any other backend connected to any
//! database: shared catalogs belong to the cluster, so quiet-in-this-database
//! is not enough, and read-only traffic is not carved out because a rule with
//! a footnote gets applied wrongly at 3am.
//!
//! That count is a glance, not a door: a session can connect the moment after
//! it, and the lifts of a cluster's databases are separate sessions minutes
//! apart with nothing counting in between. A catalog row committed anywhere in
//! that span lands in a local file the bucket has already photographed, and
//! the flip loses it without a word. So each lift records the cluster's
//! transaction counter, which every local write advances and a lift never
//! does, and each later lift and the flip refuse if it has moved. Loss becomes
//! a refusal with the fix in the message.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::catalog::PG_CATALOG_NAMESPACE;
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::FmgrBuiltin;
use ::types_rel::Relation;
use ::types_storage::lock::AccessShareLock;

mod builtins;
pub use builtins::LIFT_BUILTINS;

const RELKIND_RELATION: u8 = b'r';

// pg_class/pg_database column numbers and oids, spelled out as vacuum does
// rather than imported: these are bootstrap facts, not catalog lookups.
const RelationRelationId: Oid = 1259;
const DatabaseRelationId: Oid = 1262;
const Anum_pg_class_oid: usize = 1;
const Anum_pg_class_relname: usize = 2;
const Anum_pg_class_relnamespace: usize = 3;
// Column numbers are the on-disk pg_class's, not the in-memory struct's,
// which omits reloftype and shifts everything after it. Writing the access
// method into column 6 sets relowner and leaves relam heap: the lift reports
// success and nothing is marked objkv.
const Anum_pg_class_relam: usize = 7;
const Anum_pg_class_relisshared: usize = 16;
const Anum_pg_class_relkind: usize = 18;
const Anum_pg_database_oid: usize = 1;
const Anum_pg_database_datname: usize = 2;
const Anum_pg_database_datallowconn: usize = 7;

fn getattr(
    tup: &::types_tuple::HeapTupleData<'_>,
    attnum: usize,
    desc: &::types_tuple::TupleDescData<'_>,
) -> Datum {
    let mut isnull = false;
    // SAFETY: a catalog row read under its own descriptor.
    let d = unsafe { ::types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

/// The row's own oid, for catalogs that have one. The high-water mark must
/// cover every lifted row carrying one, not just pg_class's: a function or
/// type created after the last table outranks every table, and a counter
/// restarted below it hands the number out twice. Postgres puts the oid column
/// first in every catalog that has one, so the descriptor answers this.
fn own_oid(
    tup: &::types_tuple::HeapTupleData<'_>,
    desc: &::types_tuple::TupleDescData<'_>,
) -> Option<u32> {
    let first = desc.attr(0);
    if first.atttypid != ::types_core::OIDOID || first.attname.name_str() != b"oid" {
        return None;
    }
    let mut isnull = false;
    // SAFETY: a catalog row read under its own descriptor.
    let d = unsafe { ::types_tuple::heap_getattr(tup, 1, desc, &mut isnull) };
    if isnull {
        return None;
    }
    Some(d.as_oid())
}

fn name_of(d: Datum) -> String {
    // SAFETY: a name datum addresses NameData.
    let n: ::types_tuple::NameData =
        unsafe { core::ptr::read_unaligned(d.as_usize() as *const ::types_tuple::NameData) };
    String::from_utf8_lossy(n.name_str()).into_owned()
}

/// pg_am's oid for objkv, by name: the AM is a catalog row, so there is no
/// compiled-in oid.
fn am_oid_named(mcx: Mcx<'_>, want: &str) -> PgResult<Oid> {
    const AccessMethodRelationId: Oid = 2601;
    const Anum_pg_am_oid: usize = 1;
    const Anum_pg_am_amname: usize = 2;
    let rel = ::table::table_open(mcx, AccessMethodRelationId, AccessShareLock)?;
    let desc = rel.descr();
    let mut found = InvalidOid;
    let mut scan = ::genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        if name_of(getattr(tup, Anum_pg_am_amname, desc)) == want {
            found = getattr(tup, Anum_pg_am_oid, desc).as_oid();
            break;
        }
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(rel, AccessShareLock)?;
    if found == InvalidOid {
        return Err(err(format!(
            "objkv lift: no access method named {want} in this database"
        )));
    }
    Ok(found)
}

fn objkv_am_oid(mcx: Mcx<'_>) -> PgResult<Oid> {
    am_oid_named(mcx, "objkv")
}

/// Both: a lifted catalog's indexes must be objkv indexes too, or a btree
/// relam sends the scan to a local file that no longer describes the truth.
fn ams_of(mcx: Mcx<'_>) -> PgResult<Ams> {
    Ok(Ams { table: am_oid_named(mcx, "objkv")?, index: am_oid_named(mcx, "objkv_btree")? })
}

fn record_key(scope: u32) -> Vec<u8> {
    format!("lift/{scope:08x}").into_bytes()
}

/// The largest oid any lift recorded; a fresh counter must start above it.
fn oid_high_water() -> PgResult<u32> {
    let mut high = 0u32;
    for scope in ::tableam::objkv_am::lift_records()? {
        high = high.max(record_field(&scope, "oid_high=").unwrap_or(0));
    }
    Ok(high)
}

/// One `name=value` field of a lift record.
fn record_field<T: std::str::FromStr>(record: &str, name: &str) -> Option<T> {
    record.split_whitespace().find_map(|f| f.strip_prefix(name)?.parse().ok())
}

/// The cluster's next transaction id, as a clock: every write to a local
/// heap file, catalog or not, takes an id and advances it; a lift takes none.
/// Equal readings mean nothing local changed between them. A restart between
/// the two keeps the reading, since the counter lives in the control file.
fn next_xid() -> PgResult<u64> {
    Ok(::varsup::ReadNextFullTransactionId()?.value)
}

/// The photographs are only as good as the moment they were taken. Any lift
/// whose reading differs from now is of catalogs the cluster has since
/// written to, and a flip on top of it would lose those writes.
fn refuse_if_written_since_lifts(now: u64) -> PgResult<()> {
    for (key, record) in ::tableam::objkv_am::lift_records_keyed()? {
        let Some(then) = record_field::<u64>(&record, "xid=") else { continue };
        if then != now {
            return Err(err(format!(
                "objkv lift: the cluster has written since {key} was lifted \
                 (transaction counter {then} then, {now} now), so that copy is stale. \
                 Clear the lift records in the bucket and lift every database again \
                 with nothing else connected."
            )));
        }
    }
    Ok(())
}

fn err(what: String) -> Box<PgError> {
    Box::new(PgError::error(what))
}

struct Target {
    relid: Oid,
    name: String,
    shared: bool,
}

fn targets(mcx: Mcx<'_>, shared: bool) -> PgResult<Vec<Target>> {
    let pgclass = ::table::table_open(mcx, RelationRelationId, AccessShareLock)?;
    let desc = pgclass.descr();
    let mut out = Vec::new();
    let mut scan = ::genam::systable_beginscan(mcx, &pgclass, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        let get = |n| getattr(tup, n, desc);
        if get(Anum_pg_class_relkind).as_u8() != RELKIND_RELATION {
            continue;
        }
        if get(Anum_pg_class_relnamespace).as_oid() != PG_CATALOG_NAMESPACE {
            continue;
        }
        if get(Anum_pg_class_relisshared).as_bool() != shared {
            continue;
        }
        out.push(Target {
            relid: get(Anum_pg_class_oid).as_oid(),
            name: name_of(get(Anum_pg_class_relname)),
            shared,
        });
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(pgclass, AccessShareLock)?;
    out.sort_by_key(|t| t.relid);
    Ok(out)
}

fn refuse_unless_alone(mcx: Mcx<'_>, what: &str) -> PgResult<()> {
    let mut others = 0i32;
    let pgdb = ::table::table_open(mcx, DatabaseRelationId, AccessShareLock)?;
    let desc = pgdb.descr();
    let mut scan = ::genam::systable_beginscan(mcx, &pgdb, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        let oid = getattr(tup, Anum_pg_database_oid, desc).as_oid();
        others += ::procarray::CountDBBackends(oid)?;
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(pgdb, AccessShareLock)?;
    if others > 1 {
        return Err(err(format!(
            "objkv lift: {} other backend(s) are connected; the {what} must be the only session in the cluster",
            others - 1
        )));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Ams {
    table: Oid,
    index: Oid,
}

/// its relkind, since the bucket copy is the one that will be read. Rewriting
/// an *index* row to the table access method makes the index claim to be a
/// table, and bootstrap fails with "could not open critical system index".
/// Rows with no access method -- views, sequences -- keep a relam of zero.
fn with_relam(
    mcx: Mcx<'_>,
    tup: &::types_tuple::HeapTupleData<'_>,
    desc: &::types_tuple::TupleDescData<'_>,
    ams: Ams,
) -> PgResult<Vec<u8>> {
    const RELKIND_INDEX: u8 = b'i';
    const RELKIND_MATVIEW: u8 = b'm';
    const RELKIND_PARTITIONED: u8 = b'p';
    const RELKIND_TOAST: u8 = b't';

    let relkind = getattr(tup, Anum_pg_class_relkind, desc).as_u8();
    let am = match relkind {
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_PARTITIONED | RELKIND_TOAST => ams.table,
        RELKIND_INDEX => ams.index,
        // Giving a view a table access method is how something tries to scan one.
        _ => return Ok(tup_image(mcx, tup, desc)?),
    };

    let natts = desc.natts as usize;
    let mut repl = vec![Datum::null(); natts];
    let isnull = vec![false; natts];
    let mut doit = vec![false; natts];
    repl[Anum_pg_class_relam - 1] = Datum::from_oid(am);
    doit[Anum_pg_class_relam - 1] = true;
    Ok(::heaptuple::heap_modify_tuple(mcx, tup, desc, &repl, &isnull, &doit)?
        .image()
        .to_vec())
}

fn tup_image(
    mcx: Mcx<'_>,
    tup: &::types_tuple::HeapTupleData<'_>,
    desc: &::types_tuple::TupleDescData<'_>,
) -> PgResult<Vec<u8>> {
    Ok(::heaptoast_seams::toast_flatten_tuple::call(mcx, tup, desc)?
        .image()
        .to_vec())
}

/// Flattened, which is why this is not a byte copy: a catalog row can carry a
/// toast pointer into a local toast relation, and a bucket row that
/// dereferences local disk is the finish-line test failing invisibly.
fn lift_rows(mcx: Mcx<'_>, scope: u32, t: &Target, oid_high: &mut u32) -> PgResult<u64> {
    let rel = ::table::table_open(mcx, t.relid, AccessShareLock)?;
    let desc = rel.descr();
    let is_pg_class = t.relid == RelationRelationId;
    let mut n = 0u64;

    let mut scan = ::genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        let flat = ::heaptoast_seams::toast_flatten_tuple::call(mcx, tup, desc)?;

        // relam is rewritten now: the bucket copy is the one that will be read.
        let image = if is_pg_class {
            with_relam(mcx, flat.as_tuple(), desc, ams_of(mcx)?)?
        } else {
            flat.image().to_vec()
        };

        if let Some(oid) = own_oid(tup, desc) {
            *oid_high = (*oid_high).max(oid);
        }

        ::tableam::objkv_am::insert_row(scope, t.relid, image)?;
        n += 1;
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(rel, AccessShareLock)?;
    Ok(n)
}

const AttributeRelationId: Oid = 1249;
const IndexRelationId: Oid = 2610;
const Anum_pg_attribute_attrelid: usize = 1;
const Anum_pg_index_indexrelid: usize = 1;

/// The vocabulary catalogs copied into scope 0 whole. None of them is shared,
/// so none is a `targets(mcx, true)` relation: they are here because the
/// relcache needs them before a database is chosen, and `verify` has to know
/// the same list or it reports a complete check of a subset.
const SHARED_VOCABULARY: [Oid; 5] = [
    2601, // pg_am
    2616, // pg_opclass
    2602, // pg_amop
    2603, // pg_amproc
    1247, // pg_type
];

/// The relations whose rows scope 0 carries a copy of. One definition, used by
/// the lift and by the check that the lift was right.
fn shared_relation_oids(mcx: Mcx<'_>) -> PgResult<Vec<Oid>> {
    let mut wanted: Vec<Oid> = Vec::new();
    let pgclass = ::table::table_open(mcx, RelationRelationId, AccessShareLock)?;
    let desc = pgclass.descr();
    let mut scan = ::genam::systable_beginscan(mcx, &pgclass, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        if getattr(tup, Anum_pg_class_relisshared, desc).as_bool() {
            wanted.push(getattr(tup, Anum_pg_class_oid, desc).as_oid());
        }
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(pgclass, AccessShareLock)?;
    Ok(wanted)
}

/// pg_class, pg_attribute and pg_index rows describing the shared relations,
/// copied into scope 0 so they can be read before a database is chosen.
fn lift_shared_catalog_rows(mcx: Mcx<'_>, oid_high: &mut u32) -> PgResult<u64> {
    let wanted = shared_relation_oids(mcx)?;

    let ams = ams_of(mcx)?;
    let mut n = 0u64;

    {
        let rel = ::table::table_open(mcx, RelationRelationId, AccessShareLock)?;
        let desc = rel.descr();
        let mut scan = ::genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
        while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
            let oid = getattr(tup, Anum_pg_class_oid, desc).as_oid();
            if !wanted.contains(&oid) {
                continue;
            }
            *oid_high = (*oid_high).max(oid);
            let flat = ::heaptoast_seams::toast_flatten_tuple::call(mcx, tup, desc)?;
            let image = with_relam(mcx, flat.as_tuple(), desc, ams)?;
            ::tableam::objkv_am::insert_row(0, RelationRelationId, image)?;
            n += 1;
        }
        ::genam::systable_endscan(mcx, scan)?;
        ::table::table_close(rel, AccessShareLock)?;
    }

    // The vocabulary catalogs, whole. Describing a shared index takes more than
    // its pg_class row -- the relcache reads its access method, operator class
    // and that class's operators and support procedures, all per-database, and
    // there is no database when the shared critical indexes are built. They are
    // small, static and identical everywhere, which makes copying them honest.
    for relid in SHARED_VOCABULARY {
        let rel = ::table::table_open(mcx, relid, AccessShareLock)?;
        let desc = rel.descr();
        let mut scan = ::genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
        while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
            if let Some(oid) = own_oid(tup, desc) {
                *oid_high = (*oid_high).max(oid);
            }
            let flat = ::heaptoast_seams::toast_flatten_tuple::call(mcx, tup, desc)?;
            ::tableam::objkv_am::insert_row(0, relid, flat.image().to_vec())?;
            n += 1;
        }
        ::genam::systable_endscan(mcx, scan)?;
        ::table::table_close(rel, AccessShareLock)?;
    }

    for (relid, keyattr) in
        [(AttributeRelationId, Anum_pg_attribute_attrelid), (IndexRelationId, Anum_pg_index_indexrelid)]
    {
        let rel = ::table::table_open(mcx, relid, AccessShareLock)?;
        let desc = rel.descr();
        let mut scan = ::genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
        while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
            if !wanted.contains(&getattr(tup, keyattr, desc).as_oid()) {
                continue;
            }
            let flat = ::heaptoast_seams::toast_flatten_tuple::call(mcx, tup, desc)?;
            ::tableam::objkv_am::insert_row(0, relid, flat.image().to_vec())?;
            n += 1;
        }
        ::genam::systable_endscan(mcx, scan)?;
        ::table::table_close(rel, AccessShareLock)?;
    }

    Ok(n)
}

/// Index entries over what `lift_shared_catalog_rows` just wrote, so those
/// rows can be found the way boot looks for them rather than by scanning.
fn lift_shared_catalog_indexes(mcx: Mcx<'_>) -> PgResult<u64> {
    let mut n = 0u64;
    // The vocabulary is copied whole into scope 0, so its indexes go with
    // it, plus the three catalogs boot reads to find the vocabulary at all.
    for relid in [RelationRelationId, AttributeRelationId, IndexRelationId]
        .into_iter()
        .chain(SHARED_VOCABULARY)
    {
        n += lift_indexes(
            mcx,
            0,
            &Target { relid, name: String::new(), shared: true },
        )?;
    }
    Ok(n)
}

fn lift_indexes(mcx: Mcx<'_>, scope: u32, t: &Target) -> PgResult<u64> {
    let heap = ::table::table_open(mcx, t.relid, AccessShareLock)?;
    let rows = ::tableam::objkv_am::scan_rows(scope, t.relid, ::objkv::key::LATEST)?;
    let mut n = 0u64;

    for idxoid in ::relcache::RelationGetIndexList(mcx, t.relid)?.iter() {
        let index = ::indexam::index_open(mcx, *idxoid, AccessShareLock)?;
        // A catalog index on a type the encoding cannot order simply has no bucket
        // entries, and reads fall back to the sequential path.
        if !index_is_liftable(&index) {
            ::indexam::index_close(index, AccessShareLock)?;
            continue;
        }
        let mut info = ::execindexing::BuildIndexInfo(mcx, &index)?;
        let mut slot = ::exectuples::make_tuple_table_slot(
            mcx,
            ::types_slot::TupleSlotKind::HeapTuple,
            Some(heap.rd_att.clone()),
        );
        let natts = info.ii_NumIndexAttrs as usize;
        let mut values = vec![Datum::null(); natts.max(1)];
        let mut isnull = vec![false; natts.max(1)];
        for (rowid, image) in &rows {
            ::tableam::objkv_am::store_image(
                mcx,
                &mut slot,
                image,
                ::tableam::objkv_am::tid_of(*rowid),
            )?;
            ::execindexing::FormIndexDatum(mcx, mcx, &mut info, &mut slot, &mut values, &mut isnull)?;
            ::tableam::objkv_index::insert_unchecked(mcx, &index, scope, &values, &isnull, *rowid)?;
            n += 1;
        }
        ::indexam::index_close(index, AccessShareLock)?;
    }
    ::table::table_close(heap, AccessShareLock)?;
    Ok(n)
}

fn index_is_liftable(index: &Relation<'_>) -> bool {
    let Some(ind) = index.rd_index.as_ref() else { return false };
    if ind.indnkeyatts as usize != index.rd_att.natts as usize {
        return false; // INCLUDE columns
    }
    (0..ind.indnkeyatts as usize).all(|i| {
        ind.indkey[i] > 0 && ::tableam::objkv_index::supports_type(index.rd_att.attr(i).atttypid)
    })
}

/// Lifts the scope this backend is entitled to lift: the shared catalogs on
/// the first call in the cluster, then this database's own.
/// Moving the catalogs of every database into the bucket, checking that
/// move, and flipping the server over to it are cluster-wide operations.
/// The functions are created with EXECUTE open to PUBLIC, as any function
/// is, so the check lives here rather than in a GRANT someone has to make.
fn require_superuser() -> PgResult<()> {
    if ::superuser_seams::superuser::call()? {
        return Ok(());
    }
    Err(Box::new(
        ::types_error::PgError::error("must be superuser to lift the catalogs into the bucket".to_string())
            .with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
    ))
}

pub fn lift(mcx: Mcx<'_>) -> PgResult<String> {
    require_superuser()?;
    refuse_unless_alone(mcx, "lift")?;
    let xid = next_xid()?;
    refuse_if_written_since_lifts(xid)?;
    let mut report = String::new();
    if !scope_recorded(0)? {
        report.push_str(&lift_scope(mcx, 0, true, xid)?);
        report.push('\n');
    }
    let db = ::init_small::globals::MyDatabaseId();
    report.push_str(&lift_scope(mcx, db, false, xid)?);
    Ok(report)
}

fn lift_scope(mcx: Mcx<'_>, scope: u32, shared: bool, xid: u64) -> PgResult<String> {
    // The record is what "already lifted" means, written inside the same commit
    // object as the rows so it cannot disagree with them. A scope holding objkv
    // keys is a different condition -- the normal case, and the reason to lift.
    if scope_recorded(scope)? {
        return Err(err(format!(
            "objkv lift: scope {scope:08x} is already lifted; clearing it is an operator action, \
             not something a lift does on its own"
        )));
    }

    let targets = targets(mcx, shared)?;
    let mut rows = 0u64;
    let mut entries = 0u64;
    let mut oid_high = 0u32;
    for t in &targets {
        rows += lift_rows(mcx, scope, t, &mut oid_high)?;
    }
    if shared {
        // Scope 0 needs its own small catalog too: boot builds the shared critical
        // indexes before choosing a database, and their relcache entries read the
        // per-database pg_class and pg_attribute -- so the lookup asks scope 0 and
        // finds nothing ("could not open critical system index 2671"). Postgres never
        // meets this because pg_class is a local file anyone can read.
        rows += lift_shared_catalog_rows(mcx, &mut oid_high)?;
    }
    for t in &targets {
        entries += lift_indexes(mcx, scope, t)?;
    }
    if shared {
        entries += lift_shared_catalog_indexes(mcx)?;
    }

    // Each catalog was read under its own snapshot, so a write that landed
    // between two of them leaves a copy that agrees with neither moment.
    // The counter says whether that happened.
    let after = next_xid()?;
    if after != xid {
        return Err(err(format!(
            "objkv lift: the cluster wrote while scope {scope:08x} was being copied \
             (transaction counter {xid} then, {after} now); nothing was recorded. \
             Run the lift again with nothing else connected."
        )));
    }

    let ams = ams_of(mcx)?;
    let record = format!(
        "v1 relations={} rows={rows} entries={entries} oid_high={oid_high} am={} iam={} xid={xid}",
        targets.len(),
        ams.table,
        ams.index
    );
    ::tableam::objkv_am::stage_raw(record_key(scope), record.clone().into_bytes());

    Ok(format!(
        "scope {scope:08x}: {} relations, {rows} rows, {entries} index entries, oid high-water {oid_high}",
        targets.len()
    ))
}

fn scope_recorded(scope: u32) -> PgResult<bool> {
    ::tableam::objkv_am::key_exists(&record_key(scope))
}

/// Reads the bucket back and compares it with the local catalogs, exactly:
/// every row on both sides, by checksum, because sampling would miss the one
/// wrong row and that is the only interesting failure.
pub fn verify(mcx: Mcx<'_>) -> PgResult<String> {
    require_superuser()?;
    let db = ::init_small::globals::MyDatabaseId();
    let mut checked = 0usize;
    let mut rows = 0u64;
    for (scope, shared) in [(0u32, true), (db, false)] {
        if !scope_recorded(scope)? {
            continue;
        }
        if shared {
            // `targets(mcx, true)` is the relations that are themselves
            // shared, and scope 0 holds more than those: the shared subset of
            // pg_class, pg_attribute and pg_index, and the vocabulary
            // catalogs whole. Those are exactly the rows boot reads to open
            // the shared critical indexes, so leaving them out would let a
            // wrong one pass a check that calls itself complete.
            let (c, r) = verify_shared_extras(mcx)?;
            checked += c;
            rows += r;
        }
        for t in &targets(mcx, shared)? {
            let (local, bucket) = (local_sums(mcx, t)?, bucket_sums(scope, t.relid)?);
            if local.len() != bucket.len() {
                return Err(err(format!(
                    "objkv lift verify: {} has {} local rows and {} in the bucket",
                    t.name,
                    local.len(),
                    bucket.len()
                )));
            }
            if local != bucket {
                return Err(err(format!(
                    "objkv lift verify: {}'s rows differ between disk and the bucket",
                    t.name
                )));
            }
            checked += 1;
            rows += local.len() as u64;
        }
    }
    Ok(format!("verified {checked} relations, {rows} rows, disk and bucket identical"))
}

/// The scope-0 copies `targets(mcx, true)` does not name, compared row for
/// row against the same subset predicates `lift_shared_catalog_rows` used.
fn verify_shared_extras(mcx: Mcx<'_>) -> PgResult<(usize, u64)> {
    let wanted = shared_relation_oids(mcx)?;
    let mut checked = 0usize;
    let mut rows = 0u64;

    let mut compare = |relid: Oid, key: Option<usize>, name: &str| -> PgResult<()> {
        let local = local_sums_subset(mcx, relid, key, &wanted)?;
        let bucket = bucket_sums(0, relid)?;
        if local.len() != bucket.len() {
            return Err(err(format!(
                "objkv lift verify: scope 0 holds {} rows of {name} and disk has {}",
                bucket.len(),
                local.len()
            )));
        }
        if local != bucket {
            return Err(err(format!(
                "objkv lift verify: {name}'s scope-0 rows differ between disk and the bucket"
            )));
        }
        checked += 1;
        rows += local.len() as u64;
        Ok(())
    };

    compare(RelationRelationId, Some(Anum_pg_class_oid), "pg_class")?;
    for relid in SHARED_VOCABULARY {
        compare(relid, None, &format!("catalog {relid}"))?;
    }
    compare(AttributeRelationId, Some(Anum_pg_attribute_attrelid), "pg_attribute")?;
    compare(IndexRelationId, Some(Anum_pg_index_indexrelid), "pg_index")?;

    Ok((checked, rows))
}

/// `local_sums` over the subset a scope-0 copy holds: every row when `key` is
/// `None`, otherwise the rows whose attribute `key` names a shared relation.
fn local_sums_subset(
    mcx: Mcx<'_>,
    relid: Oid,
    key: Option<usize>,
    wanted: &[Oid],
) -> PgResult<Vec<u32>> {
    let rel = ::table::table_open(mcx, relid, AccessShareLock)?;
    let desc = rel.descr();
    let is_pg_class = relid == RelationRelationId;
    let ams = if is_pg_class { ams_of(mcx)? } else { Ams { table: InvalidOid, index: InvalidOid } };
    let mut out = Vec::new();
    let mut scan = ::genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        if let Some(k) = key {
            if !wanted.contains(&getattr(tup, k, desc).as_oid()) {
                continue;
            }
        }
        let flat = ::heaptoast_seams::toast_flatten_tuple::call(mcx, tup, desc)?;
        // pg_class rows were rewritten on the way in, so both sides have to be
        // asked the same question -- as `local_sums` does for the same reason.
        let image = if is_pg_class {
            with_relam(mcx, flat.as_tuple(), desc, ams)?
        } else {
            flat.image().to_vec()
        };
        out.push(crc(&image));
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(rel, AccessShareLock)?;
    out.sort_unstable();
    Ok(out)
}

fn crc(bytes: &[u8]) -> u32 {
    ::crc32c::pg_comp_crc32c(0xffff_ffff, bytes) ^ 0xffff_ffff
}

fn local_sums(mcx: Mcx<'_>, t: &Target) -> PgResult<Vec<u32>> {
    let rel = ::table::table_open(mcx, t.relid, AccessShareLock)?;
    let desc = rel.descr();
    let is_pg_class = t.relid == RelationRelationId;
    let ams = if is_pg_class { ams_of(mcx)? } else { Ams { table: InvalidOid, index: InvalidOid } };
    let mut out = Vec::new();
    let mut scan = ::genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        let flat = ::heaptoast_seams::toast_flatten_tuple::call(mcx, tup, desc)?;
        // pg_class rows were rewritten on the way in, so comparing untouched
        // local rows against them would report every row as differing. The
        // check is worthless unless both sides are the same question.
        let image = if is_pg_class {
            with_relam(mcx, flat.as_tuple(), desc, ams)?
        } else {
            flat.image().to_vec()
        };
        out.push(crc(&image));
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(rel, AccessShareLock)?;
    out.sort_unstable();
    Ok(out)
}

fn bucket_sums(scope: u32, relid: Oid) -> PgResult<Vec<u32>> {
    let mut out: Vec<u32> = ::tableam::objkv_am::scan_rows(scope, relid, ::objkv::key::LATEST)?
        .iter()
        .map(|(_, image)| crc(image))
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// The flip: every database recorded, then one marker file.
pub fn finish(mcx: Mcx<'_>) -> PgResult<String> {
    require_superuser()?;
    // The flip is the moment the cluster changes meaning. Another backend
    // sitting here keeps its pre-flip view and writes catalog rows to a local
    // file the bucket has already replaced.
    refuse_unless_alone(mcx, "flip")?;
    // Whether any photograph has gone stale comes before whether any is
    // missing: the lifts were separate sessions, the count above says nothing
    // about the minutes between them, and a stale one means starting over.
    refuse_if_written_since_lifts(next_xid()?)?;
    let mut missing = Vec::new();
    if !scope_recorded(0)? {
        missing.push("shared catalogs".to_string());
    }
    let pgdb = ::table::table_open(mcx, DatabaseRelationId, AccessShareLock)?;
    let desc = pgdb.descr();
    let mut scan = ::genam::systable_beginscan(mcx, &pgdb, InvalidOid, false, None, &[])?;
    while let Some(tup) = ::genam::systable_getnext(mcx, &mut scan)? {
        // A database nobody may connect to cannot be lifted: there is no backend to
        // run it in. template0 is a pristine copy of a shape already in the bucket.
        if !getattr(tup, Anum_pg_database_datallowconn, desc).as_bool() {
            continue;
        }
        let oid = getattr(tup, Anum_pg_database_oid, desc).as_oid();
        if !scope_recorded(oid)? {
            missing.push(name_of(getattr(tup, Anum_pg_database_datname, desc)));
        }
    }
    ::genam::systable_endscan(mcx, scan)?;
    ::table::table_close(pgdb, AccessShareLock)?;

    if !missing.is_empty() {
        return Err(err(format!(
            "objkv lift: not lifted yet: {}. Run pgrust_objkv_lift() in each.",
            missing.join(", ")
        )));
    }

    // Vouch for the lifts durably first: a commit is confirmed by the next one's
    // header, and the last lift has no next commit.
    ::tableam::objkv_am::publish_watermark()?;

    // Where the object-id counter got to. Postgres keeps this in the
    // write-ahead log and the control file, both of which die with the disk,
    // so the bucket has to hold it or a blank machine starts numbering from
    // initdb's value and hands a second relation a number already in use.
    // Nothing else writes it until the first relation is created, and by then
    // the number has already been handed out.
    ::tableam::objkv_am::claim_oid_block(oid_high_water()? + 1, 0)?;

    // Before the file exists: the marker is read once per process and cached,
    // and this process must keep the pre-flip answer it booted with.
    ::objkv_marker::prime();

    // And before the file exists, refuse catalog writes from every backend in
    // this process: a write between the rename and the flag would go to a
    // local file the bucket has just replaced. Set first, undone if the write
    // fails, since then nothing was replaced.
    ::objkv_marker::note_flip();

    let dir = ::init_small::globals::DataDir()
        .ok_or_else(|| err("objkv lift: no data directory".to_string()))?;
    let path = std::path::Path::new(&dir).join(::objkv_marker::FILENAME);
    let tmp = path.with_extension("tmp");
    let body = ::objkv_marker::body();
    // Temp, flush, rename, flush the directory. The rename is atomic but not
    // durable: unsynced, the entry can name bytes that never landed, or be lost.
    // Either way the marker reads absent and the cluster boots against local
    // catalogs the bucket has already superseded.
    (|| -> std::io::Result<()> {
        let f = std::fs::File::create(&tmp)?;
        {
            use std::io::Write;
            let mut w = &f;
            w.write_all(body.as_bytes())?;
        }
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, &path)?;
        std::fs::File::open(&dir)?.sync_all()
    })()
    .map_err(|e| {
        // Unless the rename landed and only the directory sync failed: then
        // the marker is there, and the next boot will read it.
        if !path.exists() {
            ::objkv_marker::clear_flip();
        }
        err(format!("objkv lift: cannot write {}: {e}", path.display()))
    })?;

    Ok(format!(
        "catalogs are in the bucket; {} written. Restart the server before any \
         further DDL -- this one is still running on its pre-flip view. Rollback \
         is deleting that file, and it stops working after the first write from here.",
        path.display()
    ))
}
