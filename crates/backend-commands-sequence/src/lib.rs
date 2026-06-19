#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
// `PgError` is the large shared error type; every sibling command crate returns
// `PgResult<…>` by value, so boxing it would diverge.
#![allow(clippy::result_large_err)]
// Preserve the C's literal arithmetic/declaration shape (`fetch = log = fetch +
// SEQ_LOG_VALS`, late inits).
#![allow(clippy::assign_op_pattern)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::too_many_arguments)]

//! `backend/commands/sequence.c` — PostgreSQL sequences support code
//! (CREATE / ALTER SEQUENCE, nextval / currval / setval / lastval, the
//! per-backend `SeqTable` cache, the on-disk sequence-page tuple, and sequence
//! WAL replay). PostgreSQL 18.3, ported branch-for-branch.
//!
//! The per-backend `SeqTable` hash (`seqhashtab`) and `last_used_seq` are this
//! crate's own backend-private state, modelled as `thread_local!` (the
//! backend-globals rule). Every cross-subsystem primitive — the buffer manager,
//! WAL insert/replay, the relcache, `tablecmds.c::DefineRelation`,
//! `catalog/dependency.c`, `namespace.c` name resolution, `defGet*`,
//! `pg_class_aclcheck`, `typenameTypeId` — crosses through that owner's
//! per-owner seam crate. The buffer/page/heap-tuple/WAL byte work
//! (`fill_seq_fork_with_data`, `nextval_internal`'s commit, `do_setval`'s
//! commit, `read_seq_tuple`, `seq_redo`, `seq_mask`) is done in-crate over the
//! ported `backend-storage-page` / `backend-access-common-heaptuple` / bufmgr /
//! xloginsert primitives, exactly as the C.

pub mod fmgr_builtins;

use backend_utils_error::ereport;
use types_error::{ErrorLocation, PgResult, ERROR, NOTICE, PANIC};

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::Mcx;
use types_core::primitive::{BlockNumber, ForkNumber, LocalTransactionId, Oid, BLCKSZ};
use types_core::RmgrId;
use types_storage::lock::{AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock};
use types_storage::Buffer;
use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_tuple::heaptuple::{
    HeapTupleField3, HeapTupleHeaderChoice, HeapTupleHeaderData, ItemPointerData, BOOLOID,
    FIRST_OFFSET_NUMBER, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI,
    HEAP_XMIN_FROZEN, INT2OID, INT4OID, INT8OID, INVALID_OFFSET_NUMBER, OIDOID, ON_PAGE_HEADER_SIZE,
};

use types_acl::{ACL_SELECT, ACL_UPDATE, ACL_USAGE, ACLCHECK_OK};
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL,
};
use types_catalog::pg_sequence::FormData_pg_sequence;
use types_error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_WRONG_OBJECT_TYPE, ERRCODE_DUPLICATE_TABLE,
};
use types_nodes::ddlnodes::{AlterSeqStmt, CreateSeqStmt, DefElem};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsestmt::ParseState;
use types_nodes::rawnodes::RangeVar;
use types_rel::Relation;

use backend_utils_fmgr_fmgr_seams::pg_call_mcx;
use types_tuple::Datum as ValueDatum;

mod xact {
    pub use backend_access_transam_xact_seams::*;
}

// ===========================================================================
// Constants from headers (verified against postgres-18.3 include)
// ===========================================================================

/// `RelationRelationId` (pg_class oid, `pg_class_d.h`) — 1259.
const RelationRelationId: Oid = 1259;

/// `PG_INT16_MIN`/`MAX`, `PG_INT32_MIN`/`MAX`, `PG_INT64_MIN`/`MAX` (`c.h`).
const PG_INT16_MIN: i64 = i16::MIN as i64;
const PG_INT16_MAX: i64 = i16::MAX as i64;
const PG_INT32_MIN: i64 = i32::MIN as i64;
const PG_INT32_MAX: i64 = i32::MAX as i64;
const PG_INT64_MIN: i64 = i64::MIN;
const PG_INT64_MAX: i64 = i64::MAX;

/// We pre-log a few fetches in advance (`SEQ_LOG_VALS`).
const SEQ_LOG_VALS: i64 = 32;

/// The "special area" magic number of a sequence buffer page (`SEQ_MAGIC`).
const SEQ_MAGIC: u32 = 0x1717;

/// `XLOG_SEQ_LOG` (sequence.h) — the only sequence WAL record op.
const XLOG_SEQ_LOG: u8 = 0x00;

/// `RM_SEQ_ID` (`access/rmgrlist.h`) — sequence resource-manager id.
const RM_SEQ_ID: RmgrId = 15;

/// `REGBUF_WILL_INIT` (`access/xloginsert.h`).
const REGBUF_WILL_INIT: u8 = types_wal::xloginsert::REGBUF_WILL_INIT;

/// `XLR_INFO_MASK` (`access/xlogrecord.h`).
const XLR_INFO_MASK: u8 = 0x0F;

/// `InvalidAttrNumber` (`access/attnum.h`) — 0.
const InvalidAttrNumber: i16 = 0;

/// `sizeof(xl_seq_rec)` — a single `RelFileLocator` (3 × Oid = 12 bytes, no
/// padding).
const SIZEOF_XL_SEQ_REC: usize = 12;

/// `ErrorLocation` for `ereport(...).finish(...)`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/sequence.c", 0, funcname)
}

// ===========================================================================
// SeqTableData — the per-backend cache (C: `seqhashtab` + `last_used_seq`)
// ===========================================================================

/// `SeqTableData` (sequence.c): per-backend cached state for one sequence.
#[derive(Clone, Copy, Debug)]
struct SeqTableData {
    /// pg_class OID of this sequence (hash key).
    relid: Oid,
    /// last seen relfilenumber of this sequence.
    filenumber: Oid,
    /// xact in which we last did a seq op.
    lxid: LocalTransactionId,
    /// do we have a valid "last" value?
    last_valid: bool,
    /// value last returned by nextval.
    last: i64,
    /// last value already cached for nextval.
    cached: i64,
    /// copy of sequence's increment field (zero until first nextval_internal).
    increment: i64,
}

thread_local! {
    /// `static HTAB *seqhashtab` — the per-backend SeqTable hash.
    static SEQHASHTAB: RefCell<Option<HashMap<Oid, SeqTableData>>> = const { RefCell::new(None) };
    /// `static SeqTableData *last_used_seq` — the last sequence used by nextval.
    /// Modelled as the relid key (the entry lives in SEQHASHTAB).
    static LAST_USED_SEQ: RefCell<Option<Oid>> = const { RefCell::new(None) };
}

/// `FormData_pg_sequence_data` (sequence.h): the on-page sequence tuple's three
/// columns.
#[derive(Clone, Copy, Debug, Default)]
struct FormData_pg_sequence_data {
    last_value: i64,
    log_cnt: i64,
    is_called: bool,
}

/// `xl_seq_rec` (sequence.h): the WAL record fixed part (the locator).
struct XlSeqRec {
    locator: types_storage::RelFileLocator,
}

// Helpers around the SeqTable thread_local.
fn seqtable_with<R>(relid: Oid, f: impl FnOnce(&mut SeqTableData) -> R) -> R {
    SEQHASHTAB.with(|h| {
        let mut b = h.borrow_mut();
        let map = b.as_mut().expect("seqhashtab not created");
        let e = map.get_mut(&relid).expect("SeqTable entry missing");
        f(e)
    })
}

fn seqtable_get<R>(relid: Oid, f: impl FnOnce(&SeqTableData) -> R) -> R {
    SEQHASHTAB.with(|h| {
        let b = h.borrow();
        let map = b.as_ref().expect("seqhashtab not created");
        let e = map.get(&relid).expect("SeqTable entry missing");
        f(e)
    })
}

// ===========================================================================
// Sequence-data byte (de)serialization on the page item user-data area.
// ===========================================================================

/// Decode the three sequence-data columns from a tuple user-data slice.
fn decode_seq_data(userdata: &[u8]) -> FormData_pg_sequence_data {
    let last_value = i64::from_ne_bytes(userdata[0..8].try_into().unwrap());
    let log_cnt = i64::from_ne_bytes(userdata[8..16].try_into().unwrap());
    let is_called = userdata[16] != 0;
    FormData_pg_sequence_data {
        last_value,
        log_cnt,
        is_called,
    }
}

/// Write the three sequence-data columns into a tuple user-data slice.
fn encode_seq_data(userdata: &mut [u8], seq: &FormData_pg_sequence_data) {
    userdata[0..8].copy_from_slice(&seq.last_value.to_ne_bytes());
    userdata[8..16].copy_from_slice(&seq.log_cnt.to_ne_bytes());
    userdata[16] = seq.is_called as u8;
}

// ===========================================================================
// Node accessors
// ===========================================================================

/// Borrow the `RangeVar` carried by `seq->sequence` (a non-NULL `RangeVar *`).
fn sequence_rangevar<'a, 'mcx>(seq: &'a Option<types_nodes::nodes::NodePtr<'mcx>>) -> &'a RangeVar<'mcx> {
    match seq.as_deref().and_then(|n| n.as_rangevar()) {
        Some(rv) => rv,
        _ => panic!("sequence.c: CreateSeqStmt/AlterSeqStmt.sequence is not a RangeVar node"),
    }
}

/// Borrow a sequence option `Node` as its inner `DefElem`.
fn as_defelem<'a, 'mcx>(node: &'a Node<'mcx>) -> &'a DefElem<'mcx> {
    match node.node_tag() {
        ntag::T_DefElem => node.expect_defelem(),
        _ => panic!("sequence.c: sequence option is not a DefElem node"),
    }
}

/// `boolVal(node)` (`nodes/value.h`): read a `T_Boolean` node's value.
fn bool_val(node: &Node<'_>) -> bool {
    match node.node_tag() {
        ntag::T_Boolean => node.expect_boolean().boolval,
        _ => panic!("sequence.c: CYCLE option arg is not a Boolean node"),
    }
}

/// `OidIsValid(oid)`.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != types_core::InvalidOid
}

/// `ObjectAddressSet(addr, class, object)` — sets `objectSubId = 0`.
fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// The `elog(ERROR, "cache lookup failed for sequence %u", relid)` helper.
fn elog_cache_lookup_failed(relid: Oid, funcname: &'static str) -> PgResult<()> {
    ereport(ERROR)
        .errmsg_internal(format!("cache lookup failed for sequence {relid}"))
        .finish(here(funcname))
}

// ===========================================================================
// DefineSequence — Creates a new sequence relation  (C lines 120-247)
// ===========================================================================

/// `DefineSequence(pstate, seq)` — CREATE SEQUENCE.
pub fn DefineSequence<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_>,
    seq: &CreateSeqStmt<'_>,
) -> PgResult<ObjectAddress> {
    let mut seqform = FormData_pg_sequence::default();
    let mut seqdataform = FormData_pg_sequence_data::default();
    let mut need_seq_rewrite = false;
    let mut owned_by: Vec<String> = Vec::new();

    /*
     * If if_not_exists was given and a relation with the same name already
     * exists, bail out. (Note: we needn't check this when not if_not_exists,
     * because DefineRelation will complain anyway.)
     */
    if seq.if_not_exists {
        let seqoid = backend_catalog_namespace_seams::range_var_get_and_check_creation_namespace::call(
            sequence_rangevar(&seq.sequence),
        )?;
        if OidIsValid(seqoid) {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            let address = object_address_set(RelationRelationId, seqoid);
            backend_catalog_pg_depend_seams::checkMembershipInCurrentExtension::call(mcx, &address)?;

            /* OK to skip */
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_TABLE)
                .errmsg(format!(
                    "relation \"{}\" already exists, skipping",
                    rangevar_relname(sequence_rangevar(&seq.sequence))
                ))
                .finish(here("DefineSequence"))?;
            return Ok(types_catalog::catalog_dependency::InvalidObjectAddress);
        }
    }

    /* Check and set all option values */
    init_params(
        pstate,
        &seq.options,
        seq.for_identity,
        true,
        &mut seqform,
        &mut seqdataform,
        &mut need_seq_rewrite,
        &mut owned_by,
    )?;

    /*
     * Create relation (and fill value[]/null[] for the tuple). The owner builds
     * the CreateStmt with the three NOT NULL columns and calls DefineRelation
     * with RELKIND_SEQUENCE.
     */
    let address = backend_commands_tablecmds_seams::define_sequence_relation::call(mcx, seq)?;
    let seqoid = address.objectId;
    debug_assert!(seqoid != types_core::InvalidOid);

    let rel = backend_access_sequence_seams::sequence_open::call(mcx, seqoid, AccessExclusiveLock)?;

    /* now initialize the sequence's data */
    fill_seq_with_data(mcx, &rel, &seqdataform)?;

    /* process OWNED BY if given */
    if !owned_by.is_empty() {
        process_owned_by(mcx, &rel, &owned_by, seq.for_identity)?;
    }

    // sequence_close(rel, NoLock): close the handle opened above. Closing the
    // RAII handle directly (rather than a second by-OID `sequence_close`) is
    // what keeps this balanced — the by-OID close would decrement the relcache
    // refcount once AND then `rel`'s Drop would decrement it again, leaving the
    // entry's `rd_refcnt` underflowed so a later DROP's `CheckTableNotInUse`
    // reports the sequence as "used by active queries in this session".
    rel.close(NoLock)?;

    /* fill in pg_sequence */
    seqform.seqrelid = seqoid;
    backend_catalog_indexing_seams::catalog_insert_pg_sequence::call(&seqform)?;

    Ok(object_address_set(RelationRelationId, seqoid))
}

// ===========================================================================
// ResetSequence  (C lines 261-329)
// ===========================================================================

/// `ResetSequence(seq_relid)` — TRUNCATE ... RESTART support.
pub fn ResetSequence<'mcx>(mcx: Mcx<'mcx>, seq_relid: Oid) -> PgResult<()> {
    /*
     * Read the old sequence. This does a bit more work than really necessary,
     * but it's simple, and we do want to double-check that it's indeed a
     * sequence.
     */
    let seq_rel = init_sequence(mcx, seq_relid)?;
    let (buf, mut seq) = read_seq_tuple(mcx, &seq_rel)?;

    let startv = match backend_utils_cache_syscache_seams::search_seqrelid::call(seq_relid)? {
        Some(p) => p.seqstart,
        None => {
            return elog_cache_lookup_failed(seq_relid, "ResetSequence");
        }
    };

    /* Now we're done with the old page */
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);

    /*
     * Modify the (copied) tuple to execute the restart (compare the RESTART
     * action in AlterSequence).
     */
    seq.last_value = startv;
    seq.is_called = false;
    seq.log_cnt = 0;

    /*
     * Create a new storage file for the sequence.
     */
    let persistence = rel_relpersistence(&seq_rel) as i8;
    backend_utils_cache_relcache_seams::relation_set_new_relfilenumber::call(
        rel_relid(&seq_rel),
        persistence,
    )?;

    /*
     * Insert the modified tuple into the new storage file.
     */
    fill_seq_with_data(mcx, &seq_rel, &seq)?;

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    seqtable_with(seq_relid, |elm| elm.cached = elm.last);

    // sequence_close(seq_rel, NoLock): close the open RAII handle directly. A
    // by-OID `sequence_close` here would decrement the relcache refcount AND
    // then `seq_rel`'s Drop would decrement it again (double free of the pin).
    seq_rel.close(NoLock)?;
    Ok(())
}

// ===========================================================================
// fill_seq_with_data / fill_seq_fork_with_data  (C lines 337-429)
// ===========================================================================

/// `fill_seq_with_data(rel, tuple)` — initialize a sequence's relation with the
/// specified data. Handles unlogged sequences by writing to both the main and
/// the init fork.
fn fill_seq_with_data<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    data: &FormData_pg_sequence_data,
) -> PgResult<()> {
    fill_seq_fork_with_data(mcx, rel, data, ForkNumber::MAIN_FORKNUM)?;

    if rel_relpersistence(rel) == RELPERSISTENCE_UNLOGGED {
        let locator = rel_locator(rel);

        // srel = smgropen(rel->rd_locator, INVALID_PROC_NUMBER);
        // smgrcreate(srel, INIT_FORKNUM, false);
        // log_smgrcreate(&rel->rd_locator, INIT_FORKNUM);
        // (the transient SMgrRelation stays inside the storage owner.)
        backend_catalog_storage_seams::smgr_create_init_fork_and_log::call(locator)?;

        fill_seq_fork_with_data(mcx, rel, data, ForkNumber::INIT_FORKNUM)?;

        // FlushRelationBuffers(rel);
        backend_storage_buffer_bufmgr_seams::flush_relation_buffers::call(rel)?;

        // smgrclose(srel);
        backend_storage_smgr_seams::relation_close_smgr::call(
            types_storage::RelFileLocatorBackend {
                locator,
                backend: types_core::primitive::INVALID_PROC_NUMBER,
            },
        );
    }
    Ok(())
}

/// `fill_seq_fork_with_data(rel, tuple, forkNum)` — initialize a sequence's
/// relation fork with the specified data.
fn fill_seq_fork_with_data<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    data: &FormData_pg_sequence_data,
    fork_num: ForkNumber,
) -> PgResult<()> {
    /* Initialize first page of relation with special magic number. */
    // ExtendBufferedRel(BMR_REL(rel), forkNum, NULL, EB_LOCK_FIRST |
    // EB_SKIP_EXTENSION_LOCK) returns an already write-locked, pinned buffer at
    // block 0; C does not lock it again.
    let buf = backend_storage_buffer_bufmgr_seams::extend_buffered_rel::call(rel, fork_num)?;
    debug_assert!(backend_storage_buffer_bufmgr_seams::buffer_get_block_number::call(buf) == 0);

    /*
     * Build the on-page item image (the heap tuple t_data) in local workspace:
     * the frozen-xmin 23-byte header + the three-column user data. This is the
     * exact byte image PageAddItem installs and XLogRegisterData logs.
     */
    let item = build_seq_item(mcx, rel, data)?;

    /* check the comment above nextval_internal()'s equivalent call. */
    if rel_needs_wal(rel) {
        xact::get_top_transaction_id::call()?;
    }

    // START_CRIT_SECTION();
    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);

    // Initialize the page, set the special-area magic, and add the item.
    let item_for_page = item.clone();
    backend_storage_buffer_bufmgr_seams::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        backend_storage_page::PageInit(page, BLCKSZ, 4)?;
        // sm->magic = SEQ_MAGIC (the page's 4-byte special area).
        let off = special_pointer_offset(page)?;
        page[off..off + 4].copy_from_slice(&SEQ_MAGIC.to_ne_bytes());
        let mut pm = backend_storage_page::PageMut::new(page)?;
        let offnum = backend_storage_page::PageAddItemExtended(
            &mut pm,
            &item_for_page,
            INVALID_OFFSET_NUMBER,
            0,
        )?;
        if offnum != FIRST_OFFSET_NUMBER {
            return ereport(ERROR)
                .errmsg_internal("failed to add sequence tuple to page")
                .finish(here("fill_seq_fork_with_data"));
        }
        Ok(())
    })?;

    /* XLOG stuff */
    if rel_needs_wal(rel) || fork_num == ForkNumber::INIT_FORKNUM {
        backend_access_transam_xloginsert_seams::xlog_begin_insert::call()?;
        backend_access_transam_xloginsert_seams::xlog_register_buffer::call(0, buf, REGBUF_WILL_INIT)?;

        let xlrec = XlSeqRec {
            locator: rel_locator(rel),
        };
        backend_access_transam_xloginsert_seams::xlog_register_data::call(&serialize_locator(
            &xlrec.locator,
        ))?;
        backend_access_transam_xloginsert_seams::xlog_register_data::call(&item)?;

        let recptr = backend_access_transam_xloginsert_seams::xlog_insert_record::call(
            RM_SEQ_ID,
            XLOG_SEQ_LOG,
        )?;
        backend_storage_buffer_bufmgr_seams::page_set_lsn::call(buf, recptr)?;
    }

    // END_CRIT_SECTION();
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);

    Ok(())
}

/// Build the on-page heap-tuple item image (`t_data`) for a sequence-data
/// tuple: the 23-byte frozen header + the three-column user data. Mirrors the
/// C `heap_form_tuple` + the frozen-xmin / xmax-invalid header pokes in
/// `fill_seq_fork_with_data`.
fn build_seq_item<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    data: &FormData_pg_sequence_data,
) -> PgResult<Vec<u8>> {
    use backend_access_common_heaptuple::{heap_form_tuple, HeapTupleError};

    // heap_form_tuple(tupDesc, value, null) — three columns, none null.
    let values = [
        ValueDatum::from_i64(data.last_value),
        ValueDatum::from_i64(data.log_cnt),
        ValueDatum::from_bool(data.is_called),
    ];
    let isnull = [false, false, false];
    let formed = heap_form_tuple(mcx, &rel.rd_att, &values, &isnull).map_err(|e| match e {
        HeapTupleError::Pg(p) => p,
        other => ereport(ERROR)
            .errmsg_internal(format!("heap_form_tuple failed for sequence: {other:?}"))
            .finish(here("build_seq_item"))
            .unwrap_err(),
    })?;

    // Reconstruct the on-disk t_data image: 23-byte header + (null bitmap, none)
    // + user data starting at t_hoff. The formed header carries t_hoff; write
    // the fixed header, apply the frozen-xmin pokes, and append user data.
    let mut hdr: HeapTupleHeaderData = match &formed.tuple.t_data {
        Some(h) => (**h).clone(),
        None => panic!("sequence.c: heap_form_tuple produced no t_data header"),
    };
    let t_hoff = hdr.t_hoff as usize;
    let mut item = vec![0u8; t_hoff + formed.data.len()];

    // HeapTupleHeaderSetXmin(FrozenTransactionId);
    // HeapTupleHeaderSetXminFrozen();  (sets HEAP_XMIN_FROZEN bits in t_infomask)
    // HeapTupleHeaderSetCmin(FirstCommandId);  (t_field3 = TCid(0))
    // HeapTupleHeaderSetXmax(InvalidTransactionId);
    // t_data->t_infomask |= HEAP_XMAX_INVALID;
    // ItemPointerSet(&t_data->t_ctid, 0, FirstOffsetNumber);
    set_header_field3_cmin(&mut hdr, types_core::xact::FirstCommandId);
    set_header_xmin(&mut hdr, types_core::xact::FrozenTransactionId);
    hdr.t_infomask |= HEAP_XMIN_FROZEN;
    set_header_xmax(&mut hdr, types_core::xact::InvalidTransactionId);
    hdr.t_infomask |= HEAP_XMAX_INVALID;
    hdr.t_ctid = ItemPointerData::new(0, FIRST_OFFSET_NUMBER);

    hdr.write_on_page(&mut item[..ON_PAGE_HEADER_SIZE])?;
    // Copy the formed user-data area into [t_hoff..].
    item[t_hoff..].copy_from_slice(&formed.data);

    let _ = mcx; // formed allocated in mcx
    Ok(item)
}

fn set_header_xmin(hdr: &mut HeapTupleHeaderData, xid: types_core::TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmin = xid;
    }
}
fn set_header_xmax(hdr: &mut HeapTupleHeaderData, xid: types_core::TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmax = xid;
    }
}
fn set_header_field3_cmin(hdr: &mut HeapTupleHeaderData, cid: types_core::CommandId) {
    // HeapTupleHeaderSetCmin: t_field3.t_cid = cid; also clears HEAP_XMAX_IS_MULTI
    // (Assert(!(t_infomask & HEAP_MOVED))). For a fresh formed tuple t_infomask
    // has neither bit; mirror the field write.
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_field3 = HeapTupleField3::TCid(cid);
    }
    hdr.t_infomask &= !HEAP_XMAX_IS_MULTI;
}

/// The byte offset of a page's special-area pointer within `page`
/// (`PageGetSpecialPointer`). Computed from a read-only `PageRef` so the
/// caller can then write the magic with a fresh mutable borrow.
fn special_pointer_offset(page: &[u8]) -> PgResult<usize> {
    let pref = backend_storage_page::PageRef::new(page)?;
    let sp = backend_storage_page::PageGetSpecialPointer(&pref)?;
    Ok(sp.as_ptr() as usize - page.as_ptr() as usize)
}

/// Serialize a `RelFileLocator`'s three Oids to native-endian bytes (C struct
/// order: spcOid, dbOid, relNumber).
fn serialize_locator(loc: &types_storage::RelFileLocator) -> Vec<u8> {
    let mut b = Vec::with_capacity(SIZEOF_XL_SEQ_REC);
    b.extend_from_slice(&loc.spcOid.to_ne_bytes());
    b.extend_from_slice(&loc.dbOid.to_ne_bytes());
    b.extend_from_slice(&loc.relNumber.to_ne_bytes());
    b
}

// ===========================================================================
// AlterSequence  (C lines 436-538)
// ===========================================================================

/// `AlterSequence(pstate, stmt)` — ALTER SEQUENCE.
pub fn AlterSequence<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_>,
    stmt: &AlterSeqStmt<'_>,
) -> PgResult<ObjectAddress> {
    /* Open and lock sequence, and check for ownership along the way. */
    let relid = backend_catalog_namespace_seams::range_var_get_relid_owns_seq::call(
        sequence_rangevar(&stmt.sequence),
        stmt.missing_ok,
    )?;
    if relid == types_core::InvalidOid {
        ereport(NOTICE)
            .errmsg(format!(
                "relation \"{}\" does not exist, skipping",
                rangevar_relname(sequence_rangevar(&stmt.sequence))
            ))
            .finish(here("AlterSequence"))?;
        return Ok(types_catalog::catalog_dependency::InvalidObjectAddress);
    }

    let seqrel = init_sequence(mcx, relid)?;

    // rel = table_open(SequenceRelationId, RowExclusiveLock);
    // seqtuple = SearchSysCacheCopy1(SEQRELID, relid);  (the owner re-opens and
    // updates by seqrelid in catalog_update_pg_sequence)
    let mut seqform = match backend_utils_cache_syscache_seams::search_seqrelid::call(relid)? {
        Some(t) => t,
        None => {
            return elog_cache_lookup_failed(relid, "AlterSequence")
                .map(|()| types_catalog::catalog_dependency::InvalidObjectAddress);
        }
    };

    /* lock page buffer and read tuple into new sequence structure */
    let (buf, mut newdataform) = read_seq_tuple(mcx, &seqrel)?;
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);

    /* Check and set new values */
    let mut need_seq_rewrite = false;
    let mut owned_by: Vec<String> = Vec::new();
    init_params(
        pstate,
        &stmt.options,
        stmt.for_identity,
        false,
        &mut seqform,
        &mut newdataform,
        &mut need_seq_rewrite,
        &mut owned_by,
    )?;

    /* If needed, rewrite the sequence relation itself */
    if need_seq_rewrite {
        /* check the comment above nextval_internal()'s equivalent call. */
        if rel_needs_wal(&seqrel) {
            xact::get_top_transaction_id::call()?;
        }

        let persistence = rel_relpersistence(&seqrel) as i8;
        backend_utils_cache_relcache_seams::relation_set_new_relfilenumber::call(
            rel_relid(&seqrel),
            persistence,
        )?;

        fill_seq_with_data(mcx, &seqrel, &newdataform)?;
    }

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    seqtable_with(relid, |elm| elm.cached = elm.last);

    /* process OWNED BY if given */
    if !owned_by.is_empty() {
        process_owned_by(mcx, &seqrel, &owned_by, stmt.for_identity)?;
    }

    /* update the pg_sequence tuple */
    seqform.seqrelid = relid;
    let found = backend_catalog_indexing_seams::catalog_update_pg_sequence::call(&seqform)?;
    if !found {
        return elog_cache_lookup_failed(relid, "AlterSequence")
            .map(|()| types_catalog::catalog_dependency::InvalidObjectAddress);
    }
    // InvokeObjectPostAlterHook folded into catalog_update_pg_sequence.

    let address = object_address_set(RelationRelationId, relid);

    // sequence_close(seqrel, NoLock): close the open RAII handle directly (a
    // by-OID close plus the handle's Drop would double-decrement the pin).
    seqrel.close(NoLock)?;

    Ok(address)
}

// ===========================================================================
// SequenceChangePersistence  (C lines 540-567)
// ===========================================================================

/// `SequenceChangePersistence(relid, newrelpersistence)`.
pub fn SequenceChangePersistence<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    new_relpersistence: u8,
) -> PgResult<()> {
    /*
     * ALTER SEQUENCE acquires this lock earlier. If we're processing an owned
     * sequence for ALTER TABLE, lock now.
     */
    backend_storage_lmgr_lmgr::LockRelationOid(relid, AccessExclusiveLock)?;
    let seqrel = init_sequence(mcx, relid)?;

    /* check the comment above nextval_internal()'s equivalent call. */
    if rel_needs_wal(&seqrel) {
        xact::get_top_transaction_id::call()?;
    }

    let (buf, seqdatatuple) = read_seq_tuple(mcx, &seqrel)?;
    backend_utils_cache_relcache_seams::relation_set_new_relfilenumber::call(
        rel_relid(&seqrel),
        new_relpersistence as i8,
    )?;
    fill_seq_with_data(mcx, &seqrel, &seqdatatuple)?;
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);

    // sequence_close(seqrel, NoLock): close the RAII handle directly (a by-OID
    // close plus the handle's Drop would double-decrement the pin).
    seqrel.close(NoLock)?;
    Ok(())
}

// ===========================================================================
// DeleteSequenceTuple  (C lines 569-585)
// ===========================================================================

/// `DeleteSequenceTuple(relid)` — delete the pg_sequence row.
pub fn DeleteSequenceTuple(relid: Oid) -> PgResult<()> {
    let found = backend_catalog_indexing_seams::catalog_delete_pg_sequence::call(relid)?;
    if !found {
        return elog_cache_lookup_failed(relid, "DeleteSequenceTuple");
    }
    Ok(())
}

// ===========================================================================
// nextval / nextval_oid / nextval_internal  (C lines 592-863)
// ===========================================================================

/// `nextval(PG_FUNCTION_ARGS)` — SQL `nextval(text)`.
pub fn nextval<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let seqin = backend_utils_fmgr_fmgr_seams::pg_getarg_text_pp::call(fcinfo, 0)?;
    let name = text_to_str(&seqin);

    /*
     * XXX: This is not safe in the presence of concurrent DDL, but acquiring a
     * lock here is more expensive than letting nextval_internal do it.
     */
    // relid = RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(seqin)), NoLock, false);
    let relid =
        backend_catalog_namespace_seams::range_var_get_relid_from_text::call(mcx, &name, NoLock, false)?;

    let v = nextval_internal(mcx, relid, true)?;
    Ok(ValueDatum::from_i64(v))
}

/// `nextval_oid(PG_FUNCTION_ARGS)` — SQL `nextval(regclass)`.
pub fn nextval_oid<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let relid = backend_utils_fmgr_fmgr_seams::pg_getarg_oid::call(fcinfo, 0);
    let v = nextval_internal(mcx, relid, true)?;
    Ok(ValueDatum::from_i64(v))
}

/// `nextval_internal(relid, check_permissions)`.
pub fn nextval_internal<'mcx>(mcx: Mcx<'mcx>, relid: Oid, check_permissions: bool) -> PgResult<i64> {
    let mut rescnt: i64 = 0;
    let mut logit = false;

    /* open and lock sequence */
    let seqrel = init_sequence(mcx, relid)?;

    if check_permissions
        && backend_catalog_aclchk_seams::pg_class_aclcheck::call(
            seqtable_get(relid, |e| e.relid),
            backend_utils_init_miscinit_seams::get_user_id::call(),
            ACL_USAGE | ACL_UPDATE,
        )? != ACLCHECK_OK
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for sequence {}", rel_name(&seqrel)))
            .finish(here("nextval_internal"))
            .map(|()| 0);
    }

    /* read-only transactions may only modify temp sequences */
    if !rel_is_localtemp(&seqrel) {
        xact::prevent_command_if_read_only::call("nextval()")?;
    }

    /*
     * Forbid this during parallel operation.
     */
    xact::prevent_command_if_parallel_mode::call("nextval()")?;

    let (cur_last, cur_cached, cur_incr, cur_valid) =
        seqtable_get(relid, |e| (e.last, e.cached, e.increment, e.last_valid));
    if cur_last != cur_cached {
        /* some numbers were cached */
        debug_assert!(cur_valid);
        debug_assert!(cur_incr != 0);
        let new_last = cur_last + cur_incr;
        seqtable_with(relid, |e| e.last = new_last);
        // sequence_close(seqrel, NoLock): close the RAII handle directly (a
        // by-OID close plus the handle's Drop would double-decrement the pin).
        seqrel.close(NoLock)?;
        LAST_USED_SEQ.with(|s| *s.borrow_mut() = Some(relid));
        return Ok(new_last);
    }

    let (incby, maxv, minv, cache, cycle) =
        match backend_utils_cache_syscache_seams::search_seqrelid::call(relid)? {
            Some(p) => (p.seqincrement, p.seqmax, p.seqmin, p.seqcache, p.seqcycle),
            None => return elog_cache_lookup_failed(relid, "nextval_internal").map(|()| 0),
        };

    /* lock page buffer and read tuple */
    let (buf, seq_read) = read_seq_tuple(mcx, &seqrel)?;
    let page_lsn = backend_storage_buffer_bufmgr_seams::page_get_lsn::call(buf)?;

    let mut last;
    let mut next;
    let mut result;
    last = seq_read.last_value;
    next = last;
    result = last;
    let mut fetch = cache;
    let mut log = seq_read.log_cnt;

    if !seq_read.is_called {
        rescnt += 1; /* return last_value if not is_called */
        fetch -= 1;
    }

    /*
     * Decide whether we should emit a WAL log record.
     */
    if log < fetch || !seq_read.is_called {
        /* forced log to satisfy local demand for values */
        fetch = fetch + SEQ_LOG_VALS;
        log = fetch;
        logit = true;
    } else {
        let redoptr = backend_access_transam_xlog_seams::get_redo_rec_ptr::call();
        if page_lsn <= redoptr {
            /* last update of seq was before checkpoint */
            fetch = fetch + SEQ_LOG_VALS;
            log = fetch;
            logit = true;
        }
    }

    while fetch != 0 {
        /* try to fetch cache [+ log ] numbers */
        if incby > 0 {
            /* ascending sequence */
            if (maxv >= 0 && next > maxv - incby) || (maxv < 0 && next + incby > maxv) {
                if rescnt > 0 {
                    break; /* stop fetching */
                }
                if !cycle {
                    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
                    return ereport(ERROR)
                        .errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED)
                        .errmsg(format!(
                            "nextval: reached maximum value of sequence \"{}\" ({})",
                            rel_name(&seqrel),
                            maxv
                        ))
                        .finish(here("nextval_internal"))
                        .map(|()| 0);
                }
                next = minv;
            } else {
                next += incby;
            }
        } else {
            /* descending sequence */
            if (minv < 0 && next < minv - incby) || (minv >= 0 && next + incby < minv) {
                if rescnt > 0 {
                    break; /* stop fetching */
                }
                if !cycle {
                    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
                    return ereport(ERROR)
                        .errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED)
                        .errmsg(format!(
                            "nextval: reached minimum value of sequence \"{}\" ({})",
                            rel_name(&seqrel),
                            minv
                        ))
                        .finish(here("nextval_internal"))
                        .map(|()| 0);
                }
                next = maxv;
            } else {
                next += incby;
            }
        }
        fetch -= 1;
        if rescnt < cache {
            log -= 1;
            rescnt += 1;
            last = next;
            if rescnt == 1 {
                /* if it's first result - it's what to return */
                result = next;
            }
        }
    }

    log -= fetch; /* adjust for any unfetched numbers */
    debug_assert!(log >= 0);

    /* save info in local cache */
    seqtable_with(relid, |e| {
        e.increment = incby;
        e.last = result;
        e.cached = last;
        e.last_valid = true;
    });

    LAST_USED_SEQ.with(|s| *s.borrow_mut() = Some(relid));

    /*
     * If something needs to be WAL logged, acquire an xid.
     */
    if logit && rel_needs_wal(&seqrel) {
        xact::get_top_transaction_id::call()?;
    }

    /* ready to change the on-disk (or really, in-buffer) tuple */
    // START_CRIT_SECTION();
    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);

    /* XLOG stuff */
    if logit && rel_needs_wal(&seqrel) {
        /*
         * We log the state as it would appear after "log" more fetches:
         * last_value = next, is_called = true, log_cnt = 0.
         */
        let logged = FormData_pg_sequence_data {
            last_value: next,
            is_called: true,
            log_cnt: 0,
        };
        let logged_item = write_seq_data_into_buffer(mcx, &seqrel, buf, &logged)?;

        backend_access_transam_xloginsert_seams::xlog_begin_insert::call()?;
        backend_access_transam_xloginsert_seams::xlog_register_buffer::call(0, buf, REGBUF_WILL_INIT)?;
        let loc = rel_locator(&seqrel);
        backend_access_transam_xloginsert_seams::xlog_register_data::call(&serialize_locator(&loc))?;
        backend_access_transam_xloginsert_seams::xlog_register_data::call(&logged_item)?;
        let recptr = backend_access_transam_xloginsert_seams::xlog_insert_record::call(
            RM_SEQ_ID,
            XLOG_SEQ_LOG,
        )?;
        backend_storage_buffer_bufmgr_seams::page_set_lsn::call(buf, recptr)?;
    }

    /* Now update sequence tuple to the intended final state */
    let finalstate = FormData_pg_sequence_data {
        last_value: last,
        is_called: true,
        log_cnt: log,
    };
    write_seq_data_into_buffer(mcx, &seqrel, buf, &finalstate)?;

    // END_CRIT_SECTION();
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
    // sequence_close(seqrel, NoLock): close the RAII handle directly (a by-OID
    // close plus the handle's Drop would double-decrement the pin).
    seqrel.close(NoLock)?;

    Ok(result)
}

/// Overwrite the three sequence-data columns of the in-buffer tuple at
/// `FirstOffsetNumber`, returning the resulting on-page item image (for
/// `XLogRegisterData`). Mirrors the C in-place `seq->...` writes on `t_data`.
fn write_seq_data_into_buffer<'mcx>(
    mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    buf: Buffer,
    data: &FormData_pg_sequence_data,
) -> PgResult<Vec<u8>> {
    let mut image: Vec<u8> = Vec::new();
    backend_storage_buffer_bufmgr_seams::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let (item_off, item_len, t_hoff) = {
            let pref = backend_storage_page::PageRef::new(page)?;
            let lp = backend_storage_page::PageGetItemId(&pref, FIRST_OFFSET_NUMBER)?;
            let item = backend_storage_page::PageGetItem(&pref, &lp)?;
            let off = item.as_ptr() as usize - page.as_ptr() as usize;
            let len = backend_storage_page::ItemIdGetLength(&lp) as usize;
            // t_hoff lives in the on-page header (byte 22).
            let hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
            (off, len, hdr.t_hoff as usize)
        };
        let user = &mut page[item_off + t_hoff..item_off + item_len];
        encode_seq_data(user, data);
        image.extend_from_slice(&page[item_off..item_off + item_len]);
        Ok(())
    })?;
    Ok(image)
}

// ===========================================================================
// currval_oid  (C lines 865-894)
// ===========================================================================

/// `currval_oid(PG_FUNCTION_ARGS)` — SQL `currval(regclass)`.
pub fn currval_oid<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let relid = backend_utils_fmgr_fmgr_seams::pg_getarg_oid::call(fcinfo, 0);
    Ok(ValueDatum::from_i64(currval_internal(mcx, relid)?))
}

/// The `currval_oid(PG_FUNCTION_ARGS)` body, factored out so both the fmgr
/// frame entry point and the by-OID builtin adapter can drive it.
pub fn currval_internal<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<i64> {
    let result: i64;

    /* open and lock sequence */
    let seqrel = init_sequence(mcx, relid)?;

    if backend_catalog_aclchk_seams::pg_class_aclcheck::call(
        seqtable_get(relid, |e| e.relid),
        backend_utils_init_miscinit_seams::get_user_id::call(),
        ACL_SELECT | ACL_USAGE,
    )? != ACLCHECK_OK
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for sequence {}", rel_name(&seqrel)))
            .finish(here("currval_oid"))
            .map(|()| 0i64);
    }

    if !seqtable_get(relid, |e| e.last_valid) {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "currval of sequence \"{}\" is not yet defined in this session",
                rel_name(&seqrel)
            ))
            .finish(here("currval_oid"))
            .map(|()| 0i64);
    }

    result = seqtable_get(relid, |e| e.last);

    // sequence_close(seqrel, NoLock): close the RAII handle directly (a by-OID
    // close plus the handle's Drop would double-decrement the pin).
    seqrel.close(NoLock)?;

    Ok(result)
}

// ===========================================================================
// lastval  (C lines 896-929)
// ===========================================================================

/// `lastval(PG_FUNCTION_ARGS)` — SQL `lastval()`.
pub fn lastval<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    Ok(ValueDatum::from_i64(lastval_internal(mcx)?))
}

/// The `lastval(PG_FUNCTION_ARGS)` body, factored out so both the fmgr frame
/// entry point and the by-OID builtin adapter can drive it.
pub fn lastval_internal<'mcx>(mcx: Mcx<'mcx>) -> PgResult<i64> {
    let result: i64;

    let last_relid = match LAST_USED_SEQ.with(|s| *s.borrow()) {
        Some(r) => r,
        None => {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("lastval is not yet defined in this session")
                .finish(here("lastval"))
                .map(|()| 0i64);
        }
    };

    /* Someone may have dropped the sequence since the last nextval() */
    if !backend_utils_cache_syscache_seams::search_syscache_exists_reloid::call(last_relid)? {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("lastval is not yet defined in this session")
            .finish(here("lastval"))
            .map(|()| 0i64);
    }

    let seqrel = lock_and_open_sequence(mcx, last_relid)?;

    /* nextval() must have already been called for this sequence */
    debug_assert!(seqtable_get(last_relid, |e| e.last_valid));

    if backend_catalog_aclchk_seams::pg_class_aclcheck::call(
        seqtable_get(last_relid, |e| e.relid),
        backend_utils_init_miscinit_seams::get_user_id::call(),
        ACL_SELECT | ACL_USAGE,
    )? != ACLCHECK_OK
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for sequence {}", rel_name(&seqrel)))
            .finish(here("lastval"))
            .map(|()| 0i64);
    }

    result = seqtable_get(last_relid, |e| e.last);
    // sequence_close(seqrel, NoLock): close the RAII handle directly (a by-OID
    // close plus the handle's Drop would double-decrement the pin).
    seqrel.close(NoLock)?;

    Ok(result)
}

// ===========================================================================
// do_setval / setval_oid / setval3_oid  (C lines 944-1073)
// ===========================================================================

/// `do_setval(relid, next, iscalled)` — handles the 2 & 3 arg SETVAL forms.
fn do_setval<'mcx>(mcx: Mcx<'mcx>, relid: Oid, next: i64, iscalled: bool) -> PgResult<()> {
    /* open and lock sequence */
    let seqrel = init_sequence(mcx, relid)?;

    if backend_catalog_aclchk_seams::pg_class_aclcheck::call(
        seqtable_get(relid, |e| e.relid),
        backend_utils_init_miscinit_seams::get_user_id::call(),
        ACL_UPDATE,
    )? != ACLCHECK_OK
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for sequence {}", rel_name(&seqrel)))
            .finish(here("do_setval"));
    }

    let (maxv, minv) = match backend_utils_cache_syscache_seams::search_seqrelid::call(relid)? {
        Some(p) => (p.seqmax, p.seqmin),
        None => return elog_cache_lookup_failed(relid, "do_setval"),
    };

    /* read-only transactions may only modify temp sequences */
    if !rel_is_localtemp(&seqrel) {
        xact::prevent_command_if_read_only::call("setval()")?;
    }

    xact::prevent_command_if_parallel_mode::call("setval()")?;

    /* lock page buffer and read tuple */
    let (buf, _seq_read) = read_seq_tuple(mcx, &seqrel)?;

    if (next < minv) || (next > maxv) {
        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
        return ereport(ERROR)
            .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!(
                "setval: value {} is out of bounds for sequence \"{}\" ({}..{})",
                next,
                rel_name(&seqrel),
                minv,
                maxv
            ))
            .finish(here("do_setval"));
    }

    /* Set the currval() state only if iscalled = true */
    if iscalled {
        seqtable_with(relid, |e| {
            e.last = next;
            e.last_valid = true;
        });
    }

    /* In any case, forget any future cached numbers */
    seqtable_with(relid, |e| e.cached = e.last);

    /* check the comment above nextval_internal()'s equivalent call. */
    if rel_needs_wal(&seqrel) {
        xact::get_top_transaction_id::call()?;
    }

    // START_CRIT_SECTION();
    let newstate = FormData_pg_sequence_data {
        last_value: next,
        is_called: iscalled,
        log_cnt: 0,
    };
    let item = write_seq_data_into_buffer(mcx, &seqrel, buf, &newstate)?;

    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);

    /* XLOG stuff */
    if rel_needs_wal(&seqrel) {
        backend_access_transam_xloginsert_seams::xlog_begin_insert::call()?;
        backend_access_transam_xloginsert_seams::xlog_register_buffer::call(0, buf, REGBUF_WILL_INIT)?;
        let loc = rel_locator(&seqrel);
        backend_access_transam_xloginsert_seams::xlog_register_data::call(&serialize_locator(&loc))?;
        backend_access_transam_xloginsert_seams::xlog_register_data::call(&item)?;
        let recptr = backend_access_transam_xloginsert_seams::xlog_insert_record::call(
            RM_SEQ_ID,
            XLOG_SEQ_LOG,
        )?;
        backend_storage_buffer_bufmgr_seams::page_set_lsn::call(buf, recptr)?;
    }

    // END_CRIT_SECTION();
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
    // sequence_close(seqrel, NoLock): close the RAII handle directly (a by-OID
    // close plus the handle's Drop would double-decrement the pin).
    seqrel.close(NoLock)?;
    Ok(())
}

/// `setval_oid(PG_FUNCTION_ARGS)` — SQL `setval(regclass, bigint)`.
pub fn setval_oid<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let relid = backend_utils_fmgr_fmgr_seams::pg_getarg_oid::call(fcinfo, 0);
    let next = backend_utils_fmgr_fmgr_seams::pg_getarg_int64::call(fcinfo, 1);

    do_setval(mcx, relid, next, true)?;

    Ok(ValueDatum::from_i64(next))
}

/// `setval3_oid(PG_FUNCTION_ARGS)` — SQL `setval(regclass, bigint, boolean)`.
pub fn setval3_oid<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let relid = backend_utils_fmgr_fmgr_seams::pg_getarg_oid::call(fcinfo, 0);
    let next = backend_utils_fmgr_fmgr_seams::pg_getarg_int64::call(fcinfo, 1);
    let iscalled = backend_utils_fmgr_fmgr_seams::pg_getarg_bool::call(fcinfo, 2);

    do_setval(mcx, relid, next, iscalled)?;

    Ok(ValueDatum::from_i64(next))
}

// ===========================================================================
// lock_and_open_sequence  (C lines 1084-1107)
// ===========================================================================

/// `lock_and_open_sequence(seq)` — open the sequence, acquiring the lock under
/// the top transaction's resource owner if not already held this xact.
fn lock_and_open_sequence<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Relation<'mcx>> {
    let thislxid = backend_storage_lmgr_proc_seams::my_proc_lxid::call();

    /* Get the lock if not already held in this xact */
    let held = seqtable_get(relid, |e| e.lxid == thislxid);
    if !held {
        // CurrentResourceOwner = TopTransactionResourceOwner around
        // LockRelationOid so the lock is owned by the top transaction. The
        // resowner switch is the lmgr owner's concern; the lock acquisition is
        // the observable effect.
        backend_storage_lmgr_lmgr::LockRelationOid(relid, RowExclusiveLock)?;
        /* Flag that we have a lock in the current xact */
        seqtable_with(relid, |e| e.lxid = thislxid);
    }

    /* We now know we have the lock, and can safely open the rel */
    backend_access_sequence_seams::sequence_open::call(mcx, relid, NoLock)
}

// ===========================================================================
// init_sequence  (C lines 1128-1177)
// ===========================================================================

/// `init_sequence(relid, &elm, &rel)` — find-or-create the SeqTable entry,
/// open+lock the sequence, and re-sync the cached relfilenumber.
fn init_sequence<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Relation<'mcx>> {
    /* Find or create a hash table entry for this sequence. */
    SEQHASHTAB.with(|h| {
        let mut b = h.borrow_mut();
        if b.is_none() {
            // create_seq_hashtable()
            *b = Some(HashMap::new());
        }
        let map = b.as_mut().unwrap();
        map.entry(relid).or_insert_with(|| SeqTableData {
            relid,
            filenumber: types_core::InvalidOid, // InvalidRelFileNumber
            lxid: 0,                            // InvalidLocalTransactionId
            last_valid: false,
            last: 0,
            cached: 0,
            increment: 0,
        });
    });

    /* Open the sequence relation. */
    let seqrel = lock_and_open_sequence(mcx, relid)?;

    /*
     * If the sequence has been transactionally replaced since we last saw it,
     * discard any cached-but-unissued values. We do not touch currval() state.
     */
    let cur_filenode = rel_relfilenode(&seqrel);
    seqtable_with(relid, |e| {
        if cur_filenode != e.filenumber {
            e.filenumber = cur_filenode;
            e.cached = e.last;
        }
    });

    Ok(seqrel)
}

// ===========================================================================
// read_seq_tuple  (C lines 1189-1234)
// ===========================================================================

/// `read_seq_tuple(rel, &buf, &seqdatatuple)` — lock the page buffer, validate
/// the magic, return the pinned-and-exclusive-locked buffer plus the decoded
/// sequence-data columns. The hint-bit xmax cleanup is applied in place.
fn read_seq_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<(Buffer, FormData_pg_sequence_data)> {
    let buf = backend_storage_buffer_bufmgr_seams::read_buffer::call(rel, 0)?;
    backend_storage_buffer_bufmgr_seams::lock_buffer_exclusive::call(buf)?;

    let mut needs_dirty_hint = false;
    let mut seq = FormData_pg_sequence_data::default();

    backend_storage_buffer_bufmgr_seams::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        // Validate the special-area magic.
        {
            let pref = backend_storage_page::PageRef::new(page)?;
            let sp = backend_storage_page::PageGetSpecialPointer(&pref)?;
            let magic = u32::from_ne_bytes(sp[0..4].try_into().unwrap());
            if magic != SEQ_MAGIC {
                return ereport(ERROR)
                    .errmsg_internal(format!(
                        "bad magic number in sequence \"{}\": {:08X}",
                        rel_name(rel),
                        magic
                    ))
                    .finish(here("read_seq_tuple"));
            }
        }

        let (item_off, item_len, t_hoff, infomask, raw_xmax) = {
            let pref = backend_storage_page::PageRef::new(page)?;
            let lp = backend_storage_page::PageGetItemId(&pref, FIRST_OFFSET_NUMBER)?;
            let item = backend_storage_page::PageGetItem(&pref, &lp)?;
            let off = item.as_ptr() as usize - page.as_ptr() as usize;
            let len = backend_storage_page::ItemIdGetLength(&lp) as usize;
            let hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
            let raw_xmax = match hdr.t_choice {
                HeapTupleHeaderChoice::THeap(f) => f.t_xmax,
                _ => 0,
            };
            (off, len, hdr.t_hoff as usize, hdr.t_infomask, raw_xmax)
        };

        debug_assert!((infomask & HEAP_XMAX_IS_MULTI) == 0);

        // SELECT FOR UPDATE leftover cleanup: clear a non-frozen xmax in place.
        if raw_xmax != types_core::xact::InvalidTransactionId {
            let mut hdr =
                HeapTupleHeaderData::read_on_page(mcx, &page[item_off..item_off + item_len])?;
            set_header_xmax(&mut hdr, types_core::xact::InvalidTransactionId);
            hdr.t_infomask &= !HEAP_XMAX_COMMITTED;
            hdr.t_infomask |= HEAP_XMAX_INVALID;
            hdr.write_on_page(&mut page[item_off..item_off + ON_PAGE_HEADER_SIZE])?;
            needs_dirty_hint = true;
        }

        // Decode the three sequence-data columns from the user-data area.
        let user = &page[item_off + t_hoff..item_off + item_len];
        seq = decode_seq_data(user);
        Ok(())
    })?;

    if needs_dirty_hint {
        backend_storage_buffer_bufmgr_seams::mark_buffer_dirty_hint::call(buf, true);
    }

    Ok((buf, seq))
}

// ===========================================================================
// init_params  (C lines 1256-1582)
// ===========================================================================

/// `init_params(...)` — process the options list of CREATE or ALTER SEQUENCE.
fn init_params(
    pstate: &ParseState<'_>,
    options: &[types_nodes::nodes::NodePtr<'_>],
    for_identity: bool,
    is_init: bool,
    seqform: &mut FormData_pg_sequence,
    seqdataform: &mut FormData_pg_sequence_data,
    need_seq_rewrite: &mut bool,
    owned_by: &mut Vec<String>,
) -> PgResult<()> {
    let mut as_type: Option<&DefElem> = None;
    let mut start_value: Option<&DefElem> = None;
    let mut restart_value: Option<&DefElem> = None;
    let mut increment_by: Option<&DefElem> = None;
    let mut max_value: Option<&DefElem> = None;
    let mut min_value: Option<&DefElem> = None;
    let mut cache_value: Option<&DefElem> = None;
    let mut is_cycled: Option<&DefElem> = None;
    let mut reset_max_value = false;
    let mut reset_min_value = false;

    *need_seq_rewrite = false;
    owned_by.clear();

    for option in options {
        let defel = as_defelem(option);
        let defname = defel.defname.as_deref().unwrap_or("");
        if defname == "as" {
            if as_type.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            as_type = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "increment" {
            if increment_by.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            increment_by = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "start" {
            if start_value.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            start_value = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "restart" {
            if restart_value.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            restart_value = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "maxvalue" {
            if max_value.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            max_value = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "minvalue" {
            if min_value.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            min_value = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "cache" {
            if cache_value.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            cache_value = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "cycle" {
            if is_cycled.is_some() {
                return error_conflicting_def_elem(defel, pstate);
            }
            is_cycled = Some(defel);
            *need_seq_rewrite = true;
        } else if defname == "owned_by" {
            if !owned_by.is_empty() {
                return error_conflicting_def_elem(defel, pstate);
            }
            *owned_by = def_get_qualified_name(defel)?;
        } else if defname == "sequence_name" {
            /*
             * The parser allows this, but it is only for identity columns,
             * filtered out in parse_utilcmd.c. Redundant in CREATE SEQUENCE.
             */
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("invalid sequence option SEQUENCE NAME")
                .errposition(defel.location)
                .finish(here("init_params"));
        } else {
            return ereport(ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .finish(here("init_params"));
        }
    }

    /*
     * We must reset log_cnt when isInit or when changing any parameters that
     * would affect future nextval allocations.
     */
    if is_init {
        seqdataform.log_cnt = 0;
    }

    /* AS type */
    if let Some(as_type) = as_type {
        // typenameTypeId(pstate, defGetTypeName(as_type))
        let newtypid =
            backend_parser_parse_type_seams::typename_type_id_from_defelem::call(as_type)?;

        if newtypid != INT2OID && newtypid != INT4OID && newtypid != INT8OID {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(if for_identity {
                    "identity column type must be smallint, integer, or bigint"
                } else {
                    "sequence type must be smallint, integer, or bigint"
                })
                .finish(here("init_params"));
        }

        if !is_init {
            if (seqform.seqtypid == INT2OID && seqform.seqmax == PG_INT16_MAX)
                || (seqform.seqtypid == INT4OID && seqform.seqmax == PG_INT32_MAX)
                || (seqform.seqtypid == INT8OID && seqform.seqmax == PG_INT64_MAX)
            {
                reset_max_value = true;
            }
            if (seqform.seqtypid == INT2OID && seqform.seqmin == PG_INT16_MIN)
                || (seqform.seqtypid == INT4OID && seqform.seqmin == PG_INT32_MIN)
                || (seqform.seqtypid == INT8OID && seqform.seqmin == PG_INT64_MIN)
            {
                reset_min_value = true;
            }
        }

        seqform.seqtypid = newtypid;
    } else if is_init {
        seqform.seqtypid = INT8OID;
    }

    /* INCREMENT BY */
    if let Some(increment_by) = increment_by {
        seqform.seqincrement = def_get_int64(increment_by)?;
        if seqform.seqincrement == 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("INCREMENT must not be zero")
                .finish(here("init_params"));
        }
        seqdataform.log_cnt = 0;
    } else if is_init {
        seqform.seqincrement = 1;
    }

    /* CYCLE */
    if let Some(is_cycled) = is_cycled {
        // boolVal(is_cycled->arg)
        let arg = is_cycled
            .arg
            .as_deref()
            .expect("sequence.c: CYCLE option has no arg");
        seqform.seqcycle = bool_val(arg);
        seqdataform.log_cnt = 0;
    } else if is_init {
        seqform.seqcycle = false;
    }

    /* MAXVALUE (null arg means NO MAXVALUE) */
    if let Some(mv) = max_value.filter(|d| d.arg.is_some()) {
        seqform.seqmax = def_get_int64(mv)?;
        seqdataform.log_cnt = 0;
    } else if is_init || max_value.is_some() || reset_max_value {
        if seqform.seqincrement > 0 || reset_max_value {
            /* ascending seq */
            if seqform.seqtypid == INT2OID {
                seqform.seqmax = PG_INT16_MAX;
            } else if seqform.seqtypid == INT4OID {
                seqform.seqmax = PG_INT32_MAX;
            } else {
                seqform.seqmax = PG_INT64_MAX;
            }
        } else {
            seqform.seqmax = -1; /* descending seq */
        }
        seqdataform.log_cnt = 0;
    }

    /* Validate maximum value. No need to check INT8 as seqmax is an int64 */
    if (seqform.seqtypid == INT2OID
        && (seqform.seqmax < PG_INT16_MIN || seqform.seqmax > PG_INT16_MAX))
        || (seqform.seqtypid == INT4OID
            && (seqform.seqmax < PG_INT32_MIN || seqform.seqmax > PG_INT32_MAX))
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "MAXVALUE ({}) is out of range for sequence data type {}",
                seqform.seqmax,
                format_type_be(seqform.seqtypid)?
            ))
            .finish(here("init_params"));
    }

    /* MINVALUE (null arg means NO MINVALUE) */
    if let Some(mv) = min_value.filter(|d| d.arg.is_some()) {
        seqform.seqmin = def_get_int64(mv)?;
        seqdataform.log_cnt = 0;
    } else if is_init || min_value.is_some() || reset_min_value {
        if seqform.seqincrement < 0 || reset_min_value {
            /* descending seq */
            if seqform.seqtypid == INT2OID {
                seqform.seqmin = PG_INT16_MIN;
            } else if seqform.seqtypid == INT4OID {
                seqform.seqmin = PG_INT32_MIN;
            } else {
                seqform.seqmin = PG_INT64_MIN;
            }
        } else {
            seqform.seqmin = 1; /* ascending seq */
        }
        seqdataform.log_cnt = 0;
    }

    /* Validate minimum value. No need to check INT8 as seqmin is an int64 */
    if (seqform.seqtypid == INT2OID
        && (seqform.seqmin < PG_INT16_MIN || seqform.seqmin > PG_INT16_MAX))
        || (seqform.seqtypid == INT4OID
            && (seqform.seqmin < PG_INT32_MIN || seqform.seqmin > PG_INT32_MAX))
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "MINVALUE ({}) is out of range for sequence data type {}",
                seqform.seqmin,
                format_type_be(seqform.seqtypid)?
            ))
            .finish(here("init_params"));
    }

    /* crosscheck min/max */
    if seqform.seqmin >= seqform.seqmax {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "MINVALUE ({}) must be less than MAXVALUE ({})",
                seqform.seqmin, seqform.seqmax
            ))
            .finish(here("init_params"));
    }

    /* START WITH */
    if let Some(start_value) = start_value {
        seqform.seqstart = def_get_int64(start_value)?;
    } else if is_init {
        if seqform.seqincrement > 0 {
            seqform.seqstart = seqform.seqmin; /* ascending seq */
        } else {
            seqform.seqstart = seqform.seqmax; /* descending seq */
        }
    }

    /* crosscheck START */
    if seqform.seqstart < seqform.seqmin {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "START value ({}) cannot be less than MINVALUE ({})",
                seqform.seqstart, seqform.seqmin
            ))
            .finish(here("init_params"));
    }
    if seqform.seqstart > seqform.seqmax {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "START value ({}) cannot be greater than MAXVALUE ({})",
                seqform.seqstart, seqform.seqmax
            ))
            .finish(here("init_params"));
    }

    /* RESTART [WITH] */
    if let Some(restart_value) = restart_value {
        if restart_value.arg.is_some() {
            seqdataform.last_value = def_get_int64(restart_value)?;
        } else {
            seqdataform.last_value = seqform.seqstart;
        }
        seqdataform.is_called = false;
        seqdataform.log_cnt = 0;
    } else if is_init {
        seqdataform.last_value = seqform.seqstart;
        seqdataform.is_called = false;
    }

    /* crosscheck RESTART (or current value, if changing MIN/MAX) */
    if seqdataform.last_value < seqform.seqmin {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "RESTART value ({}) cannot be less than MINVALUE ({})",
                seqdataform.last_value, seqform.seqmin
            ))
            .finish(here("init_params"));
    }
    if seqdataform.last_value > seqform.seqmax {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "RESTART value ({}) cannot be greater than MAXVALUE ({})",
                seqdataform.last_value, seqform.seqmax
            ))
            .finish(here("init_params"));
    }

    /* CACHE */
    if let Some(cache_value) = cache_value {
        seqform.seqcache = def_get_int64(cache_value)?;
        if seqform.seqcache <= 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "CACHE ({}) must be greater than zero",
                    seqform.seqcache
                ))
                .finish(here("init_params"));
        }
        seqdataform.log_cnt = 0;
    } else if is_init {
        seqform.seqcache = 1;
    }

    Ok(())
}


// ===========================================================================
// process_owned_by  (C lines 1592-1700)
// ===========================================================================

/// `defGetInt64(def)` (define.c): the `DefElem`'s value as an `int64`. Reads the
/// canonical value node directly (`T_Integer` → `intVal`; `T_Float` → parse the
/// literal text as int8). `Err(ERRCODE_SYNTAX_ERROR)` for `NULL`/other tags.
fn def_get_int64(def: &DefElem<'_>) -> PgResult<i64> {
    let defname = def.defname.as_deref().unwrap_or("");
    match def.arg.as_deref().map(|n| n.node_tag()) {
        Some(ntag::T_Integer) => Ok(def.arg.as_deref().unwrap().expect_integer().ival as i64),
        Some(ntag::T_Float) => def
            .arg
            .as_deref()
            .unwrap()
            .expect_float()
            .fval
            .as_str()
            .trim()
            .parse::<i64>()
            .map_err(|_| {
                ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("{defname} requires a numeric value"))
                    .finish(here("defGetInt64"))
                    .unwrap_err()
            }),
        _ => ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{defname} requires a numeric value"))
            .finish(here("defGetInt64"))
            .map(|()| 0),
    }
}

/// `defGetQualifiedName(def)` (define.c): the `DefElem`'s value as a
/// possibly-qualified name (`T_TypeName.names`, `T_List` of `String`, or a
/// single `T_String`), returning the name components.
fn def_get_qualified_name(def: &DefElem<'_>) -> PgResult<Vec<String>> {
    let defname = def.defname.as_deref().unwrap_or("");
    let strval = |n: &Node<'_>| -> Option<String> {
        n.as_string().map(|s| s.sval.as_str().to_string())
    };
    match def.arg.as_deref().map(|n| n.node_tag()) {
        Some(ntag::T_TypeName) => Ok(def
            .arg
            .as_deref()
            .unwrap()
            .expect_typename()
            .names
            .iter()
            .filter_map(|n| strval(n))
            .collect()),
        Some(ntag::T_List) => Ok(def
            .arg
            .as_deref()
            .unwrap()
            .expect_list()
            .iter()
            .filter_map(|n| strval(n))
            .collect()),
        Some(ntag::T_String) => Ok(vec![def
            .arg
            .as_deref()
            .unwrap()
            .expect_string()
            .sval
            .as_str()
            .to_string()]),
        _ => ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("argument of {defname} must be a name"))
            .finish(here("defGetQualifiedName"))
            .map(|()| Vec::new()),
    }
}

/// `errorConflictingDefElem(defel, pstate)` (define.c): raise
/// `ERRCODE_SYNTAX_ERROR` "conflicting or redundant options" at the option's
/// parse location.
fn error_conflicting_def_elem(def: &DefElem<'_>, _pstate: &ParseState<'_>) -> PgResult<()> {
    // parser_errposition(pstate, defel->location): the error position is the
    // option's parse location; attached via errposition.
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(def.location)
        .finish(here("errorConflictingDefElem"))
}

/// `process_owned_by(seqrel, owned_by, for_identity)` — OWNED BY processing.
fn process_owned_by<'mcx>(
    mcx: Mcx<'mcx>,
    seqrel: &Relation<'mcx>,
    owned_by: &[String],
    for_identity: bool,
) -> PgResult<()> {
    let deptype = if for_identity {
        DEPENDENCY_INTERNAL
    } else {
        DEPENDENCY_AUTO
    };

    let nnames = owned_by.len();
    debug_assert!(nnames > 0);

    let tablerel: Option<Relation<'mcx>>;
    let tablerel_oid: Oid;
    let attnum: i16;

    if nnames == 1 {
        /* Must be OWNED BY NONE */
        if owned_by[0] != "none" {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("invalid OWNED BY option")
                .errhint("Specify OWNED BY table.column or OWNED BY NONE.")
                .finish(here("process_owned_by"));
        }
        tablerel = None;
        tablerel_oid = types_core::InvalidOid;
        attnum = 0;
    } else {
        /* Separate relname and attr name */
        let relname: Vec<&str> = owned_by[..nnames - 1].iter().map(|s| s.as_str()).collect();
        let attrname = &owned_by[nnames - 1];

        /* Open and lock rel to ensure it won't go away meanwhile */
        let rv = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&relname)?;
        // relation_openrv(rel, AccessShareLock): resolve the RangeVar and open.
        let trelid =
            backend_catalog_namespace_seams::range_var_get_relid::call(mcx, &rv, AccessShareLock, false)?;
        let trel = backend_access_table_table_seams::table_open::call(mcx, trelid, NoLock)?;

        /* Must be a regular or foreign table (or view / partitioned table) */
        let relkind = rel_relkind(&trel);
        if !(relkind == RELKIND_RELATION
            || relkind == RELKIND_FOREIGN_TABLE
            || relkind == RELKIND_VIEW
            || relkind == RELKIND_PARTITIONED_TABLE)
        {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "sequence cannot be owned by relation \"{}\"",
                    rel_name(&trel)
                ))
                .finish(here("process_owned_by"));
        }

        /* We insist on same owner and schema */
        if rel_relowner(seqrel) != rel_relowner(&trel) {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("sequence must have same owner as table it is linked to")
                .finish(here("process_owned_by"));
        }
        if rel_relnamespace(seqrel) != rel_relnamespace(&trel) {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("sequence must be in same schema as table it is linked to")
                .finish(here("process_owned_by"));
        }

        /* Now, fetch the attribute number from the system cache */
        let an = backend_utils_cache_lsyscache_seams::get_attnum::call(rel_relid(&trel), attrname)?;
        if an == InvalidAttrNumber {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    attrname,
                    rel_name(&trel)
                ))
                .finish(here("process_owned_by"));
        }
        attnum = an;
        tablerel_oid = trelid;
        tablerel = Some(trel);
    }

    /*
     * Catch user explicitly running OWNED BY on identity sequence.
     */
    if deptype == DEPENDENCY_AUTO {
        if let Some((table_id, _col_id)) =
            backend_catalog_dependency_seams::sequence_is_owned::call(rel_relid(seqrel), DEPENDENCY_INTERNAL)?
        {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot change ownership of identity sequence")
                .errdetail(format!(
                    "Sequence \"{}\" is linked to table \"{}\".",
                    rel_name(seqrel),
                    get_rel_name_or_qmarks(mcx, table_id)?
                ))
                .finish(here("process_owned_by"));
        }
    }

    /*
     * OK, we are ready to update pg_depend. First remove any existing
     * dependencies for the sequence, then optionally add a new one.
     */
    backend_catalog_dependency_seams::delete_dependency_records_for_class::call(
        RelationRelationId,
        rel_relid(seqrel),
        RelationRelationId,
        deptype,
    )?;

    if let Some(ref trel) = tablerel {
        let refobject = ObjectAddress {
            classId: RelationRelationId,
            objectId: rel_relid(trel),
            objectSubId: attnum as i32,
        };
        let depobject = ObjectAddress {
            classId: RelationRelationId,
            objectId: rel_relid(seqrel),
            objectSubId: 0,
        };
        backend_catalog_dependency_seams::record_dependency_on::call(depobject, refobject, deptype)?;
    }

    /* Done, but hold lock until commit */
    // relation_close(tablerel, NoLock): close the handle opened above. Closing
    // the RAII handle directly (rather than a second by-OID `relation_close`)
    // keeps the relcache refcount balanced — the by-OID close plus `tablerel`'s
    // Drop would decrement `rd_refcnt` twice, underflowing the owning table's
    // pin so a later DROP's `CheckTableNotInUse` reports it as still in use.
    let _ = tablerel_oid;
    if let Some(trel) = tablerel {
        trel.close(NoLock)?;
    }

    Ok(())
}

// ===========================================================================
// sequence_options  (C lines 1706-1735)
// ===========================================================================

/// `sequence_options(relid)` — return sequence parameters as a list of
/// `DefElem` nodes (the parser-created form). Allocated in `mcx`.
pub fn sequence_options<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>> {
    let pgsform = match backend_utils_cache_syscache_seams::search_seqrelid::call(relid)? {
        Some(p) => p,
        None => {
            elog_cache_lookup_failed(relid, "sequence_options")?;
            unreachable!()
        }
    };

    let mut options = mcx::vec_with_capacity_in(mcx, 6)?;

    // Use makeFloat() for 64-bit integers, like gram.y does.
    push_def_float(mcx, &mut options, "cache", pgsform.seqcache)?;
    push_def_bool(mcx, &mut options, "cycle", pgsform.seqcycle)?;
    push_def_float(mcx, &mut options, "increment", pgsform.seqincrement)?;
    push_def_float(mcx, &mut options, "maxvalue", pgsform.seqmax)?;
    push_def_float(mcx, &mut options, "minvalue", pgsform.seqmin)?;
    push_def_float(mcx, &mut options, "start", pgsform.seqstart)?;

    Ok(options)
}

fn push_def_float<'mcx>(
    mcx: Mcx<'mcx>,
    options: &mut mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
    name: &str,
    val: i64,
) -> PgResult<()> {
    // makeDefElem(name, (Node *) makeFloat(psprintf(INT64_FORMAT, val)), -1)
    let float = Node::mk_float(mcx, types_nodes::value::Float {
        fval: mcx::PgString::from_str_in(&val.to_string(), mcx)?,
    });
    let de = DefElem {
        defnamespace: None,
        defname: Some(mcx::PgString::from_str_in(name, mcx)?),
        arg: Some(mcx::alloc_in(mcx, float)?),
        defaction: types_nodes::ddlnodes::DefElemAction::DEFELEM_UNSPEC,
        location: -1,
    };
    let node = mcx::alloc_in(mcx, Node::mk_def_elem(mcx, de))?;
    options.push(node);
    Ok(())
}

fn push_def_bool<'mcx>(
    mcx: Mcx<'mcx>,
    options: &mut mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
    name: &str,
    val: bool,
) -> PgResult<()> {
    // makeDefElem(name, (Node *) makeBoolean(val), -1)
    let b = Node::mk_boolean(mcx, types_nodes::value::Boolean { boolval: val });
    let de = DefElem {
        defnamespace: None,
        defname: Some(mcx::PgString::from_str_in(name, mcx)?),
        arg: Some(mcx::alloc_in(mcx, b)?),
        defaction: types_nodes::ddlnodes::DefElemAction::DEFELEM_UNSPEC,
        location: -1,
    };
    let node = mcx::alloc_in(mcx, Node::mk_def_elem(mcx, de))?;
    options.push(node);
    Ok(())
}

// ===========================================================================
// pg_sequence_parameters  (C lines 1740-1777)
// ===========================================================================

/// `pg_sequence_parameters(PG_FUNCTION_ARGS)`.
pub fn pg_sequence_parameters<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let relid = backend_utils_fmgr_fmgr_seams::pg_getarg_oid::call(fcinfo, 0);

    if backend_catalog_aclchk_seams::pg_class_aclcheck::call(
        relid,
        backend_utils_init_miscinit_seams::get_user_id::call(),
        ACL_SELECT | ACL_UPDATE | ACL_USAGE,
    )? != ACLCHECK_OK
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied for sequence {}",
                get_rel_name_or_qmarks(mcx, relid)?
            ))
            .finish(here("pg_sequence_parameters"))
            .map(|()| ValueDatum::null());
    }

    let pgsform = match backend_utils_cache_syscache_seams::search_seqrelid::call(relid)? {
        Some(p) => p,
        None => {
            return elog_cache_lookup_failed(relid, "pg_sequence_parameters")
                .map(|()| ValueDatum::null())
        }
    };

    let coltypes = [INT8OID, INT8OID, INT8OID, INT8OID, BOOLOID, INT8OID, OIDOID];
    let values = [
        ValueDatum::from_i64(pgsform.seqstart),
        ValueDatum::from_i64(pgsform.seqmin),
        ValueDatum::from_i64(pgsform.seqmax),
        ValueDatum::from_i64(pgsform.seqincrement),
        ValueDatum::from_bool(pgsform.seqcycle),
        ValueDatum::from_i64(pgsform.seqcache),
        ValueDatum::from_oid(pgsform.seqtypid),
    ];
    let isnull = [false; 7];

    let datum =
        backend_utils_fmgr_funcapi_seams::record_from_values::call(mcx, &coltypes, &values, &isnull)?;
    Ok(datum)
}

// ===========================================================================
// pg_get_sequence_data  (C lines 1786-1838)
// ===========================================================================

/// `pg_get_sequence_data(PG_FUNCTION_ARGS)`.
pub fn pg_get_sequence_data<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let relid = backend_utils_fmgr_fmgr_seams::pg_getarg_oid::call(fcinfo, 0);

    let coltypes = [INT8OID, BOOLOID];
    let mut values = [ValueDatum::from_i64(0), ValueDatum::from_bool(false)];
    let mut isnull = [false; 2];

    // seqrel = try_relation_open(relid, AccessShareLock);  -- returns NULL only
    // for a missing relation; other errors propagate.
    let seqrel =
        backend_access_common_relation_seams::try_relation_open::call(mcx, relid, AccessShareLock)?;

    if let Some(ref seqrel) = seqrel {
        if rel_relkind(seqrel) == types_tuple::access::RELKIND_SEQUENCE
            && backend_catalog_aclchk_seams::pg_class_aclcheck::call(
                relid,
                backend_utils_init_miscinit_seams::get_user_id::call(),
                ACL_SELECT,
            )? == ACLCHECK_OK
            && !rel_is_other_temp(seqrel)
            && (rel_is_permanent(seqrel)
                || !backend_access_transam_xlog_seams::recovery_in_progress::call())
        {
            let (buf, seq) = read_seq_tuple(mcx, seqrel)?;
            values[0] = ValueDatum::from_i64(seq.last_value);
            values[1] = ValueDatum::from_bool(seq.is_called);
            backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
        } else {
            isnull = [true; 2];
        }
    } else {
        isnull = [true; 2];
    }

    // if (seqrel) relation_close(seqrel, AccessShareLock);
    if let Some(seqrel) = seqrel {
        seqrel.close(AccessShareLock)?;
    }

    let datum =
        backend_utils_fmgr_funcapi_seams::record_from_values::call(mcx, &coltypes, &values, &isnull)?;
    Ok(datum)
}

// ===========================================================================
// pg_sequence_last_value  (C lines 1846-1888)
// ===========================================================================

/// `pg_sequence_last_value(PG_FUNCTION_ARGS)`.
pub fn pg_sequence_last_value<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<ValueDatum<'mcx>> {
    let mcx = pg_call_mcx::call(fcinfo);
    let relid = backend_utils_fmgr_fmgr_seams::pg_getarg_oid::call(fcinfo, 0);
    let mut is_called = false;
    let mut result: i64 = 0;

    /* open and lock sequence */
    let seqrel = init_sequence(mcx, relid)?;

    if backend_catalog_aclchk_seams::pg_class_aclcheck::call(
        relid,
        backend_utils_init_miscinit_seams::get_user_id::call(),
        ACL_SELECT | ACL_USAGE,
    )? == ACLCHECK_OK
        && !rel_is_other_temp(&seqrel)
        && (rel_is_permanent(&seqrel)
            || !backend_access_transam_xlog_seams::recovery_in_progress::call())
    {
        let (buf, seq) = read_seq_tuple(mcx, &seqrel)?;
        is_called = seq.is_called;
        result = seq.last_value;
        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
    }
    // sequence_close(seqrel, NoLock): close the RAII handle directly (a by-OID
    // close plus the handle's Drop would double-decrement the pin).
    seqrel.close(NoLock)?;

    if is_called {
        Ok(ValueDatum::from_i64(result))
    } else {
        Ok(ValueDatum::null())
    }
}

// ===========================================================================
// seq_redo  (C lines 1891-1939)
// ===========================================================================

/// `seq_redo(record)` — WAL replay of an `XLOG_SEQ_LOG` record.
pub fn seq_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let info = record_get_info(record) & !XLR_INFO_MASK;

    if info != XLOG_SEQ_LOG {
        return ereport(PANIC)
            .errmsg_internal(format!("seq_redo: unknown op code {info}"))
            .finish(here("seq_redo"));
    }

    let xlrec_data = record_get_data(record);
    let data_len = xlrec_data.len();

    let buffer = backend_access_transam_xlogutils_seams::xlog_init_buffer_for_redo::call(record, 0)?;

    /*
     * Build the correct new page contents in local workspace and then memcpy
     * into the buffer, so a concurrent hot-standby reader never sees a
     * transiently-trashed page.
     */
    let item = &xlrec_data[SIZEOF_XL_SEQ_REC..];
    let _itemsz = data_len - SIZEOF_XL_SEQ_REC;

    let mut localpage = vec![0u8; BLCKSZ];
    backend_storage_page::PageInit(&mut localpage, BLCKSZ, 4)?;
    {
        // sm->magic = SEQ_MAGIC
        let pref = backend_storage_page::PageRef::new(&localpage)?;
        let off = backend_storage_page::PageGetSpecialPointer(&pref)?.as_ptr() as usize
            - localpage.as_ptr() as usize;
        localpage[off..off + 4].copy_from_slice(&SEQ_MAGIC.to_ne_bytes());
    }
    {
        let mut pm = backend_storage_page::PageMut::new(&mut localpage)?;
        let off = backend_storage_page::PageAddItemExtended(&mut pm, item, FIRST_OFFSET_NUMBER, 0)?;
        if off == INVALID_OFFSET_NUMBER {
            return ereport(PANIC)
                .errmsg_internal("seq_redo: failed to add item to page")
                .finish(here("seq_redo"));
        }
    }
    {
        let mut pm = backend_storage_page::PageMut::new(&mut localpage)?;
        backend_storage_page::PageSetLSN(&mut pm, lsn);
    }

    backend_storage_buffer_bufmgr_seams::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        page.copy_from_slice(&localpage);
        Ok(())
    })?;
    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buffer);
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buffer);

    Ok(())
}

fn record_get_data<'a>(record: &'a types_wal::rmgr::XLogReaderState<'_>) -> &'a [u8] {
    record.record.as_ref().map(|r| r.data()).unwrap_or(&[])
}

fn record_get_info(record: &types_wal::rmgr::XLogReaderState<'_>) -> u8 {
    record.record.as_ref().map(|r| r.info()).unwrap_or(0)
}

// ===========================================================================
// ResetSequenceCaches  (C lines 1944-1954)
// ===========================================================================

/// `ResetSequenceCaches()` — DISCARD SEQUENCES: drop the per-backend cache.
pub fn ResetSequenceCaches() -> PgResult<()> {
    SEQHASHTAB.with(|h| *h.borrow_mut() = None);
    LAST_USED_SEQ.with(|s| *s.borrow_mut() = None);
    Ok(())
}

// ===========================================================================
// seq_mask  (C lines 1959-1965)
// ===========================================================================

/// `seq_mask(page, blkno)` — mask a sequence page for WAL consistency checking.
pub fn seq_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    backend_access_common_bufmask_seams::mask_page_lsn_and_checksum::call(pagedata);
    backend_access_common_bufmask_seams::mask_unused_space::call(pagedata)?;
    Ok(())
}

// ===========================================================================
// Relation field helpers (RelationGet* macros)
// ===========================================================================

fn rel_relid(rel: &Relation<'_>) -> Oid {
    rel.rd_id
}
fn rel_relkind(rel: &Relation<'_>) -> u8 {
    rel.rd_rel.relkind
}
fn rel_relpersistence(rel: &Relation<'_>) -> u8 {
    rel.rd_rel.relpersistence
}
fn rel_relowner(rel: &Relation<'_>) -> Oid {
    rel.rd_rel.relowner
}
fn rel_relnamespace(rel: &Relation<'_>) -> Oid {
    rel.rd_rel.relnamespace
}
fn rel_relfilenode(rel: &Relation<'_>) -> Oid {
    rel.rd_rel.relfilenode
}
fn rel_locator(rel: &Relation<'_>) -> types_storage::RelFileLocator {
    rel.rd_locator
}
fn rel_name(rel: &Relation<'_>) -> String {
    rel.rd_rel.relname.as_str().to_string()
}
fn rangevar_relname(rv: &RangeVar<'_>) -> String {
    rv.relname.as_deref().unwrap_or("").to_string()
}
/// `RelationNeedsWAL` / `RelationIsPermanent`.
fn rel_needs_wal(rel: &Relation<'_>) -> bool {
    rel.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
}
fn rel_is_permanent(rel: &Relation<'_>) -> bool {
    rel.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
}
/// `rel->rd_islocaltemp` ≈ temp persistence owned by this backend.
fn rel_is_localtemp(rel: &Relation<'_>) -> bool {
    rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        && rel.rd_backend == backend_utils_init_small_seams::my_proc_number::call()
}
/// `RELATION_IS_OTHER_TEMP(rel)` — a temp relation owned by another backend.
fn rel_is_other_temp(rel: &Relation<'_>) -> bool {
    rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        && rel.rd_backend != backend_utils_init_small_seams::my_proc_number::call()
}

fn format_type_be(type_oid: Oid) -> PgResult<String> {
    backend_utils_adt_format_type_seams::format_type_be_str::call(type_oid)
}

/// `get_rel_name(relid)` returning "(?)" only if NULL would be printed — C uses
/// `get_rel_name` directly (returns NULL → "%s" prints "(null)"); the owned
/// model returns the name or an empty string the same way the seam yields None.
fn get_rel_name_or_qmarks<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<String> {
    Ok(backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default())
}

/// Convert a detoasted `text` varlena image to a Rust string (drops the 4-byte
/// varlena header; `textToQualifiedNameList`'s input).
fn text_to_str(t: &types_datum::varlena::Bytea<'_>) -> String {
    let bytes = t.as_bytes();
    // VARDATA: skip the 4-byte varlena header.
    let data = if bytes.len() >= 4 { &bytes[4..] } else { bytes };
    String::from_utf8_lossy(data).into_owned()
}

// ===========================================================================
// init_seams
// ===========================================================================

/// `case T_CreateSeqStmt: DefineSequence(pstate, stmt)` (utility.c). The
/// ProcessUtilitySlow dispatch carries the parse tree as `&Node`; extract the
/// `CreateSeqStmt` variant and forward.
fn define_sequence_arm<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &Node<'mcx>,
) -> PgResult<ObjectAddress> {
    let s = match stmt.node_tag() {
        ntag::T_CreateSeqStmt => stmt.expect_createseqstmt(),
        _ => panic!("define_sequence: parse tree is not a CreateSeqStmt"),
    };
    DefineSequence(mcx, pstate, s)
}

/// `case T_AlterSeqStmt: AlterSequence(pstate, stmt)` (utility.c).
fn alter_sequence_arm<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &Node<'mcx>,
) -> PgResult<ObjectAddress> {
    let s = match stmt.node_tag() {
        ntag::T_AlterSeqStmt => stmt.expect_alterseqstmt(),
        _ => panic!("alter_sequence: parse tree is not an AlterSeqStmt"),
    };
    AlterSequence(mcx, pstate, s)
}

/// Install this crate's owned seams.
pub fn init_seams() {
    backend_commands_sequence_seams::seq_redo::set(seq_redo);
    backend_commands_sequence_seams::seq_mask::set(seq_mask);
    backend_commands_sequence_seams::reset_sequence_caches::set(ResetSequenceCaches);
    backend_commands_sequence_seams::DeleteSequenceTuple::set(DeleteSequenceTuple);
    backend_commands_sequence_seams::nextval_internal::set(nextval_internal);

    // ProcessUtilitySlow dispatch arms (utility.c CREATE/ALTER SEQUENCE).
    backend_tcop_utility_out_seams::define_sequence::set(define_sequence_arm);
    backend_tcop_utility_out_seams::alter_sequence::set(alter_sequence_arm);

    // fmgr builtin table: nextval/currval/lastval/setval by-OID dispatch.
    fmgr_builtins::register_sequence_builtins();
}
