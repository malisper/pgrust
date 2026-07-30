//! sequence.c write half: DefineSequence, AlterSequence, nextval/currval/
//! lastval/setval, the backend SeqTable cache, and OWNED BY. Loud (named
//! panics): IF NOT EXISTS.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod builtins;

use std::cell::RefCell;

use datum::Datum;
use mcx::{Mcx, PgFxHashMap};
use sequence_xlog::{SizeOfXlSeqRec, SEQ_MAGIC, XLOG_SEQ_LOG};
use types_core::{
    Buffer, FirstCommandId, FrozenTransactionId, LocalTransactionId, Oid, RelFileNumber,
    BOOLOID, INT2OID, INT4OID, INT8OID, RELATION_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use types_nodes::parsenodes::DefElem;
use types_nodes::rawnodes::{ColumnDef, CreateSeqStmt, CreateStmt};
use types_nodes::{AlterSeqStmt, Node, NodeList, TypeName};
use types_rel::{
    AccessExclusiveLock, AccessShareLock, NoLock, Relation, RowExclusiveLock,
    ShareRowExclusiveLock, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_VIEW,
};
use types_core::{RELPERSISTENCE_PERMANENT, RELPERSISTENCE_UNLOGGED};
use types_storage::bufpage::{PageMut, PageRef};
use types_tuple::{
    HeapTupleHeaderData, ItemPointerSet, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID,
};
use adt_acl::{ACL_SELECT, ACL_UPDATE, ACL_USAGE};
use xloginsert_seams::{XLogRegBuf, REGBUF_WILL_INIT};

const SEQ_LOG_VALS: i64 = 32;
const SEQ_COLS: usize = 3;
const SequenceRelationId: Oid = 2224;
const SequenceRelidIndexId: Oid = 5002;
const Natts_pg_sequence: usize = 8;
const RM_SEQ_ID: u8 = 15;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: sequence {what}")
}

#[track_caller]
#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

#[derive(Clone, Copy)]
struct SeqTableData {
    filenumber: RelFileNumber,
    lxid: LocalTransactionId,
    last_valid: bool,
    last: i64,
    cached: i64,
    // zero until the first nextval_internal
    increment: i64,
}

const NEW_ELM: SeqTableData = SeqTableData {
    filenumber: 0,
    lxid: 0,
    last_valid: false,
    last: 0,
    cached: 0,
    increment: 0,
};

struct SeqState {
    tab: PgFxHashMap<'static, Oid, SeqTableData>,
    last_used: Option<Oid>,
}

thread_local! {
    static STATE: RefCell<Option<std::mem::ManuallyDrop<SeqState>>> = const { RefCell::new(None) };
}

// INVARIANT: `f` must not re-enter sequence state (no relation/catalog calls
// inside); every call site holds the borrow for a field read/write only.
fn with_state<R>(f: impl FnOnce(&mut SeqState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = mcx::session_root("SeqTable").mcx();
            std::mem::ManuallyDrop::new(SeqState {
                tab: PgFxHashMap::with_capacity_and_hasher_in(16, Default::default(), mcx),
                last_used: None,
            })
        });
        f(st)
    })
}

fn with_elm<R>(relid: Oid, f: impl FnOnce(&mut SeqTableData) -> R) -> R {
    with_state(|s| f(s.tab.get_mut(&relid).expect("SeqTable entry exists")))
}

/// C `ResetSequenceCaches` (sequence.c:1944-1954): C hash_destroys the table;
/// the port clears in place because the map's arena is the leaked backing
/// context (entries are inline PODs, so clear() forgets everything C frees).
pub fn ResetSequenceCaches() {
    STATE.with(|cell| {
        if let Some(s) = cell.borrow_mut().as_mut() {
            s.tab.clear();
            s.last_used = None;
        }
    });
}

// FormData_pg_sequence_data on-page layout: int8 @0, int8 @8, bool @16.
// data points into the buffer page: valid only while the read_seq_tuple
// buffer stays pinned + exclusively locked.
struct SeqTuple {
    data: *mut u8,
    t_len: u32,
    #[cfg(debug_assertions)]
    buf: Buffer,
}

impl SeqTuple {
    #[inline]
    fn assert_pinned(&self) {
        // InvalidBuffer marks a synthetic (non-page-backed) image.
        #[cfg(debug_assertions)]
        debug_assert!(self.buf == types_core::InvalidBuffer || bufmgr::BufferIsPinned(self.buf));
    }

    fn header(&self) -> *mut HeapTupleHeaderData {
        self.data.cast()
    }
    fn payload(&self) -> *mut u8 {
        // SAFETY: data points at the page item; t_hoff is within it.
        unsafe { self.data.add((*self.header()).t_hoff as usize) }
    }
    fn image(&self) -> &[u8] {
        self.assert_pinned();
        // SAFETY: item bytes are t_len long inside the locked page.
        unsafe { core::slice::from_raw_parts(self.data, self.t_len as usize) }
    }
    fn last_value(&self) -> i64 {
        // SAFETY: fixed 3-column no-null layout checked by read_seq_tuple.
        unsafe { self.payload().cast::<i64>().read_unaligned() }
    }
    fn log_cnt(&self) -> i64 {
        // SAFETY: as above.
        unsafe { self.payload().add(8).cast::<i64>().read_unaligned() }
    }
    fn is_called(&self) -> bool {
        // SAFETY: as above.
        unsafe { self.payload().add(16).read() != 0 }
    }
    fn set(&self, last_value: i64, log_cnt: i64, is_called: bool) {
        self.assert_pinned();
        // SAFETY: exclusive buffer lock held by the caller.
        unsafe {
            self.payload().cast::<i64>().write_unaligned(last_value);
            self.payload().add(8).cast::<i64>().write_unaligned(log_cnt);
            self.payload().add(16).write(is_called as u8);
        }
    }
}

pub fn init_seams() {
    builtins::register_builtins();
    sequence_seams::nextval_internal::set(nextval_internal_entry);
    sequence_seams::currval_internal::set(currval_internal);
    sequence_seams::lastval_internal::set(lastval_internal);
    sequence_seams::do_setval::set(do_setval_entry);
    sequence_seams::delete_sequence_tuple::set(delete_sequence_tuple_entry);
    sequence_seams::define_sequence::set(define_sequence_entry);
    sequence_seams::alter_sequence::set(AlterSequence);
    sequence_seams::reset_sequence::set(ResetSequence);
    sequence_seams::sequence_change_persistence::set(SequenceChangePersistence);
}

fn define_sequence_entry<'mcx>(mcx: Mcx<'mcx>, seq: &CreateSeqStmt<'mcx>) -> PgResult<Oid> {
    DefineSequence(mcx, None, seq)
}

fn my_lxid() -> LocalTransactionId {
    let procno = lmgr_proc::MyProc().expect("MyProc is not set");
    lmgr_proc::GetPGProcByNumber(procno)
        .vxid
        .lxid
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn sequence_open<'mcx>(mcx: Mcx<'mcx>, relid: Oid, lockmode: i32) -> PgResult<Relation<'mcx>> {
    let r = relation::relation_open(mcx, relid, lockmode)?;
    if r.rd_rel.relkind != RELKIND_SEQUENCE {
        let mut e = err(
            format!("cannot open relation \"{}\"", r.name()),
            ERRCODE_WRONG_OBJECT_TYPE,
        );
        e.detail = Some(pg_class_seams::errdetail_relkind_not_supported::call(
            r.rd_rel.relkind as u8,
        )?);
        return Err(e);
    }
    Ok(r)
}

// C holds the lock under TopTransactionResourceOwner; locks here are
// transaction-scoped already, so only the once-per-lxid memo is ported.
fn lock_and_open_sequence<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Relation<'mcx>> {
    let thislxid = my_lxid();
    if with_elm(relid, |e| e.lxid) != thislxid {
        lmgr::LockRelationOid(relid, RowExclusiveLock)?;
        with_elm(relid, |e| e.lxid = thislxid);
    }
    sequence_open(mcx, relid, NoLock)
}

fn init_sequence<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Relation<'mcx>> {
    with_state(|s| {
        s.tab.entry(relid).or_insert(NEW_ELM);
    });
    let seqrel = lock_and_open_sequence(mcx, relid)?;
    let filenumber = seqrel.rd_rel.relfilenode;
    with_elm(relid, |e| {
        if e.filenumber != filenumber {
            e.filenumber = filenumber;
            e.cached = e.last;
        }
    });
    Ok(seqrel)
}

fn read_seq_tuple(rel: &Relation<'_>) -> PgResult<(Buffer, SeqTuple)> {
    let buf = bufmgr::ReadBuffer(rel, 0)?;
    bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_EXCLUSIVE)?;

    let raw = bufmgr::BufferGetPagePtr(buf);
    // SAFETY: pinned + exclusively locked above.
    let page = unsafe { PageRef::from_raw(raw) };
    let magic = sequence_xlog::seq_page_magic(&page);
    if magic != SEQ_MAGIC {
        return Err(err(
            format!("bad magic number in sequence \"{}\": {magic:08X}", rel.name()),
            types_error::ERRCODE_INTERNAL_ERROR,
        ));
    }

    let lp = page.item_id(1);
    debug_assert!(lp.is_used());
    let (ptr, len) = page.item_raw(lp);
    let tup = SeqTuple {
        data: ptr.cast_mut(),
        t_len: len,
        #[cfg(debug_assertions)]
        buf,
    };

    // Clear any leftover xmax from historical SELECT FOR UPDATE, hint-style.
    // SAFETY: exclusive lock held; header is within the item.
    unsafe {
        let hdr = &mut *tup.header();
        if hdr.xmax_raw() != 0 {
            hdr.set_xmax(0);
            hdr.t_infomask &= !HEAP_XMAX_COMMITTED;
            hdr.t_infomask |= HEAP_XMAX_INVALID;
            bufmgr::MarkBufferDirtyHint(buf, true)?;
        }
    }
    Ok((buf, tup))
}

// RelationNeedsWAL (rel.h), including the wal_level=minimal skip-WAL clause.
fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

fn rd_locator_bytes(rel: &Relation<'_>) -> [u8; SizeOfXlSeqRec] {
    let loc = rel.rd_locator.get();
    let mut out = [0u8; SizeOfXlSeqRec];
    out[0..4].copy_from_slice(&loc.spcOid.to_ne_bytes());
    out[4..8].copy_from_slice(&loc.dbOid.to_ne_bytes());
    out[8..12].copy_from_slice(&loc.relNumber.to_ne_bytes());
    out
}

// persistence is passed explicitly: after RelationSetNewRelfilenumber the C
// relcache rebuild refreshes rel->rd_rel->relpersistence in place, but our
// open handle keeps the pre-CCI snapshot (SequenceChangePersistence must fill
// per the NEW persistence).
fn fill_seq_with_data(
    rel: &Relation<'_>,
    tuple: &mut heaptuple::HeapTuple<'_>,
    persistence: u8,
) -> PgResult<()> {
    fill_seq_fork_with_data(rel, tuple, persistence, types_core::ForkNumber::MAIN_FORKNUM)?;

    if persistence == RELPERSISTENCE_UNLOGGED {
        let key = types_storage::RelFileLocatorBackend {
            locator: rel.rd_locator.get(),
            backend: rel.rd_backend,
        };
        smgr::smgropen(key.locator, key.backend)?;
        smgr::smgrcreate(key, types_core::ForkNumber::INIT_FORKNUM, false)?;
        catalog_storage::log_smgrcreate(&key.locator, types_core::ForkNumber::INIT_FORKNUM)?;
        fill_seq_fork_with_data(rel, tuple, persistence, types_core::ForkNumber::INIT_FORKNUM)?;
        bufmgr::FlushRelationBuffers(key)?;
        smgr::smgrclose(key)?;
    }
    Ok(())
}

fn fill_seq_fork_with_data(
    rel: &Relation<'_>,
    tuple: &mut heaptuple::HeapTuple<'_>,
    persistence: u8,
    forknum: types_core::ForkNumber,
) -> PgResult<()> {
    let (buf, _) = bufmgr::ExtendBufferedRelBy(
        rel,
        forknum,
        None,
        bufmgr_seams::EB_LOCK_FIRST | bufmgr_seams::EB_SKIP_EXTENSION_LOCK,
        1,
    )?;
    debug_assert!(bufmgr::BufferGetBlockNumber(buf) == 0);

    let raw = bufmgr::BufferGetPagePtr(buf);
    // SAFETY: EB_LOCK_FIRST left the new buffer pinned + exclusively locked.
    let mut page = unsafe { PageMut::from_raw(raw) };
    sequence_xlog::seq_page_init(&mut page);

    // VACUUM never visits sequences: force frozen xmin now (an aborted
    // creating xact means no one ever reads this tuple anyway).
    {
        let hdr = tuple.as_tuple_mut().t_data_mut();
        hdr.set_xmin(FrozenTransactionId);
        hdr.set_xmin_frozen();
        hdr.set_cmin(FirstCommandId);
        hdr.set_xmax(0);
        hdr.t_infomask |= HEAP_XMAX_INVALID;
        ItemPointerSet(&mut hdr.t_ctid, 0, 1);
    }

    if persistence == RELPERSISTENCE_PERMANENT {
        xact::GetTopTransactionId()?;
    }

    init_small::globals::StartCriticalSection();
    bufmgr::MarkBufferDirty(buf)?;

    if page.add_item(tuple.image(), 0, 0) != Some(1) {
        init_small::globals::EndCriticalSection();
        return Err(err(
            "failed to add sequence tuple to page".into(),
            types_error::ERRCODE_INTERNAL_ERROR,
        ));
    }

    if persistence == RELPERSISTENCE_PERMANENT || forknum == types_core::ForkNumber::INIT_FORKNUM
    {
        let xlrec = rd_locator_bytes(rel);
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SEQ_ID,
            XLOG_SEQ_LOG,
            0,
            &[&xlrec, tuple.image()],
            &[XLogRegBuf { block_id: 0, buffer: buf, flags: REGBUF_WILL_INIT, bufdata: &[] }],
        )?;
        page.set_lsn(recptr);
    }

    init_small::globals::EndCriticalSection();
    bufmgr::UnlockReleaseBuffer(buf)
}

struct SeqFormLocal {
    seqtypid: Oid,
    seqstart: i64,
    seqincrement: i64,
    seqmax: i64,
    seqmin: i64,
    seqcache: i64,
    seqcycle: bool,
}

struct SeqDataFormLocal {
    last_value: i64,
    log_cnt: i64,
    is_called: bool,
}

fn def_get_i64(defel: &DefElem<'_>) -> PgResult<i64> {
    let bad = || {
        err(
            format!("{} requires a numeric value", defel.defname.unwrap_or("")),
            ERRCODE_SYNTAX_ERROR,
        )
    };
    match defel.arg {
        Some(n) => {
            if let Some(i) = n.as_integer() {
                Ok(i.ival as i64)
            } else if let Some(f) = n.as_float() {
                Ok(adt_int8::int8in(f.fval, None)?)
            } else {
                Err(bad())
            }
        }
        None => Err(bad()),
    }
}

#[track_caller]
#[cold]
fn conflicting_def_elem() -> Box<PgError> {
    err("conflicting or redundant options".into(), ERRCODE_SYNTAX_ERROR)
}

struct InitParamsOut<'mcx> {
    need_seq_rewrite: bool,
    owned_by: Option<&'mcx NodeList<'mcx>>,
}

fn init_params<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    options: &NodeList<'mcx>,
    for_identity: bool,
    is_init: bool,
    form: &mut SeqFormLocal,
    dataform: &mut SeqDataFormLocal,
) -> PgResult<InitParamsOut<'mcx>> {
    let mut as_type: Option<&DefElem<'_>> = None;
    let mut start_value: Option<&DefElem<'_>> = None;
    let mut restart_value: Option<&DefElem<'_>> = None;
    let mut increment_by: Option<&DefElem<'_>> = None;
    let mut max_value: Option<&DefElem<'_>> = None;
    let mut min_value: Option<&DefElem<'_>> = None;
    let mut cache_value: Option<&DefElem<'_>> = None;
    let mut is_cycled: Option<&DefElem<'_>> = None;
    let mut owned_by: Option<&'mcx NodeList<'mcx>> = None;
    let mut need_seq_rewrite = false;
    let mut reset_max_value = false;
    let mut reset_min_value = false;

    for opt in options.iter() {
        let defel = opt.as_variant::<DefElem>().expect("DefElem");
        let slot: &mut Option<&DefElem<'_>> = match defel.defname.expect("defname") {
            "as" => &mut as_type,
            "increment" => &mut increment_by,
            "start" => &mut start_value,
            "restart" => &mut restart_value,
            "maxvalue" => &mut max_value,
            "minvalue" => &mut min_value,
            "cache" => &mut cache_value,
            "cycle" => &mut is_cycled,
            "owned_by" => {
                if owned_by.is_some() {
                    return Err(conflicting_def_elem());
                }
                owned_by =
                    Some(defel.arg.expect("owned_by arg").as_list().expect("owned_by name list"));
                continue;
            }
            "sequence_name" => {
                return Err(err(
                    "invalid sequence option SEQUENCE NAME".into(),
                    ERRCODE_SYNTAX_ERROR,
                ))
            }
            other => {
                return Err(Box::new(PgError::new(
                    ERROR,
                    format!("option \"{other}\" not recognized"),
                )))
            }
        };
        if slot.is_some() {
            return Err(conflicting_def_elem());
        }
        *slot = Some(defel);
        need_seq_rewrite = true;
    }

    if is_init {
        dataform.log_cnt = 0;
    }

    if let Some(d) = as_type {
        let tn = d.arg.expect("AS arg").as_variant::<TypeName>().expect("AS TypeName");
        let (newtypid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, pstate, tn)?;
        if newtypid != INT2OID && newtypid != INT4OID && newtypid != INT8OID {
            return Err(err(
                if for_identity {
                    "identity column type must be smallint, integer, or bigint".into()
                } else {
                    "sequence type must be smallint, integer, or bigint".into()
                },
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if !is_init {
            // Old bounds at the old type's limits follow the type; explicit
            // user bounds stay.
            if (form.seqtypid == INT2OID && form.seqmax == i16::MAX as i64)
                || (form.seqtypid == INT4OID && form.seqmax == i32::MAX as i64)
                || (form.seqtypid == INT8OID && form.seqmax == i64::MAX)
            {
                reset_max_value = true;
            }
            if (form.seqtypid == INT2OID && form.seqmin == i16::MIN as i64)
                || (form.seqtypid == INT4OID && form.seqmin == i32::MIN as i64)
                || (form.seqtypid == INT8OID && form.seqmin == i64::MIN)
            {
                reset_min_value = true;
            }
        }
        form.seqtypid = newtypid;
    } else if is_init {
        form.seqtypid = INT8OID;
    }

    if let Some(d) = increment_by {
        form.seqincrement = def_get_i64(d)?;
        if form.seqincrement == 0 {
            return Err(err("INCREMENT must not be zero".into(), ERRCODE_INVALID_PARAMETER_VALUE));
        }
        dataform.log_cnt = 0;
    } else if is_init {
        form.seqincrement = 1;
    }

    if let Some(d) = is_cycled {
        form.seqcycle = d.arg.expect("cycle arg").as_boolean().expect("Boolean").boolval;
        dataform.log_cnt = 0;
    } else if is_init {
        form.seqcycle = false;
    }

    let type_max = match form.seqtypid {
        INT2OID => i16::MAX as i64,
        INT4OID => i32::MAX as i64,
        _ => i64::MAX,
    };
    let type_min = match form.seqtypid {
        INT2OID => i16::MIN as i64,
        INT4OID => i32::MIN as i64,
        _ => i64::MIN,
    };

    match max_value {
        Some(d) if d.arg.is_some() => {
            form.seqmax = def_get_i64(d)?;
            dataform.log_cnt = 0;
        }
        _ if is_init || max_value.is_some() || reset_max_value => {
            form.seqmax =
                if form.seqincrement > 0 || reset_max_value { type_max } else { -1 };
            dataform.log_cnt = 0;
        }
        _ => {}
    }
    if form.seqmax < type_min || form.seqmax > type_max {
        return Err(err(
            format!(
                "MAXVALUE ({}) is out of range for sequence data type {}",
                form.seqmax,
                type_name_of(form.seqtypid)
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }

    match min_value {
        Some(d) if d.arg.is_some() => {
            form.seqmin = def_get_i64(d)?;
            dataform.log_cnt = 0;
        }
        _ if is_init || min_value.is_some() || reset_min_value => {
            form.seqmin =
                if form.seqincrement < 0 || reset_min_value { type_min } else { 1 };
            dataform.log_cnt = 0;
        }
        _ => {}
    }
    if form.seqmin < type_min || form.seqmin > type_max {
        return Err(err(
            format!(
                "MINVALUE ({}) is out of range for sequence data type {}",
                form.seqmin,
                type_name_of(form.seqtypid)
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }

    if form.seqmin >= form.seqmax {
        return Err(err(
            format!("MINVALUE ({}) must be less than MAXVALUE ({})", form.seqmin, form.seqmax),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }

    if let Some(d) = start_value {
        form.seqstart = def_get_i64(d)?;
    } else if is_init {
        form.seqstart = if form.seqincrement > 0 { form.seqmin } else { form.seqmax };
    }
    if form.seqstart < form.seqmin {
        return Err(err(
            format!(
                "START value ({}) cannot be less than MINVALUE ({})",
                form.seqstart, form.seqmin
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }
    if form.seqstart > form.seqmax {
        return Err(err(
            format!(
                "START value ({}) cannot be greater than MAXVALUE ({})",
                form.seqstart, form.seqmax
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }

    if let Some(d) = restart_value {
        dataform.last_value =
            if d.arg.is_some() { def_get_i64(d)? } else { form.seqstart };
        dataform.is_called = false;
        dataform.log_cnt = 0;
    } else if is_init {
        dataform.last_value = form.seqstart;
        dataform.is_called = false;
    }
    if dataform.last_value < form.seqmin {
        return Err(err(
            format!(
                "RESTART value ({}) cannot be less than MINVALUE ({})",
                dataform.last_value, form.seqmin
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }
    if dataform.last_value > form.seqmax {
        return Err(err(
            format!(
                "RESTART value ({}) cannot be greater than MAXVALUE ({})",
                dataform.last_value, form.seqmax
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }

    if let Some(d) = cache_value {
        form.seqcache = def_get_i64(d)?;
        if form.seqcache <= 0 {
            return Err(err(
                format!("CACHE ({}) must be greater than zero", form.seqcache),
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        dataform.log_cnt = 0;
    } else if is_init {
        form.seqcache = 1;
    }

    Ok(InitParamsOut { need_seq_rewrite, owned_by })
}

fn type_name_of(typid: Oid) -> &'static str {
    match typid {
        INT2OID => "smallint",
        INT4OID => "integer",
        _ => "bigint",
    }
}

fn make_column_def<'mcx>(mcx: Mcx<'mcx>, colname: &'mcx str, typid: Oid) -> PgResult<Node<'mcx>> {
    let mut tn = Node::build::<TypeName>(mcx)?;
    tn.typeOid = typid;
    tn.typemod = -1;
    tn.location = -1;
    let tn = tn.seal();

    let mut cd = Node::build::<ColumnDef>(mcx)?;
    cd.colname = Some(colname);
    cd.typeName = Some(tn);
    cd.is_local = true;
    cd.is_not_null = true;
    cd.location = -1;
    Ok(cd.seal())
}

pub fn DefineSequence<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    seq: &CreateSeqStmt<'mcx>,
) -> PgResult<Oid> {
    let rv = seq.sequence.expect("CreateSeqStmt.sequence");
    if seq.if_not_exists {
        let rvv = rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname: rv.relname.expect("RangeVar.relname"),
            inh: rv.inh,
            relpersistence: rv.relpersistence,
            location: rv.location,
        };
        // RangeVarGetAndCheckCreationNamespace(seq->sequence, NoLock, &seqoid)
        // (sequence.c:146): resolve + ACL_CREATE + namespace lock, and the
        // existing-relation probe the IF NOT EXISTS test below reads.
        let (_nspid, seqoid, _relpersistence) =
            catalog_namespace::RangeVarGetAndCheckCreationNamespace(
                mcx,
                &rvv,
                types_rel::NoLock,
                true,
            )?;
        if seqoid != types_core::InvalidOid {
            // checkMembershipInCurrentExtension only bites inside an
            // extension script (parse_utilcmd CREATE TABLE INE precedent).
            if pg_depend::creating_extension() {
                unported("CREATE SEQUENCE IF NOT EXISTS inside an extension script");
            }
            elog::ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_TABLE)
                .errmsg(format!("relation \"{}\" already exists, skipping", rvv.relname))
                .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "DefineSequence"))?;
            return Ok(types_core::InvalidOid);
        }
    }

    let mut form = SeqFormLocal {
        seqtypid: INT8OID,
        seqstart: 0,
        seqincrement: 1,
        seqmax: 0,
        seqmin: 0,
        seqcache: 1,
        seqcycle: false,
    };
    let mut dataform = SeqDataFormLocal { last_value: 0, log_cnt: 0, is_called: false };
    let p = init_params(mcx, pstate, &seq.options, seq.for_identity, true, &mut form, &mut dataform)?;

    let mut elts = NodeList::make1(mcx, make_column_def(mcx, "last_value", INT8OID)?)?;
    elts.lappend(mcx, make_column_def(mcx, "log_cnt", INT8OID)?)?;
    elts.lappend(mcx, make_column_def(mcx, "is_called", BOOLOID)?)?;

    let mut stmt = Node::build::<CreateStmt>(mcx)?;
    stmt.relation = Some(rv);
    stmt.tableElts = elts;
    stmt.if_not_exists = seq.if_not_exists;
    let seqoid = tablecmds::DefineRelation(mcx, &stmt, RELKIND_SEQUENCE, seq.ownerId, "")?;

    let seqrel = sequence_open(mcx, seqoid, AccessExclusiveLock)?;
    let values = [
        Datum::from_i64(dataform.last_value),
        Datum::from_i64(dataform.log_cnt),
        Datum::from_bool(dataform.is_called),
    ];
    let nulls = [false; SEQ_COLS];
    let mut tuple = heaptuple::heap_form_tuple(mcx, seqrel.descr(), &values, &nulls)?;
    fill_seq_with_data(&seqrel, &mut tuple, seqrel.rd_rel.relpersistence)?;

    if let Some(owned_by) = p.owned_by {
        process_owned_by(mcx, &seqrel, owned_by, seq.for_identity)?;
    }
    seqrel.close(NoLock)?;

    let rel = table::table_open(mcx, SequenceRelationId, RowExclusiveLock)?;
    let pgs_values = [
        Datum::from_oid(seqoid),
        Datum::from_oid(form.seqtypid),
        Datum::from_i64(form.seqstart),
        Datum::from_i64(form.seqincrement),
        Datum::from_i64(form.seqmax),
        Datum::from_i64(form.seqmin),
        Datum::from_i64(form.seqcache),
        Datum::from_bool(form.seqcycle),
    ];
    let pgs_nulls = [false; Natts_pg_sequence];
    let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &pgs_values, &pgs_nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;
    rel.close(RowExclusiveLock)?;

    Ok(seqoid)
}

// RangeVarCallbackOwnsRelation (tablecmds.c:19554-19579).
fn range_var_callback_owns_relation(rv: &rel_vocab::RangeVar<'_>, relid: Oid) -> PgResult<()> {
    if relid == types_core::InvalidOid {
        return Ok(());
    }
    if !aclchk::object_ownercheck(RELATION_RELATION_ID, relid, miscinit::GetUserId())? {
        let relkind = lsyscache::get_rel_relkind(relid)? as u8;
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            get_relkind_objtype(relkind),
            rv.relname,
        )?;
    }
    let relnamespace = lsyscache::get_rel_namespace(relid)?;
    let is_system =
        catalog::IsCatalogRelationOid(relid) || catalog::IsToastNamespace(relnamespace);
    if is_system && !init_small::globals::allowSystemTableMods() {
        return Err(err(
            format!("permission denied: \"{}\" is a system catalog", rv.relname),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }
    Ok(())
}

// get_relkind_objtype (objectaddress.c).
fn get_relkind_objtype(relkind: u8) -> types_nodes::parsenodes::ObjectType {
    use types_nodes::parsenodes::ObjectType::*;
    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => OBJECT_TABLE,
        types_rel::RELKIND_INDEX | types_rel::RELKIND_PARTITIONED_INDEX => OBJECT_INDEX,
        RELKIND_SEQUENCE => OBJECT_SEQUENCE,
        RELKIND_VIEW => OBJECT_VIEW,
        types_rel::RELKIND_MATVIEW => OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => OBJECT_FOREIGN_TABLE,
        _ => OBJECT_TABLE,
    }
}

pub fn AlterSequence<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterSeqStmt<'mcx>) -> PgResult<Oid> {
    let rv = stmt.sequence.expect("AlterSeqStmt.sequence");
    let v = rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname: rv.relname.expect("RangeVar.relname"),
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    };
    let relid =
        namespace_seams::range_var_get_relid::call(mcx, &v, ShareRowExclusiveLock, stmt.missing_ok)?;
    if relid == types_core::InvalidOid {
        ::elog::ereport(::types_error::NOTICE)
            .errmsg(format!("relation \"{}\" does not exist, skipping", v.relname))
            .finish(::types_error::ErrorLocation::new(file!(), line!() as i32, "AlterSequence"))?;
        return Ok(types_core::InvalidOid);
    }
    // C: RangeVarCallbackOwnsRelation inside RangeVarGetRelidExtended
    // (sequence.c:454-458, tablecmds.c:19554-19579); the lookup seam has no
    // callback hook, so it runs post-lookup under the already-taken lock.
    range_var_callback_owns_relation(&v, relid)?;

    let seqrel = init_sequence(mcx, relid)?;

    let rel = table::table_open(mcx, SequenceRelationId, RowExclusiveLock)?;
    let keys = [seqrelid_key(relid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, SequenceRelidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for sequence {relid}"));
    let otid = tup.t_self;
    let desc = rel.descr();
    let mut isnull = false;
    let mut get = |anum: i32| {
        // SAFETY: fixed NOT NULL pg_sequence columns under its descriptor.
        unsafe { types_tuple::heap_getattr(tup, anum, desc, &mut isnull) }
    };
    let mut form = SeqFormLocal {
        seqtypid: get(2).as_oid(),
        seqstart: get(3).as_i64(),
        seqincrement: get(4).as_i64(),
        seqmax: get(5).as_i64(),
        seqmin: get(6).as_i64(),
        seqcache: get(7).as_i64(),
        seqcycle: get(8).as_bool(),
    };
    genam::systable_endscan(mcx, scan)?;

    let (buf, seq) = read_seq_tuple(&seqrel)?;
    let mut dataform = SeqDataFormLocal {
        last_value: seq.last_value(),
        log_cnt: seq.log_cnt(),
        is_called: seq.is_called(),
    };
    bufmgr::UnlockReleaseBuffer(buf)?;

    let p = init_params(mcx, None, &stmt.options, stmt.for_identity, false, &mut form, &mut dataform)?;

    if p.need_seq_rewrite {
        if relation_needs_wal(&seqrel) {
            xact::GetTopTransactionId()?;
        }
        catalog_index::RelationSetNewRelfilenumber(mcx, &seqrel, seqrel.rd_rel.relpersistence)?;
        let values = [
            Datum::from_i64(dataform.last_value),
            Datum::from_i64(dataform.log_cnt),
            Datum::from_bool(dataform.is_called),
        ];
        let nulls = [false; SEQ_COLS];
        let mut tuple = heaptuple::heap_form_tuple(mcx, seqrel.descr(), &values, &nulls)?;
        fill_seq_with_data(&seqrel, &mut tuple, seqrel.rd_rel.relpersistence)?;
    }

    with_elm(relid, |e| e.cached = e.last);

    if let Some(owned_by) = p.owned_by {
        process_owned_by(mcx, &seqrel, owned_by, stmt.for_identity)?;
    }

    let pgs_values = [
        Datum::from_oid(relid),
        Datum::from_oid(form.seqtypid),
        Datum::from_i64(form.seqstart),
        Datum::from_i64(form.seqincrement),
        Datum::from_i64(form.seqmax),
        Datum::from_i64(form.seqmin),
        Datum::from_i64(form.seqcache),
        Datum::from_bool(form.seqcycle),
    ];
    let pgs_nulls = [false; Natts_pg_sequence];
    let mut newtup = heaptuple::heap_form_tuple(mcx, rel.descr(), &pgs_values, &pgs_nulls)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
    rel.close(RowExclusiveLock)?;
    seqrel.close(NoLock)?;
    Ok(relid)
}

// Caller holds AccessExclusiveLock on the sequence until end of transaction
// and has done the permissions check.
pub fn ResetSequence(mcx: Mcx<'_>, seq_relid: Oid) -> PgResult<()> {
    let seqrel = init_sequence(mcx, seq_relid)?;
    let (buf, seq) = read_seq_tuple(&seqrel)?;

    let startv = pgs_form(seq_relid)?.seqstart;

    // SAFETY: views the page item for the copy only, under the buffer lock.
    let view = unsafe {
        types_tuple::HeapTupleData::from_raw_parts(
            seq.data,
            seq.t_len,
            types_tuple::ItemPointerData::invalid(),
            types_core::InvalidOid,
        )
    };
    let mut tuple = heaptuple::heap_copytuple(mcx, &view)?;
    bufmgr::UnlockReleaseBuffer(buf)?;

    let copy = SeqTuple {
        data: tuple.image_mut().as_mut_ptr(),
        t_len: tuple.t_len,
        #[cfg(debug_assertions)]
        buf: types_core::InvalidBuffer,
    };
    copy.set(startv, 0, false);

    catalog_index::RelationSetNewRelfilenumber(mcx, &seqrel, seqrel.rd_rel.relpersistence)?;
    fill_seq_with_data(&seqrel, &mut tuple, seqrel.rd_rel.relpersistence)?;

    // Local cache cleared; currval() state intentionally kept.
    with_elm(seq_relid, |e| e.cached = e.last);

    seqrel.close(NoLock)
}

pub fn SequenceChangePersistence(mcx: Mcx<'_>, relid: Oid, newrelpersistence: u8) -> PgResult<()> {
    // ALTER SEQUENCE acquires this lock earlier; the ALTER TABLE owned-
    // sequence path locks here so increments from concurrent nextval()
    // between buffer unlock and commit aren't discarded.
    lmgr::LockRelationOid(relid, AccessExclusiveLock)?;
    let seqrel = init_sequence(mcx, relid)?;

    if relation_needs_wal(&seqrel) {
        xact::GetTopTransactionId()?;
    }

    let (buf, seq) = read_seq_tuple(&seqrel)?;
    // C reuses the page item in place; fill_seq_with_data here freezes the
    // header of an owned copy instead, same bytes inserted.
    let view = unsafe {
        // SAFETY: views the page item for the copy only, under the buffer lock.
        types_tuple::HeapTupleData::from_raw_parts(
            seq.data,
            seq.t_len,
            types_tuple::ItemPointerData::invalid(),
            types_core::InvalidOid,
        )
    };
    let mut tuple = heaptuple::heap_copytuple(mcx, &view)?;
    catalog_index::RelationSetNewRelfilenumber(mcx, &seqrel, newrelpersistence)?;
    fill_seq_with_data(&seqrel, &mut tuple, newrelpersistence)?;
    bufmgr::UnlockReleaseBuffer(buf)?;

    seqrel.close(NoLock)
}

fn process_owned_by<'mcx>(
    mcx: Mcx<'mcx>,
    seqrel: &Relation<'mcx>,
    owned_by: &NodeList<'mcx>,
    for_identity: bool,
) -> PgResult<()> {
    let deptype = if for_identity {
        pg_depend::DependencyType::Internal
    } else {
        pg_depend::DependencyType::Auto
    };

    let nnames = owned_by.len();
    debug_assert!(nnames > 0);

    let mut tablerel: Option<Relation<'mcx>> = None;
    let mut attnum: i32 = 0;

    if nnames == 1 {
        let name = owned_by.nth(0).as_string().expect("OWNED BY name").sval;
        if name != "none" {
            return Err((*err("invalid OWNED BY option".into(), ERRCODE_SYNTAX_ERROR))
                .with_hint("Specify OWNED BY table.column or OWNED BY NONE.")
                .into());
        }
    } else {
        if nnames > 3 {
            // unported: OWNED BY with a catalog-qualified name
            // (makeRangeVarFromNameList catalog arm)
            return Err(err(
                "OWNED BY with a catalog-qualified table name is not supported yet".to_string(),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        let mut parts = [""; 3];
        for (i, n) in owned_by.iter().enumerate() {
            parts[i] = n.as_string().expect("OWNED BY name").sval;
        }
        let attrname = parts[nnames - 1];
        let (schemaname, relname) =
            if nnames == 3 { (Some(parts[0]), parts[1]) } else { (None, parts[0]) };

        let rv = rel_vocab::RangeVar {
            catalogname: None,
            schemaname,
            relname,
            inh: true,
            relpersistence: RELPERSISTENCE_PERMANENT,
            location: -1,
        };
        let trel = relation::relation_openrv(mcx, &rv, AccessShareLock)?;

        if !matches!(
            trel.rd_rel.relkind,
            RELKIND_RELATION | RELKIND_FOREIGN_TABLE | RELKIND_VIEW | RELKIND_PARTITIONED_TABLE
        ) {
            let mut e = err(
                format!("sequence cannot be owned by relation \"{}\"", trel.name()),
                ERRCODE_WRONG_OBJECT_TYPE,
            );
            e.detail = Some(pg_class_seams::errdetail_relkind_not_supported::call(
                trel.rd_rel.relkind as u8,
            )?);
            return Err(e);
        }

        if seqrel.rd_rel.relowner != trel.rd_rel.relowner {
            return Err(err(
                "sequence must have same owner as table it is linked to".into(),
                ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            ));
        }
        if seqrel.rd_rel.relnamespace != trel.rd_rel.relnamespace {
            return Err(err(
                "sequence must be in same schema as table it is linked to".into(),
                ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            ));
        }

        let att = lsyscache::get_attnum(trel.rd_id, attrname)?;
        if att == 0 {
            return Err(err(
                format!("column \"{attrname}\" of relation \"{}\" does not exist", trel.name()),
                ERRCODE_UNDEFINED_COLUMN,
            ));
        }
        attnum = att as i32;
        tablerel = Some(trel);
    }

    if deptype == pg_depend::DependencyType::Auto {
        if let Some((table_id, _)) =
            pg_depend::sequenceIsOwned(mcx, seqrel.rd_id, pg_depend::DependencyType::Internal)?
        {
            let tname = lsyscache::get_rel_name(mcx, table_id)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err((*err(
                "cannot change ownership of identity sequence".into(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ))
            .with_detail(format!(
                "Sequence \"{}\" is linked to table \"{tname}\".",
                seqrel.name()
            ))
            .into());
        }
    }

    pg_depend::deleteDependencyRecordsForClass(
        mcx,
        RELATION_RELATION_ID,
        seqrel.rd_id,
        RELATION_RELATION_ID,
        deptype,
    )?;

    if let Some(trel) = tablerel {
        let refobject =
            pg_depend::ObjectAddress::sub_set(RELATION_RELATION_ID, trel.rd_id, attnum);
        let depobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, seqrel.rd_id);
        pg_depend::recordDependencyOn(mcx, &depobject, &refobject, deptype)?;
        trel.close(NoLock)?;
    }
    Ok(())
}

fn pgs_form(relid: Oid) -> PgResult<syscache_seams::PgSequenceForm> {
    match syscache_seams::lookup_pg_sequence_form::call(relid)? {
        Some(f) => Ok(f),
        None => Err(Box::new(PgError::new(
            ERROR,
            format!("cache lookup failed for sequence {relid}"),
        ))),
    }
}

// One retained backend context for the fmgr entry points (rule 4: no per-call
// context); nothing on these paths allocates into it.
fn fc_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: &'static mcx::MemoryContext =
            mcx::session_root("SequenceFmgr");
    }
    CTX.with(|c| c.mcx())
}

fn nextval_internal_entry(relid: Oid, check_permissions: bool) -> PgResult<i64> {
    nextval_internal(fc_mcx(), relid, check_permissions)
}

/// Output of the pure nextval fetch-loop computation (proofs/state-seam-probe).
pub struct NextvalAdvance {
    /// value to return (elm.last)
    pub result: i64,
    /// last fetched value (elm.cached; written back as last_value)
    pub last: i64,
    /// value as of "log" future fetches (written into the WAL image)
    pub next: i64,
    /// new log_cnt
    pub log: i64,
    /// whether a WAL record must be emitted
    pub logit: bool,
}

/// Bound a non-cycling sequence exhausted (proofs/state-seam-probe).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NextvalBound {
    Max,
    Min,
}

/// Pure arithmetic core of nextval_internal (C: sequence.c nextval_internal's
/// pre-log decision + fetch loop). Behavior-identical factoring of the code
/// previously inline in nextval_internal; `lsn_le_redo` is consulted exactly
/// where the original read GetRedoRecPtr/page LSN. Factored out so the proofs
/// suite can prove it against the verbatim C (proofs/state-seam-probe).
#[allow(clippy::too_many_arguments)]
pub fn nextval_advance(
    last_value: i64,
    log_cnt: i64,
    is_called: bool,
    incby: i64,
    maxv: i64,
    minv: i64,
    cache: i64,
    cycle: bool,
    lsn_le_redo: impl FnOnce() -> bool,
) -> Result<NextvalAdvance, NextvalBound> {
    let mut last = last_value;
    let mut next = last;
    let mut result = last;
    let mut fetch = cache;
    let mut log = log_cnt;
    let mut rescnt: i64 = 0;
    let mut logit = false;

    if !is_called {
        rescnt += 1;
        fetch -= 1;
    }

    // Pre-log SEQ_LOG_VALS extra fetches; also force a record for the first
    // update after a checkpoint or replay would fail to advance the sequence.
    if log < fetch || !is_called {
        fetch += SEQ_LOG_VALS;
        log = fetch;
        logit = true;
    } else if lsn_le_redo() {
        fetch += SEQ_LOG_VALS;
        log = fetch;
        logit = true;
    }

    while fetch > 0 {
        if incby > 0 {
            if (maxv >= 0 && next > maxv - incby) || (maxv < 0 && next + incby > maxv) {
                if rescnt > 0 {
                    break;
                }
                if !cycle {
                    return Err(NextvalBound::Max);
                }
                next = minv;
            } else {
                next += incby;
            }
        } else {
            if (minv < 0 && next < minv - incby) || (minv >= 0 && next + incby < minv) {
                if rescnt > 0 {
                    break;
                }
                if !cycle {
                    return Err(NextvalBound::Min);
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
                result = next;
            }
        }
    }

    log -= fetch;
    debug_assert!(log >= 0);

    Ok(NextvalAdvance { result, last, next, log, logit })
}

pub fn nextval_internal(mcx: Mcx<'_>, relid: Oid, check_permissions: bool) -> PgResult<i64> {
    let seqrel = init_sequence(mcx, relid)?;

    if check_permissions
        && aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_USAGE | ACL_UPDATE)?
            != aclchk::ACLCHECK_OK
    {
        return Err(err(
            format!("permission denied for sequence {}", seqrel.name()),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    if !seqrel.rd_islocaltemp {
        xact::PreventCommandIfReadOnly("nextval()")?;
    }
    xact::PreventCommandIfParallelMode("nextval()")?;

    let (last0, cached0) = with_elm(relid, |e| (e.last, e.cached));
    if last0 != cached0 {
        let v = with_elm(relid, |e| {
            debug_assert!(e.last_valid && e.increment != 0);
            e.last += e.increment;
            e.last
        });
        seqrel.close(NoLock)?;
        with_state(|s| s.last_used = Some(relid));
        return Ok(v);
    }

    let form = pgs_form(relid)?;
    let (incby, maxv, minv, cache, cycle) =
        (form.seqincrement, form.seqmax, form.seqmin, form.seqcache, form.seqcycle);

    let (buf, seq) = read_seq_tuple(&seqrel)?;
    let raw = bufmgr::BufferGetPagePtr(buf);
    // SAFETY: read_seq_tuple leaves the buffer pinned + exclusively locked.
    let mut page = unsafe { PageMut::from_raw(raw) };

    let adv = match nextval_advance(
        seq.last_value(),
        seq.log_cnt(),
        seq.is_called(),
        incby,
        maxv,
        minv,
        cache,
        cycle,
        || page.as_ref().lsn() <= transam_xlog_seams::get_redo_rec_ptr::call(),
    ) {
        Ok(a) => a,
        Err(bound) => {
            bufmgr::UnlockReleaseBuffer(buf)?;
            let (word, bnd) = match bound {
                NextvalBound::Max => ("maximum", maxv),
                NextvalBound::Min => ("minimum", minv),
            };
            return Err(err(
                format!(
                    "nextval: reached {word} value of sequence \"{}\" ({bnd})",
                    seqrel.name()
                ),
                ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED,
            ));
        }
    };
    let (result, last, next, log, logit) = (adv.result, adv.last, adv.next, adv.log, adv.logit);

    with_elm(relid, |e| {
        e.increment = incby;
        e.last = result;
        e.cached = last;
        e.last_valid = true;
    });
    with_state(|s| s.last_used = Some(relid));

    // Assign the top xid outside the critical section so commit flushes WAL.
    if logit && relation_needs_wal(&seqrel) {
        xact::GetTopTransactionId()?;
    }

    init_small::globals::StartCriticalSection();

    // Dirty before XLogInsert (SyncOneBuffer protocol); the intermediate
    // pre-log state below is invisible under the exclusive content lock.
    bufmgr::MarkBufferDirty(buf)?;

    if logit && relation_needs_wal(&seqrel) {
        // Log the state as of "log" future fetches, not the current one.
        seq.set(next, 0, true);
        let xlrec = rd_locator_bytes(&seqrel);
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SEQ_ID,
            XLOG_SEQ_LOG,
            0,
            &[&xlrec, seq.image()],
            &[XLogRegBuf { block_id: 0, buffer: buf, flags: REGBUF_WILL_INIT, bufdata: &[] }],
        )?;
        page.set_lsn(recptr);
    }

    seq.set(last, log, true);

    init_small::globals::EndCriticalSection();
    bufmgr::UnlockReleaseBuffer(buf)?;
    seqrel.close(NoLock)?;
    Ok(result)
}

fn currval_internal(relid: Oid) -> PgResult<i64> {
    let mcx = fc_mcx();
    let seqrel = init_sequence(mcx, relid)?;

    if aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_SELECT | ACL_USAGE)?
        != aclchk::ACLCHECK_OK
    {
        return Err(err(
            format!("permission denied for sequence {}", seqrel.name()),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let (last_valid, last) = with_elm(relid, |e| (e.last_valid, e.last));
    if !last_valid {
        return Err(err(
            format!(
                "currval of sequence \"{}\" is not yet defined in this session",
                seqrel.name()
            ),
            ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        ));
    }
    seqrel.close(NoLock)?;
    Ok(last)
}

fn lastval_internal() -> PgResult<i64> {
    let mcx = fc_mcx();

    let Some(relid) = with_state(|s| s.last_used) else {
        return Err(err(
            "lastval is not yet defined in this session".into(),
            ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        ));
    };
    // The sequence may have been dropped since the last nextval().
    if !syscache_seams::search_syscache_exists_reloid::call(relid)? {
        return Err(err(
            "lastval is not yet defined in this session".into(),
            ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        ));
    }

    let seqrel = lock_and_open_sequence(mcx, relid)?;
    debug_assert!(with_elm(relid, |e| e.last_valid));

    if aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_SELECT | ACL_USAGE)?
        != aclchk::ACLCHECK_OK
    {
        return Err(err(
            format!("permission denied for sequence {}", seqrel.name()),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let last = with_elm(relid, |e| e.last);
    seqrel.close(NoLock)?;
    Ok(last)
}

fn do_setval_entry(relid: Oid, next: i64, iscalled: bool) -> PgResult<()> {
    do_setval(fc_mcx(), relid, next, iscalled)
}

pub fn do_setval(mcx: Mcx<'_>, relid: Oid, next: i64, iscalled: bool) -> PgResult<()> {
    let seqrel = init_sequence(mcx, relid)?;

    if aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_UPDATE)?
        != aclchk::ACLCHECK_OK
    {
        return Err(err(
            format!("permission denied for sequence {}", seqrel.name()),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let form = pgs_form(relid)?;
    let (maxv, minv) = (form.seqmax, form.seqmin);

    if !seqrel.rd_islocaltemp {
        xact::PreventCommandIfReadOnly("setval()")?;
    }
    xact::PreventCommandIfParallelMode("setval()")?;

    let (buf, seq) = read_seq_tuple(&seqrel)?;
    let raw = bufmgr::BufferGetPagePtr(buf);
    // SAFETY: read_seq_tuple leaves the buffer pinned + exclusively locked.
    let mut page = unsafe { PageMut::from_raw(raw) };

    if next < minv || next > maxv {
        bufmgr::UnlockReleaseBuffer(buf)?;
        return Err(err(
            format!(
                "setval: value {next} is out of bounds for sequence \"{}\" ({minv}..{maxv})",
                seqrel.name()
            ),
            ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        ));
    }

    with_elm(relid, |e| {
        if iscalled {
            e.last = next;
            e.last_valid = true;
        }
        e.cached = e.last;
    });

    if relation_needs_wal(&seqrel) {
        xact::GetTopTransactionId()?;
    }

    init_small::globals::StartCriticalSection();

    seq.set(next, 0, iscalled);
    bufmgr::MarkBufferDirty(buf)?;

    if relation_needs_wal(&seqrel) {
        let xlrec = rd_locator_bytes(&seqrel);
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SEQ_ID,
            XLOG_SEQ_LOG,
            0,
            &[&xlrec, seq.image()],
            &[XLogRegBuf { block_id: 0, buffer: buf, flags: REGBUF_WILL_INIT, bufdata: &[] }],
        )?;
        page.set_lsn(recptr);
    }

    init_small::globals::EndCriticalSection();
    bufmgr::UnlockReleaseBuffer(buf)?;
    seqrel.close(NoLock)
}

fn delete_sequence_tuple_entry(relid: Oid) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new("DeleteSequenceTuple");
    DeleteSequenceTuple(ctx.mcx(), relid)
}

pub fn DeleteSequenceTuple(mcx: Mcx<'_>, relid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, SequenceRelationId, RowExclusiveLock)?;
    let keys = [seqrelid_key(relid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, SequenceRelidIndexId, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::new(
            ERROR,
            format!("cache lookup failed for sequence {relid}"),
        )));
    };
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

fn seqrelid_key(relid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(oideq) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(relid);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> &'static mcx::MemoryContext {
        mcx::session_root("seq-test")
    }

    fn defel<'mcx>(mcx: Mcx<'mcx>, name: &'mcx str, arg: Option<Node<'mcx>>) -> Node<'mcx> {
        Node::mk(
            mcx,
            DefElem {
                defnamespace: None,
                defname: Some(name),
                arg,
                defaction: types_nodes::parsenodes::DefElemAction::DEFELEM_UNSPEC,
                location: -1,
            },
        )
        .unwrap()
    }

    fn int_arg(mcx: Mcx<'_>, v: i64) -> Option<Node<'_>> {
        if let Ok(i) = i32::try_from(v) {
            Some(Node::mk(mcx, types_nodes::Integer { ival: i }).unwrap())
        } else {
            let s = mcx::PgString::from_str_in(&v.to_string(), mcx).unwrap();
            let s = unsafe { core::str::from_utf8_unchecked(s.into_bytes().leak()) };
            Some(Node::mk(mcx, types_nodes::Float { fval: s }).unwrap())
        }
    }

    fn run_init<'mcx>(
        mcx: Mcx<'mcx>,
        opts: &NodeList<'mcx>,
    ) -> PgResult<(SeqFormLocal, SeqDataFormLocal)> {
        let mut form = SeqFormLocal {
            seqtypid: INT8OID,
            seqstart: 0,
            seqincrement: 1,
            seqmax: 0,
            seqmin: 0,
            seqcache: 1,
            seqcycle: false,
        };
        let mut dataform = SeqDataFormLocal { last_value: 0, log_cnt: 0, is_called: false };
        init_params(mcx, None, opts, false, true, &mut form, &mut dataform)?;
        Ok((form, dataform))
    }

    #[test]
    fn init_params_defaults_match_c() {
        let mcx = ctx().mcx();
        let (form, dataform) = run_init(mcx, &NodeList::nil()).unwrap();
        assert_eq!(form.seqtypid, INT8OID);
        assert_eq!(form.seqincrement, 1);
        assert_eq!(form.seqmin, 1);
        assert_eq!(form.seqmax, i64::MAX);
        assert_eq!(form.seqstart, 1);
        assert_eq!(form.seqcache, 1);
        assert!(!form.seqcycle);
        assert_eq!(dataform.last_value, 1);
        assert!(!dataform.is_called);
        assert_eq!(dataform.log_cnt, 0);
    }

    #[test]
    fn init_params_descending_defaults() {
        let mcx = ctx().mcx();
        let opts = NodeList::make1(mcx, defel(mcx, "increment", int_arg(mcx, -3))).unwrap();
        let (form, _) = run_init(mcx, &opts).unwrap();
        assert_eq!(form.seqmax, -1);
        assert_eq!(form.seqmin, i64::MIN);
        assert_eq!(form.seqstart, -1);
    }

    #[test]
    fn init_params_error_arms() {
        let mcx = ctx().mcx();
        for (name, v, frag) in [
            ("increment", 0, "INCREMENT must not be zero"),
            ("cache", 0, "CACHE (0) must be greater than zero"),
            ("start", 0, "START value (0) cannot be less than MINVALUE (1)"),
        ] {
            let opts = NodeList::make1(mcx, defel(mcx, name, int_arg(mcx, v))).unwrap();
            let e = run_init(mcx, &opts).err().expect("error expected");
            assert!(e.message().contains(frag), "{name}: {}", e.message());
            assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        }
        let mut opts =
            NodeList::make1(mcx, defel(mcx, "increment", int_arg(mcx, 1))).unwrap();
        opts.lappend(mcx, defel(mcx, "increment", int_arg(mcx, 2))).unwrap();
        let e = run_init(mcx, &opts).err().expect("error expected");
        assert!(e.message().contains("conflicting or redundant options"));
    }

    #[test]
    fn init_params_defget_int64_via_float_node() {
        let mcx = ctx().mcx();
        let opts =
            NodeList::make1(mcx, defel(mcx, "maxvalue", int_arg(mcx, 9223372036854775806)))
                .unwrap();
        let (form, _) = run_init(mcx, &opts).unwrap();
        assert_eq!(form.seqmax, 9223372036854775806);
    }

    #[test]
    fn init_params_alter_as_retype_follows_type_bounds() {
        syscache_seams::pg_type_isdefined::set(|_| Ok(Some(true)));
        syscache_seams::pg_type_typtype::set(|_| Ok(Some(b'b' as i8)));
        let mcx = ctx().mcx();
        let mut tn = Node::build::<TypeName>(mcx).unwrap();
        tn.typeOid = INT2OID;
        tn.typemod = -1;
        tn.location = -1;
        let opts = NodeList::make1(mcx, defel(mcx, "as", Some(tn.seal()))).unwrap();
        let mut form = SeqFormLocal {
            seqtypid: INT4OID,
            seqstart: 1,
            seqincrement: 1,
            seqmax: i32::MAX as i64,
            seqmin: 1,
            seqcache: 1,
            seqcycle: false,
        };
        let mut dataform = SeqDataFormLocal { last_value: 7, log_cnt: 5, is_called: true };
        let p = init_params(mcx, None, &opts, true, false, &mut form, &mut dataform).unwrap();
        assert!(p.need_seq_rewrite);
        assert_eq!(form.seqtypid, INT2OID);
        assert_eq!(form.seqmax, i16::MAX as i64);
        assert_eq!(form.seqmin, 1);
        assert_eq!(dataform.last_value, 7);
        assert!(dataform.is_called);
        assert_eq!(dataform.log_cnt, 0);

        // Explicit old bound is kept, and an out-of-range current value errors.
        let mut tn2 = Node::build::<TypeName>(mcx).unwrap();
        tn2.typeOid = INT2OID;
        tn2.typemod = -1;
        tn2.location = -1;
        let opts2 = NodeList::make1(mcx, defel(mcx, "as", Some(tn2.seal()))).unwrap();
        let mut form2 = SeqFormLocal {
            seqtypid: INT4OID,
            seqstart: 1,
            seqincrement: 1,
            seqmax: 100000,
            seqmin: 1,
            seqcache: 1,
            seqcycle: false,
        };
        let mut dataform2 = SeqDataFormLocal { last_value: 1, log_cnt: 0, is_called: false };
        let e = init_params(mcx, None, &opts2, true, false, &mut form2, &mut dataform2)
            .err()
            .expect("error expected");
        assert!(e.message().contains("MAXVALUE (100000) is out of range"), "{}", e.message());
    }

    #[test]
    fn seq_tuple_layout_roundtrip() {
        #[repr(align(8))]
        struct Img([u8; 64]);
        let mut img = Img([0u8; 64]);
        let hoff = 24u8;
        img.0[22] = hoff; // t_hoff offset within HeapTupleHeaderData
        let tup = SeqTuple {
            data: img.0.as_mut_ptr(),
            t_len: 64,
            #[cfg(debug_assertions)]
            buf: types_core::InvalidBuffer,
        };
        tup.set(0x1122334455667788, 32, true);
        assert_eq!(tup.last_value(), 0x1122334455667788);
        assert_eq!(tup.log_cnt(), 32);
        assert!(tup.is_called());
        tup.set(-9, 0, false);
        assert_eq!(tup.last_value(), -9);
        assert_eq!(tup.log_cnt(), 0);
        assert!(!tup.is_called());
    }
}
