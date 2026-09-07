//! Witness for the spgdoinsert interrupt protocol (spgdoinsert.c:2028,
//! 2044-2048, 2170-2174, 2328-2358): the insertion loop polls
//! INTERRUPTS_PENDING_CONDITION() *non-destructively* at its head; a pending
//! interrupt breaks out with result = false, the buffers still held are
//! unlocked and released, and only then does CHECK_FOR_INTERRUPTS() run — a
//! cancel throws with nothing locked, anything else is cleared and the caller
//! retries on the `false`.
//!
//! The harness models an interrupt that lands right after the pre-loop
//! CHECK_FOR_INTERRUPTS (C:2028) serviced the previous one: the fake
//! ProcessInterrupts seam clears the flag and, on its first call only, raises
//! it again. With the C ordering the loop head then sees it pending, breaks
//! before touching any buffer, services it, and returns Ok(false). A port
//! that instead services the interrupt at the loop head (and carries on into
//! the buffer manager) trips the read_buffer fake below.
//!
//! Own test binary: seams are set-once per process.

use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

use datum::Datum;
use types_rel::{Relation, RelationData};
use types_spgist::state::SpGistState;
use types_spgist::{spgConfigOut, SpGistTypeDesc};
use types_tuple::itemptr::ItemPointerData;

static CFI_CALLS: AtomicU32 = AtomicU32::new(0);
static READ_BUFFER_CALLS: AtomicU32 = AtomicU32::new(0);

fn install_seams() {
    postgres_seams::check_for_interrupts::set(|| {
        // ProcessInterrupts: consume the pending flag ...
        let n = CFI_CALLS.fetch_add(1, Relaxed);
        init_small::globals::SetInterruptPending(false);
        if n == 0 {
            // ... and let a fresh interrupt arrive right behind the pre-loop
            // service (C:2028), so it is pending at the first loop head.
            init_small::globals::SetInterruptPending(true);
        }
        Ok(())
    });
    bufmgr_seams::read_buffer::set(|_rel, block_num| {
        READ_BUFFER_CALLS.fetch_add(1, Relaxed);
        Err(Box::new(types_error::PgError::error(format!(
            "witness: buffer manager reached for block {block_num} with an interrupt pending"
        ))))
    });
}

fn test_index<'mcx>(mcx: mcx::Mcx<'mcx>) -> RelationData<'mcx> {
    use std::cell::Cell;
    use std::rc::Rc;
    use types_rel::*;
    use types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: types_tuple::TYPALIGN_INT,
        attstorage: types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = mcx::PgVec::new_in(mcx);
    let mut compact = mcx::PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    let rd_att = Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    });
    let mut relname = types_tuple::NameData::default();
    relname.namestrcpy("spgdoinsert_interrupt_witness_idx");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: 4000,
        relfilenode: 1000,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_INDEX,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'n',
        relispartition: false,
        relfrozenxid: 0,
        relminmxid: 0,
    };
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: 1000,
        rd_backend: types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: 1000, dbId: 5 },
        },
        rd_rel,
        rd_att,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily: mcx::PgVec::new_in(mcx),
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: mcx::PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    }
}

// An int4 key opclass state with no compress function: the pre-loop needs
// only the leaf tuple descriptor and the (byval) key type.
fn test_state<'mcx>(index: &RelationData<'mcx>) -> SpGistState<'mcx> {
    let int4 = SpGistTypeDesc {
        type_: 23,
        attlen: 4,
        attbyval: true,
        attalign: types_tuple::TYPALIGN_INT,
        attstorage: types_tuple::TYPSTORAGE_PLAIN,
    };
    SpGistState {
        config: spgConfigOut::default(),
        attType: int4,
        attLeafType: int4,
        attPrefixType: SpGistTypeDesc::default(),
        attLabelType: SpGistTypeDesc::default(),
        leafTupDesc: index.rd_att.clone(),
        redirectXid: 0,
        isBuild: false,
        indexCollation: 0,
        chooseFn: types_fmgr::FmgrInfo::unresolved(),
        picksplitFn: types_fmgr::FmgrInfo::unresolved(),
        compressFn: types_fmgr::FmgrInfo::unresolved(),
        opclassOptions: None,
        frame1: types_fmgr::LocalFcinfo::<1>::new(0),
        frame2: types_fmgr::LocalFcinfo::<2>::new(0),
    }
}

#[test]
fn pending_interrupt_at_loop_head_breaks_before_any_buffer_and_returns_retry() {
    install_seams();
    let owner = mcx::MemoryContext::new("spgdoinsert interrupt witness");
    let mcx = owner.mcx();
    let index = Relation::open(test_index(mcx), None);
    let mut state = test_state(&index);

    init_small::globals::SetInterruptPending(true);
    let r = spgist::spgdoinsert(
        mcx,
        &index,
        &mut state,
        &ItemPointerData::new(1, 1),
        &[Datum::from_i32(42)],
        &[false],
    );

    // C:2028 serviced the first interrupt; C:2044 saw the second one pending
    // and broke out; C:2358 serviced it after the (empty) buffer release.
    assert_eq!(CFI_CALLS.load(Relaxed), 2, "CHECK_FOR_INTERRUPTS before the loop and after it");
    assert_eq!(
        READ_BUFFER_CALLS.load(Relaxed),
        0,
        "loop body ran (buffer manager reached) with an interrupt pending at the loop head"
    );
    match r {
        Ok(false) => {}
        Ok(true) => panic!("insertion reported done; C returns false so the caller retries"),
        Err(e) => panic!("C returns false for a cleared interrupt; got error: {}", e.message()),
    }
    assert!(
        !init_small::globals::InterruptPending(),
        "the post-release CHECK_FOR_INTERRUPTS must have serviced the interrupt"
    );
}
