//! initGISTstate (gist.c): resolve the per-column opclass support procs
//! from the relcache rd_support preload onto a GistState carrier.
use ::mcx::Mcx;
use ::types_core::catalog::DEFAULT_COLLATION_OID;
use ::types_core::fmgr::INDEX_MAX_KEYS;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrInfo, LocalFcinfo};
use ::types_gist::{
    GISTNProcs, GIST_COMPRESS_PROC, GIST_CONSISTENT_PROC, GIST_DECOMPRESS_PROC,
    GIST_DISTANCE_PROC, GIST_EQUAL_PROC, GIST_FETCH_PROC, GIST_PENALTY_PROC, GIST_PICKSPLIT_PROC,
    GIST_UNION_PROC,
};
use ::types_rel::Relation;

pub use ::types_gist::state::GistState;

const InvalidOid: Oid = 0;

fn resolve(proc: Oid) -> PgResult<FmgrInfo> {
    fmgr_seams::fmgr_info::call(proc)
}

fn resolve_optional(proc: Oid) -> PgResult<FmgrInfo> {
    if proc != InvalidOid {
        resolve(proc)
    } else {
        Ok(FmgrInfo::unresolved())
    }
}

#[cold]
#[inline(never)]
fn missing_support_proc(procnum: u16, attno: usize, rel: &Relation<'_>) -> ! {
    panic!(
        "missing support function {procnum} for attribute {} of index \"{}\"",
        attno + 1,
        rel.name()
    )
}

// index_getprocid over the relcache rd_support preload.
pub fn index_getprocid(index: &Relation<'_>, attno_0based: usize, procnum: u16) -> Oid {
    let nsupport = GISTNProcs;
    let base = attno_0based * nsupport;
    index
        .rd_support
        .get(base + (procnum as usize - 1))
        .copied()
        .unwrap_or(InvalidOid)
}



/// initGISTstate; scan-lifetime data lives with the returned owner value.
/// `mcx` must outlive the state (holds the truncated nonLeafTupdesc when the
/// index has INCLUDE columns).
pub fn initGISTstate<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<GistState<'mcx>> {
    let natts = index.rd_att.natts;
    if natts > INDEX_MAX_KEYS as i32 {
        panic!("numberOfAttributes {natts} > {INDEX_MAX_KEYS}");
    }
    let n = natts as usize;
    let nkeys = index.indnkeyatts() as usize;

    let leafTupdesc = index.rd_att.clone();
    // gist.c:1572: internal-page tuples drop the INCLUDE attributes.
    let nonLeafTupdesc = if nkeys == n {
        index.rd_att.clone()
    } else {
        std::rc::Rc::new(tupdesc::CreateTupleDescTruncatedCopy(
            mcx,
            &index.rd_att,
            nkeys as i32,
        )?)
    };

    let mut st = GistState {
        leafTupdesc,
        nonLeafTupdesc,
        fetchTupdesc: None,
        consistentFn: Vec::with_capacity(n),
        unionFn: Vec::with_capacity(n),
        compressFn: Vec::with_capacity(n),
        decompressFn: Vec::with_capacity(n),
        penaltyFn: Vec::with_capacity(n),
        picksplitFn: Vec::with_capacity(n),
        equalFn: Vec::with_capacity(n),
        distanceFn: Vec::with_capacity(n),
        fetchFn: Vec::with_capacity(n),
        supportCollation: Vec::with_capacity(n),
        opclassOptions: Vec::with_capacity(n),
        frame1: LocalFcinfo::<1>::new(0),
        frame2: LocalFcinfo::<2>::new(0),
        frame3: LocalFcinfo::<3>::new(0),
        frame5: LocalFcinfo::<5>::new(0),
    };

    let attoptions = if indexam_seams::relation_get_index_att_options::is_installed() {
        Some(indexam_seams::relation_get_index_att_options::call(index)?)
    } else {
        None
    };

    for i in 0..nkeys {
        let mandatory = |procnum: u16| -> PgResult<FmgrInfo> {
            let oid = index_getprocid(index, i, procnum);
            if oid == InvalidOid {
                missing_support_proc(procnum, i, index);
            }
            resolve(oid)
        };
        st.consistentFn.push(mandatory(GIST_CONSISTENT_PROC)?);
        st.unionFn.push(mandatory(GIST_UNION_PROC)?);
        st.compressFn
            .push(resolve_optional(index_getprocid(index, i, GIST_COMPRESS_PROC))?);
        st.decompressFn
            .push(resolve_optional(index_getprocid(index, i, GIST_DECOMPRESS_PROC))?);
        st.penaltyFn.push(mandatory(GIST_PENALTY_PROC)?);
        st.picksplitFn.push(mandatory(GIST_PICKSPLIT_PROC)?);
        st.equalFn.push(mandatory(GIST_EQUAL_PROC)?);
        st.distanceFn
            .push(resolve_optional(index_getprocid(index, i, GIST_DISTANCE_PROC))?);
        st.fetchFn
            .push(resolve_optional(index_getprocid(index, i, GIST_FETCH_PROC))?);

        let collation = index.rd_indcollation.get(i).copied().unwrap_or(InvalidOid);
        st.supportCollation.push(if collation != InvalidOid {
            collation
        } else {
            DEFAULT_COLLATION_OID
        });

        // C index_getprocinfo: every support-fn FmgrInfo carries the column's
        // parsed opclass options on fn_expr (uninstalled seam = unit-test /
        // bootstrap paths, where no opclass has options).
        let opts = attoptions
            .as_ref()
            .and_then(|a| a.get(i))
            .and_then(|o| o.as_ref())
            .map(|img| Box::new(::types_fmgr::OpclassOptions(img.clone())));
        if let Some(o) = &opts {
            for f in [
                &mut st.consistentFn[i],
                &mut st.unionFn[i],
                &mut st.compressFn[i],
                &mut st.decompressFn[i],
                &mut st.penaltyFn[i],
                &mut st.picksplitFn[i],
                &mut st.equalFn[i],
                &mut st.distanceFn[i],
                &mut st.fetchFn[i],
            ] {
                // SAFETY: the box lives in st.opclassOptions, dropped with
                // the FmgrInfos it serves.
                unsafe { f.set_opclass_options(o) };
            }
        }
        st.opclassOptions.push(opts);
    }

    Ok(st)
}
