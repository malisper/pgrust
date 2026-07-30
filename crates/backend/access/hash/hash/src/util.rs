//! hashutil.c.

use std::cell::RefCell;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext};
use ::types_core::{Oid, RegProcedure};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED};
use ::types_fmgr::{FmgrInfo, LocalFcinfo};
use ::types_hash::{
    Bucket, HashMetaPageData, HashPageOpaqueData, HASHSTANDARD_PROC, HASH_MAGIC, HASH_METAPAGE,
    HASH_READ, HASH_VERSION, LH_META_PAGE,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::PageRef;

use crate::page::{_hash_getbuf, _hash_relbuf, page_opaque, with_meta};

pub use ::types_hash::{_hash_get_totalbuckets, _hash_hashkey2bucket, _hash_spareindex};

/// _hash_checkqual: no scan condition can be rechecked against the stored
/// hash code; hashgettuple sets xs_recheck instead (C keeps the same stub).
#[inline]
pub(crate) fn _hash_checkqual() -> bool {
    true
}

/// index_getprocinfo(rel, 1, HASHSTANDARD_PROC) through the relcache
/// rd_supportinfo resolved-once cache (proc slot 1 is preloaded for hash).
pub fn hash_procinfo(rel: &Relation<'_>) -> PgResult<FmgrInfo> {
    {
        let mut cache = rel.rd_supportinfo.borrow_mut();
        if cache.is_empty() {
            cache.resize_with(rel.indnkeyatts() as usize, || None);
        }
        if let Some(fi) = &cache[0] {
            return Ok(fi.clone());
        }
    }
    // Borrow released across the lookup: resolving the support proc can scan
    // pg_amproc, rebuilding relcache entries and re-entering this scan.
    let opcintype = rel.rd_opcintype[0];
    let proc = lsyscache::get_opfamily_proc(
        rel.rd_opfamily[0],
        opcintype,
        opcintype,
        HASHSTANDARD_PROC as i16,
    )?;
    if proc == 0 {
        return Err(missing_support_function(rel, opcintype, opcintype));
    }
    let fi = fmgr_core::fmgr_info(proc)?;
    rel.rd_supportinfo.borrow_mut()[0] = Some(fi.clone());
    Ok(fi)
}

thread_local! {
    static HASH_PROC_SCRATCH: RefCell<MemoryContext> =
        RefCell::new(MemoryContext::new_bump("hash proc scratch"));
}

#[inline]
fn invoke_hash_proc(finfo: &mut FmgrInfo, collation: Oid, key: Datum) -> PgResult<u32> {
    // Known-set kernel dispatch (rule 4): hashint4 is the M-workload column type.
    match finfo.fn_oid {
        450 => Ok(::hashfn::hash_bytes_uint32(key.as_i32() as u32)),
        _ => HASH_PROC_SCRATCH.with(|cell| match cell.try_borrow_mut() {
            Ok(mut ctx) => {
                ctx.reset();
                invoke_hash_proc_slow(finfo, collation, key, ctx.mcx())
            }
            Err(_) => {
                let ctx = MemoryContext::new_bump("hash proc scratch (reentrant)");
                invoke_hash_proc_slow(finfo, collation, key, ctx.mcx())
            }
        }),
    }
}

// Toasted keys detoast into the armed scratch (C: CurrentMemoryContext); the
// u32 result is by-val, so the copy dies with the reset on next acquisition.
fn invoke_hash_proc_slow(
    finfo: &mut FmgrInfo,
    collation: Oid,
    key: Datum,
    mcx: Mcx<'_>,
) -> PgResult<u32> {
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.rearm(collation);
    // SAFETY: the scratch context outlives the invoke; the by-val result
    // carries no reference into it.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, key);
    let r = finfo.invoke(&mut fcinfo)?;
    if fcinfo.isnull {
        return Err(Box::new(PgError::error(format!(
            "function {} returned NULL",
            finfo.fn_oid
        ))));
    }
    Ok(r.as_u32())
}

/// _hash_datum2hashkey.
pub(crate) fn _hash_datum2hashkey(rel: &Relation<'_>, key: Datum) -> PgResult<u32> {
    let mut procinfo = hash_procinfo(rel)?;
    let collation = rel.rd_indcollation[0];
    invoke_hash_proc(&mut procinfo, collation, key)
}

/// _hash_datum2hashkey_type: cross-type probe through pg_amproc.
pub(crate) fn _hash_datum2hashkey_type(
    rel: &Relation<'_>,
    key: Datum,
    keytype: Oid,
) -> PgResult<u32> {
    let hash_proc: RegProcedure =
        lsyscache::get_opfamily_proc(rel.rd_opfamily[0], keytype, keytype, HASHSTANDARD_PROC as i16)?;
    if hash_proc == 0 {
        return Err(Box::new(PgError::error(format!(
            "missing support function {HASHSTANDARD_PROC}({keytype},{keytype}) for index \"{}\"",
            rel.name()
        ))));
    }
    let mut finfo = fmgr_core::fmgr_info(hash_proc)?;
    let collation = rel.rd_indcollation[0];
    invoke_hash_proc(&mut finfo, collation, key)
}

#[track_caller]
#[cold]
#[inline(never)]
fn missing_support_function(rel: &Relation<'_>, lefttype: Oid, righttype: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "missing support function {HASHSTANDARD_PROC}({lefttype},{righttype}) for attribute 1 of index \"{}\"",
        rel.name()
    )))
}

#[cold]
#[inline(never)]
pub(crate) fn index_corrupted(msg: String, hint: bool) -> Box<PgError> {
    let e = PgError::error(msg).with_sqlstate(ERRCODE_INDEX_CORRUPTED);
    Box::new(if hint { e.with_hint("Please REINDEX it.") } else { e })
}

/// _hash_checkpage.
pub(crate) fn _hash_checkpage(rel: &Relation<'_>, buf: types_core::Buffer, flags: u16) -> PgResult<()> {
    // SAFETY: caller holds the pin (and lock unless HASH_NOLOCK) per the C
    // call contract.
    let page = unsafe { crate::page::page_ref(buf) };
    if page.is_new() {
        return Err(index_corrupted(
            format!(
                "index \"{}\" contains unexpected zero page at block {}",
                rel.name(),
                bufmgr_seams::buffer_get_block_number::call(buf)
            ),
            true,
        ));
    }
    let special_size = ::types_core::BLCKSZ - page.pd_special() as usize;
    if special_size != core::mem::size_of::<HashPageOpaqueData>() {
        return Err(index_corrupted(
            format!(
                "index \"{}\" contains corrupted page at block {}",
                rel.name(),
                bufmgr_seams::buffer_get_block_number::call(buf)
            ),
            true,
        ));
    }
    if flags != 0 {
        let opaque = page_opaque(&page);
        if opaque.hasho_flag & flags == 0 {
            return Err(index_corrupted(
                format!(
                    "index \"{}\" contains corrupted page at block {}",
                    rel.name(),
                    bufmgr_seams::buffer_get_block_number::call(buf)
                ),
                true,
            ));
        }
    }
    if flags == LH_META_PAGE {
        // SAFETY: pinned metapage per above.
        let (magic, version) = unsafe {
            let m = crate::page::meta_ptr(buf);
            ((*m).hashm_magic, (*m).hashm_version)
        };
        if magic != HASH_MAGIC {
            return Err(index_corrupted(
                format!("index \"{}\" is not a hash index", rel.name()),
                false,
            ));
        }
        if version != HASH_VERSION {
            return Err(index_corrupted(
                format!("index \"{}\" has wrong hash version", rel.name()),
                true,
            ));
        }
    }
    Ok(())
}

/// _hash_get_indextuple_hashkey: first attribute, never null, read raw.
/// # Safety
/// `itup` is a live index-tuple image.
#[inline]
pub unsafe fn _hash_get_indextuple_hashkey(itup: nbtree::itup::ITup) -> u32 {
    unsafe {
        let off = nbtree::itup::index_info_find_data_offset(nbtree::itup::t_info(itup));
        itup.add(off).cast::<u32>().read_unaligned()
    }
}

/// _hash_convert_tuple: null keys are not indexed.
pub fn _hash_convert_tuple(
    rel: &Relation<'_>,
    user_values: &[Datum],
    user_isnull: &[bool],
) -> PgResult<Option<Datum>> {
    if user_isnull[0] {
        return Ok(None);
    }
    let hashkey = _hash_datum2hashkey(rel, user_values[0])?;
    Ok(Some(Datum::from_u32(hashkey)))
}

/// _hash_binsearch: first offset with hashkey >= hash_value.
pub(crate) fn _hash_binsearch(page: &PageRef<'_>, hash_value: u32) -> ::types_core::OffsetNumber {
    let mut upper = page.max_offset_number() + 1;
    let mut lower = 1;
    while upper > lower {
        let off = (upper + lower) / 2;
        // SAFETY: off <= max_offset_number on a pinned, checked hash page.
        let hashkey = unsafe {
            let id = page.item_id(off);
            _hash_get_indextuple_hashkey(page.item_raw(id).0)
        };
        if hashkey < hash_value {
            lower = off + 1;
        } else {
            upper = off;
        }
    }
    lower
}

/// _hash_binsearch_last: last offset with hashkey <= hash_value (0 if none).
pub(crate) fn _hash_binsearch_last(page: &PageRef<'_>, hash_value: u32) -> ::types_core::OffsetNumber {
    let mut upper = page.max_offset_number();
    let mut lower = 0;
    while upper > lower {
        let off = (upper + lower + 1) / 2;
        // SAFETY: 1 <= off <= max_offset_number as above.
        let hashkey = unsafe {
            let id = page.item_id(off);
            _hash_get_indextuple_hashkey(page.item_raw(id).0)
        };
        if hashkey > hash_value {
            upper = off - 1;
        } else {
            lower = off;
        }
    }
    lower
}

const CALC_NEW_BUCKET: fn(Bucket, u32) -> Bucket = |old_bucket, lowmask| old_bucket | (lowmask + 1);

/// _hash_get_oldblock_from_newbucket.
pub(crate) fn _hash_get_oldblock_from_newbucket(
    rel: &Relation<'_>,
    new_bucket: Bucket,
) -> PgResult<::types_core::BlockNumber> {
    let mask = (1u32 << (31 - new_bucket.leading_zeros())) - 1;
    let old_bucket = new_bucket & mask;

    let metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE)?;
    let blkno = with_meta(metabuf, |metap: &HashMetaPageData| metap.bucket_to_blkno(old_bucket));
    _hash_relbuf(metabuf)?;
    Ok(blkno)
}

/// _hash_get_newblock_from_oldbucket.
pub(crate) fn _hash_get_newblock_from_oldbucket(
    rel: &Relation<'_>,
    old_bucket: Bucket,
) -> PgResult<::types_core::BlockNumber> {
    let metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE)?;
    let blkno = with_meta(metabuf, |metap: &HashMetaPageData| {
        let new_bucket = _hash_get_newbucket_from_oldbucket(
            old_bucket,
            metap.hashm_lowmask,
            metap.hashm_maxbucket,
        );
        metap.bucket_to_blkno(new_bucket)
    });
    _hash_relbuf(metabuf)?;
    Ok(blkno)
}

/// _hash_get_newbucket_from_oldbucket.
pub(crate) fn _hash_get_newbucket_from_oldbucket(
    old_bucket: Bucket,
    mut lowmask: u32,
    maxbucket: u32,
) -> Bucket {
    let mut new_bucket = CALC_NEW_BUCKET(old_bucket, lowmask);
    if new_bucket > maxbucket {
        lowmask >>= 1;
        new_bucket = CALC_NEW_BUCKET(old_bucket, lowmask);
    }
    new_bucket
}
