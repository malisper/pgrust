//! hashfuncs.c — hash page inspection.

use crate::*;
use types_core::{BlockNumber, MaxBlockNumber, HASH_AM_OID};
use types_error::{ERRCODE_INDEX_CORRUPTED, ERRCODE_WRONG_OBJECT_TYPE};
use types_hash::hashpage::{
    HashMetaPageData, HashPageOpaqueData, HASH_MAGIC, HASH_MAX_BITMAPS, HASH_MAX_SPLITPOINTS,
    HASH_VERSION, LH_BITMAP_PAGE, LH_BUCKET_PAGE, LH_META_PAGE, LH_OVERFLOW_PAGE, LH_PAGE_TYPE,
    LH_UNUSED_PAGE,
};
use types_rel::pg_class::RELKIND_INDEX;

const HASHO_PAGE_ID: u16 = 0xFF80;
const HASH_OPAQUE_SIZE: usize = core::mem::size_of::<HashPageOpaqueData>();

pub(crate) fn hash_opaque(b: &[u8]) -> HashPageOpaqueData {
    let sp = pd_special(b) as usize;
    if sp + HASH_OPAQUE_SIZE > b.len() {
        return HashPageOpaqueData {
            hasho_prevblkno: 0,
            hasho_nextblkno: 0,
            hasho_bucket: 0,
            hasho_flag: 0,
            hasho_page_id: 0,
        };
    }
    HashPageOpaqueData {
        hasho_prevblkno: r_u32(b, sp),
        hasho_nextblkno: r_u32(b, sp + 4),
        hasho_bucket: r_u32(b, sp + 8),
        hasho_flag: r_u16(b, sp + 12),
        hasho_page_id: r_u16(b, sp + 14),
    }
}

fn read_meta(b: &[u8]) -> HashMetaPageData {
    // SAFETY: HashMetaPageData is repr(C), offset-asserted, and fits in the
    // page (metapage layout).
    unsafe {
        b.as_ptr()
            .add(SizeOfPageHeaderData)
            .cast::<HashMetaPageData>()
            .read_unaligned()
    }
}

/// C verify_hash_page: type-check a raw page image or die in the attempt.
fn verify_hash_page(page: &RawPage, flags: u16) -> PgResult<u16> {
    let b = page.bytes();
    let mut pagetype = LH_UNUSED_PAGE;
    if !page_is_new(b) {
        if page_special_size(b) as usize != maxalign(HASH_OPAQUE_SIZE) {
            return Err(Box::new(
                PgError::error(format!("input page is not a valid {} page", "hash"))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(format!(
                        "Expected special size {}, got {}.",
                        maxalign(HASH_OPAQUE_SIZE),
                        page_special_size(b)
                    )),
            ));
        }
        let opaque = hash_opaque(b);
        if opaque.hasho_page_id != HASHO_PAGE_ID {
            return Err(Box::new(
                PgError::error(format!("input page is not a valid {} page", "hash"))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(format!(
                        "Expected {:08x}, got {:08x}.",
                        HASHO_PAGE_ID, opaque.hasho_page_id
                    )),
            ));
        }
        pagetype = opaque.hasho_flag & LH_PAGE_TYPE;
    }

    if pagetype != LH_OVERFLOW_PAGE
        && pagetype != LH_BUCKET_PAGE
        && pagetype != LH_BITMAP_PAGE
        && pagetype != LH_META_PAGE
        && pagetype != LH_UNUSED_PAGE
    {
        return Err(param_err(format!("invalid hash page type {pagetype:08x}")));
    }

    if flags != 0 && pagetype & flags == 0 {
        let msg = match flags {
            LH_META_PAGE => "page is not a hash meta page",
            f if f == LH_BUCKET_PAGE | LH_OVERFLOW_PAGE => {
                "page is not a hash bucket or overflow page"
            }
            LH_OVERFLOW_PAGE => "page is not a hash overflow page",
            _ => {
                return Err(Box::new(PgError::error(format!(
                    "hash page of type {pagetype:08x} not in mask {flags:08x}"
                ))))
            }
        };
        return Err(param_err(msg));
    }

    if pagetype == LH_META_PAGE {
        let metap = read_meta(b);
        if metap.hashm_magic != HASH_MAGIC {
            return Err(Box::new(
                PgError::error("invalid magic number for metadata")
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Expected 0x{:08x}, got 0x{:08x}.",
                        HASH_MAGIC, metap.hashm_magic
                    )),
            ));
        }
        if metap.hashm_version != HASH_VERSION {
            return Err(Box::new(
                PgError::error("invalid version for metadata")
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Expected {}, got {}.",
                        HASH_VERSION, metap.hashm_version
                    )),
            ));
        }
    }

    Ok(pagetype)
}

pub(crate) fn fc_hash_page_type(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    let pagetype = verify_hash_page(&page, 0)?;

    let typ: &str = if page_is_new(page.bytes()) {
        "unused"
    } else {
        match pagetype {
            LH_META_PAGE => "metapage",
            LH_OVERFLOW_PAGE => "overflow",
            LH_BUCKET_PAGE => "bucket",
            LH_BITMAP_PAGE => "bitmap",
            _ => "unused",
        }
    };

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    text_datum(mcx, typ.as_bytes())
}

pub(crate) fn fc_hash_page_stats(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hash_page_stats: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    verify_hash_page(&page, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;
    let b = page.bytes();

    let opaque = hash_opaque(b);
    let mut live_items = 0i32;
    let mut dead_items = 0i32;
    for off in 1..=page_max_offset_number(b) {
        if page_item_id(b, off).is_dead() {
            dead_items += 1;
        } else {
            live_items += 1;
        }
    }

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let values = [
        Datum::from_i32(live_items),
        Datum::from_i32(dead_items),
        Datum::from_i32(page_size(b) as i32),
        Datum::from_i32(page_free_space(b) as i32),
        Datum::from_i64(opaque.hasho_prevblkno as i64),
        Datum::from_i64(opaque.hasho_nextblkno as i64),
        Datum::from_i64(opaque.hasho_bucket as i64),
        Datum::from_i32(opaque.hasho_flag as i32),
        Datum::from_i32(opaque.hasho_page_id as i32),
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 9])
}

pub(crate) fn fc_hash_page_items(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hash_page_items: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let rows = if !flinfo.has_fn_extra() {
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let page = RawPage::arg(fcinfo, 0)?;
        verify_hash_page(&page, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;
        let b = page.bytes();

        let tupdesc = composite_tupdesc(mcx, flinfo)?;
        let maxoff = page_max_offset_number(b);
        let mut rows = Vec::with_capacity(maxoff);
        for offnum in 1..=maxoff {
            let id = page_item_id(b, offnum);
            if !id.is_valid() {
                return Err(Box::new(PgError::error("invalid ItemId")));
            }
            let pos = id.off as usize;
            if pos + 12 > b.len() {
                return Err(Box::new(PgError::error("invalid ItemId")));
            }
            // SAFETY: hashkey sits at the tuple data offset, bounded above.
            let hashkey = unsafe { hash::util::_hash_get_indextuple_hashkey(b.as_ptr().add(pos)) };

            let values = [
                Datum::from_i32(offnum as i32),
                tid_datum(mcx, &b[pos..pos + 6])?,
                Datum::from_i64(hashkey as i64),
            ];
            rows.push(tuple_image(mcx, &tupdesc, &values, &[false; 3])?);
        }
        Some(rows)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, rows)
}

/// _hash_checkpage's buffer-page variant for relation reads.
fn checked_rel_hash_page(
    rel: &types_rel::RelationData<'_>,
    blkno: BlockNumber,
    flags: u16,
) -> PgResult<RawPage> {
    let page = read_rel_page(rel, blkno)?;
    let b = page.bytes();
    if page_is_new(b) {
        return Err(Box::new(
            PgError::error(format!(
                "index \"{}\" contains unexpected zero page at block {}",
                rel.name(),
                blkno
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint("Please REINDEX it."),
        ));
    }
    let corrupted = || {
        Box::new(
            PgError::error(format!(
                "index \"{}\" contains corrupted page at block {}",
                rel.name(),
                blkno
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint("Please REINDEX it."),
        )
    };
    if page_special_size(b) as usize != maxalign(HASH_OPAQUE_SIZE) {
        return Err(corrupted());
    }
    let opaque = hash_opaque(b);
    if flags != 0 && opaque.hasho_flag & flags == 0 {
        return Err(corrupted());
    }
    if flags == LH_META_PAGE {
        let metap = read_meta(b);
        if metap.hashm_magic != HASH_MAGIC {
            // C _hash_checkpage's magic error carries no errhint.
            return Err(Box::new(
                PgError::error(format!("index \"{}\" is not a hash index", rel.name()))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED),
            ));
        }
        if metap.hashm_version != HASH_VERSION {
            return Err(Box::new(
                PgError::error(format!(
                    "index \"{}\" has wrong hash version",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_hint("Please REINDEX it."),
            ));
        }
    }
    Ok(page)
}

pub(crate) fn fc_hash_bitmap_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hash_bitmap_info: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let index_relid = fcinfo.arg(0).as_oid();
    let ovflblkno = fcinfo.arg(1).as_i64();

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, index_relid, types_rel::AccessShareLock)?;

    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != HASH_AM_OID {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not a {} index", rel.name(), "hash"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if relation_is_other_temp(&rel) {
        return Err(other_temp_err());
    }
    if ovflblkno < 0 || ovflblkno > MaxBlockNumber as i64 {
        return Err(param_err("invalid block number"));
    }
    if ovflblkno as BlockNumber
        >= bufmgr::RelationGetNumberOfBlocksInFork(&rel, types_core::ForkNumber::MAIN_FORKNUM)?
    {
        return Err(param_err(format!(
            "block number {} is out of range for relation \"{}\"",
            ovflblkno,
            rel.name()
        )));
    }

    // Read the metapage to find the right bitmap page.
    let metapage = checked_rel_hash_page(&rel, 0, LH_META_PAGE)?;
    let metap = read_meta(metapage.bytes());

    // The bit is only meaningful for overflow pages.
    if ovflblkno == 0 {
        return Err(param_err(format!("invalid overflow block number {ovflblkno}")));
    }
    for i in 0..metap.hashm_nmaps as usize {
        if metap.hashm_mapp[i] as i64 == ovflblkno {
            return Err(param_err(format!("invalid overflow block number {ovflblkno}")));
        }
    }

    let ovflbitno = hash::ovfl::_hash_ovflblkno_to_bitno(&metap, ovflblkno as BlockNumber)?;

    let bmpgsz_bit = (metap.hashm_bmsize as u32) << 3;
    let bitmappage = ovflbitno >> metap.hashm_bmshift;
    let bitmapbit = ovflbitno & (bmpgsz_bit - 1);

    if bitmappage >= metap.hashm_nmaps {
        return Err(param_err(format!("invalid overflow block number {ovflblkno}")));
    }
    let bitmapblkno = metap.hashm_mapp[bitmappage as usize];

    let mappage = checked_rel_hash_page(&rel, bitmapblkno, LH_BITMAP_PAGE)?;
    let word = r_u32(
        mappage.bytes(),
        SizeOfPageHeaderData + (bitmapbit as usize / 32) * 4,
    );
    let bit = word & (1 << (bitmapbit % 32)) != 0;

    rel.close(types_rel::AccessShareLock)?;

    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    let values = [
        Datum::from_i64(bitmapblkno as i64),
        Datum::from_i32(bitmapbit as i32),
        Datum::from_bool(bit),
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 3])
}

pub(crate) fn fc_hash_metapage_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hash_metapage_info: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    verify_hash_page(&page, LH_META_PAGE)?;
    let metad = read_meta(page.bytes());

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let mut spares = Vec::with_capacity(HASH_MAX_SPLITPOINTS);
    for i in 0..HASH_MAX_SPLITPOINTS {
        spares.push(Datum::from_i64(metad.hashm_spares[i] as i64));
    }
    let mut mapp = Vec::with_capacity(HASH_MAX_BITMAPS);
    for i in 0..HASH_MAX_BITMAPS {
        mapp.push(Datum::from_i64(metad.hashm_mapp[i] as i64));
    }

    let values = [
        Datum::from_i64(metad.hashm_magic as i64),
        Datum::from_i64(metad.hashm_version as i64),
        Datum::from_f64(metad.hashm_ntuples),
        Datum::from_i32(metad.hashm_ffactor as i32),
        Datum::from_i32(metad.hashm_bsize as i32),
        Datum::from_i32(metad.hashm_bmsize as i32),
        Datum::from_i32(metad.hashm_bmshift as i32),
        Datum::from_i64(metad.hashm_maxbucket as i64),
        Datum::from_i64(metad.hashm_highmask as i64),
        Datum::from_i64(metad.hashm_lowmask as i64),
        Datum::from_i64(metad.hashm_ovflpoint as i64),
        Datum::from_i64(metad.hashm_firstfree as i64),
        Datum::from_i64(metad.hashm_nmaps as i64),
        Datum::from_oid(metad.hashm_procid),
        int8_array_datum(mcx, &spares)?,
        int8_array_datum(mcx, &mapp)?,
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 16])
}
