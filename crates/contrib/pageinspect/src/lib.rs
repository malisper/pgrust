//! `contrib/pageinspect` — raw-page inspection functions.
//!
//! Decoders read C's on-disk page layout byte-for-byte; struct shapes and
//! constants come from the `types_*` crates (WAL cross-replay proves the
//! layouts C-compatible). Bytea page images are copied into a MAXALIGN'd
//! buffer (C's `get_page_from_raw`) and decoded with bounds-checked reads:
//! corrupt input yields C's "just enough validity checking" behavior, never
//! a panic.

#![allow(non_snake_case)]

mod brinfuncs;
mod btreefuncs;
mod fsmfuncs;
mod ginfuncs;
mod gistfuncs;
mod hashfuncs;
mod heapfuncs;
mod rawpage;

use std::any::Any;

use datum::Datum;
use mcx::Mcx;
use types_core::{TEXTOID, TIDOID};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
};
use types_fmgr::{
    byref_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use types_storage::bufpage::{SizeOfPageHeaderData, LP_DEAD, LP_NORMAL, LP_UNUSED};
use types_tuple::tupdesc::TupleDescData;

const LIBRARY: &str = "pageinspect";
pub(crate) const BLCKSZ: usize = types_core::BLCKSZ;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PiVersion {
    V1_8,
    V1_9,
}

pub(crate) fn require_superuser(what: &str) -> PgResult<()> {
    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::error(format!("must be superuser to use {what} functions"))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

#[cold]
pub(crate) fn param_err(msg: impl Into<String>) -> Box<PgError> {
    Box::new(PgError::error(msg.into()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

// ---------------------------------------------------------------- raw pages

#[repr(C, align(8))]
struct AlignedPage([u8; BLCKSZ]);

/// A MAXALIGN'd private copy of one page image (C `get_page_from_raw`).
pub(crate) struct RawPage(Box<AlignedPage>);

impl RawPage {
    pub(crate) fn from_bytes(data: &[u8]) -> PgResult<RawPage> {
        if data.len() != BLCKSZ {
            return Err(Box::new(
                PgError::error("invalid page size")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(format!("Expected {} bytes, got {}.", BLCKSZ, data.len())),
            ));
        }
        let mut b = Box::new(AlignedPage([0u8; BLCKSZ]));
        b.0.copy_from_slice(data);
        Ok(RawPage(b))
    }

    pub(crate) fn arg(fcinfo: &Fcinfo, i: usize) -> PgResult<RawPage> {
        // SAFETY: arg i is a non-null bytea (STRICT or caller-checked).
        let v = unsafe { fcinfo.arg_varlena_packed(i)? };
        Self::from_bytes(v.data())
    }

    pub(crate) fn bytes(&self) -> &[u8; BLCKSZ] {
        &self.0 .0
    }

    pub(crate) fn page_ref(&self) -> types_storage::bufpage::PageRef<'_> {
        let ptr = core::ptr::NonNull::from(&self.0 .0[0]);
        // SAFETY: the buffer is 8-aligned, BLCKSZ long, and lives as long as
        // &self; nothing writes it.
        unsafe { types_storage::bufpage::PageRef::from_raw(ptr) }
    }
}

// ------------------------------------------------- page-header field reads

pub(crate) fn r_u16(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(b[off..off + 2].try_into().unwrap())
}

pub(crate) fn r_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

pub(crate) fn r_u64(b: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}

pub(crate) fn page_lsn(b: &[u8]) -> u64 {
    // pd_lsn is {xlogid, xrecoff}, not a native u64.
    ((r_u32(b, 0) as u64) << 32) | r_u32(b, 4) as u64
}

pub(crate) fn pd_checksum(b: &[u8]) -> u16 {
    r_u16(b, 8)
}
pub(crate) fn pd_flags(b: &[u8]) -> u16 {
    r_u16(b, 10)
}
pub(crate) fn pd_lower(b: &[u8]) -> u16 {
    r_u16(b, 12)
}
pub(crate) fn pd_upper(b: &[u8]) -> u16 {
    r_u16(b, 14)
}
pub(crate) fn pd_special(b: &[u8]) -> u16 {
    r_u16(b, 16)
}
pub(crate) fn pd_pagesize_version(b: &[u8]) -> u16 {
    r_u16(b, 18)
}
pub(crate) fn pd_prune_xid(b: &[u8]) -> u32 {
    r_u32(b, 20)
}

pub(crate) fn page_is_new(b: &[u8]) -> bool {
    pd_upper(b) == 0
}

pub(crate) fn page_size(b: &[u8]) -> u16 {
    pd_pagesize_version(b) & 0xFF00
}

pub(crate) fn page_layout_version(b: &[u8]) -> u16 {
    pd_pagesize_version(b) & 0x00FF
}

pub(crate) fn page_special_size(b: &[u8]) -> u16 {
    page_size(b).wrapping_sub(pd_special(b))
}

pub(crate) fn page_max_offset_number(b: &[u8]) -> usize {
    let lower = pd_lower(b) as usize;
    if lower <= SizeOfPageHeaderData {
        0
    } else {
        (lower - SizeOfPageHeaderData) / 4
    }
}

/// PageGetFreeSpace: usable space assuming one more line pointer is needed.
pub(crate) fn page_free_space(b: &[u8]) -> usize {
    let space = pd_upper(b) as i32 - pd_lower(b) as i32;
    if space < 4 {
        0
    } else {
        (space - 4) as usize
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ItemIdView {
    pub off: u16,
    pub flags: u8,
    pub len: u16,
}

impl ItemIdView {
    pub(crate) fn is_used(&self) -> bool {
        self.flags != LP_UNUSED as u8
    }
    pub(crate) fn is_valid(&self) -> bool {
        // C ItemIdIsValid: lp_flags != LP_UNUSED covers used items; unused
        // entries with lp_len 0 are invalid.
        self.flags != LP_UNUSED as u8 || self.len != 0
    }
    pub(crate) fn is_dead(&self) -> bool {
        self.flags == LP_DEAD as u8
    }
    pub(crate) fn has_storage(&self) -> bool {
        self.len != 0
    }
    pub(crate) fn is_normal(&self) -> bool {
        self.flags == LP_NORMAL as u8
    }
}

/// PageGetItemId over a raw image; out-of-buffer reads (corrupt pd_lower)
/// decode as an all-zero (unused) entry instead of C's overread.
pub(crate) fn page_item_id(b: &[u8], offnum: usize) -> ItemIdView {
    let pos = SizeOfPageHeaderData + (offnum - 1) * 4;
    let raw = if pos + 4 <= b.len() { r_u32(b, pos) } else { 0 };
    ItemIdView {
        off: (raw & 0x7FFF) as u16,
        flags: ((raw >> 15) & 0x3) as u8,
        len: (raw >> 17) as u16,
    }
}

pub(crate) const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

// -------------------------------------------------------------- result glue

pub(crate) fn text_datum(mcx: Mcx<'_>, s: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s)?))
}

pub(crate) fn bytea_datum(mcx: Mcx<'_>, data: &[u8]) -> PgResult<Datum> {
    let mut image = Vec::with_capacity(data.len() + 4);
    image.extend_from_slice(&(((data.len() + 4) as u32) << 2).to_ne_bytes());
    image.extend_from_slice(data);
    byref_result(mcx, &image)
}

pub(crate) fn tid_datum(mcx: Mcx<'_>, tid: &[u8]) -> PgResult<Datum> {
    debug_assert_eq!(tid.len(), 6);
    byref_result(mcx, tid)
}

pub(crate) fn tid_bytes(ip: types_tuple::itemptr::ItemPointerData) -> [u8; 6] {
    let mut out = [0u8; 6];
    out[0..2].copy_from_slice(&ip.ip_blkid.bi_hi.to_ne_bytes());
    out[2..4].copy_from_slice(&ip.ip_blkid.bi_lo.to_ne_bytes());
    out[4..6].copy_from_slice(&ip.ip_posid.to_ne_bytes());
    out
}

pub(crate) fn text_array_datum(mcx: Mcx<'_>, elems: &[Datum]) -> PgResult<Datum> {
    let image = if elems.is_empty() {
        datum::array_build::construct_empty_array_image(mcx, TEXTOID)?
    } else {
        datum::array_build::construct_array_image(mcx, elems, TEXTOID, -1, false, b'i')?
    };
    byref_result(mcx, &image)
}

pub(crate) fn tid_array_datum(mcx: Mcx<'_>, elems: &[Datum]) -> PgResult<Datum> {
    let image = if elems.is_empty() {
        datum::array_build::construct_empty_array_image(mcx, TIDOID)?
    } else {
        datum::array_build::construct_array_image(mcx, elems, TIDOID, 6, false, b's')?
    };
    byref_result(mcx, &image)
}

pub(crate) fn int8_array_datum(mcx: Mcx<'_>, elems: &[Datum]) -> PgResult<Datum> {
    let image =
        datum::array_build::construct_array_image(mcx, elems, types_core::INT8OID, 8, true, b'd')?;
    byref_result(mcx, &image)
}

pub(crate) fn composite_tupdesc<'m>(
    mcx: Mcx<'m>,
    flinfo: &FmgrInfo,
) -> PgResult<TupleDescData<'m>> {
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    Ok(resolved.result_tuple_desc.expect("composite result has tupdesc"))
}

pub(crate) fn tuple_image(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Vec<u8>> {
    let tup = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    Ok(tup.image().to_vec())
}

pub(crate) fn composite_result(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Datum> {
    let tup = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

/// BuildTupleFromCStrings: per-column type input functions over C-formatted
/// strings, so old extension versions' column types resolve exactly as in C.
pub(crate) fn cstrings_tuple_image(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Option<String>],
) -> PgResult<Vec<u8>> {
    let n = tupdesc.natts as usize;
    debug_assert!(values.len() >= n);
    let mut datums = vec![Datum::null(); n];
    let mut nulls = vec![false; n];
    for i in 0..n {
        match &values[i] {
            Some(s) => {
                let att = tupdesc.attr(i);
                let (infunc, typioparam) = lsyscache::getTypeInputInfo(att.atttypid)?;
                let mut flinfo = fmgr_core::fmgr_info(infunc)?;
                let cstr =
                    std::ffi::CString::new(s.as_str()).expect("cstrings_tuple_image: interior NUL");
                datums[i] = types_fmgr::input_function_call(
                    &mut flinfo,
                    Some(&cstr),
                    typioparam,
                    att.atttypmod,
                    mcx,
                )?;
            }
            None => nulls[i] = true,
        }
    }
    tuple_image(mcx, tupdesc, &datums, &nulls)
}

pub(crate) fn cstrings_composite_result(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Option<String>],
) -> PgResult<Datum> {
    let img = cstrings_tuple_image(mcx, tupdesc, values)?;
    byref_result(mcx, &img)
}

// --------------------------------------------- value-per-call SRF plumbing

/// Cross-call state: rows are pre-formed images (tuple images for record
/// sets, bare datum images for scalar sets), streamed one per call.
pub(crate) struct RowSet {
    rows: Vec<Vec<u8>>,
}

pub(crate) fn srf_stream(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    first_call_rows: Option<Vec<Vec<u8>>>,
) -> PgResult<Datum> {
    if let Some(rows) = first_call_rows {
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(RowSet { rows }) as Box<dyn Any>);
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rs = fctx
        .user_fctx
        .as_ref()
        .expect("pageinspect SRF: rows set at first call")
        .downcast_ref::<RowSet>()
        .expect("pageinspect SRF: user_fctx is RowSet");
    match rs.rows.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub(crate) fn notice(msg: impl Into<String>) -> PgResult<()> {
    elog_seams::ereport_msg::call(types_error::NOTICE, msg.into(), None)
}

// --------------------------------------------------------- relation access

/// textToQualifiedNameList + makeRangeVarFromNameList + relation_openrv.
pub(crate) fn relation_open_by_text_arg<'m>(
    mcx: Mcx<'m>,
    fcinfo: &Fcinfo,
    i: usize,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<types_rel::Relation<'m>> {
    // SAFETY: arg i is a non-null text (STRICT).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    let rawname = String::from_utf8_lossy(v.data()).into_owned();
    let encoding = if mbutils_seams::get_database_encoding::is_installed() {
        mbutils_seams::get_database_encoding::call()
    } else {
        wchar::PG_SQL_ASCII
    };
    let names = varlena::split_identifier_string(mcx, &rawname, b'.', encoding)?
        .filter(|l| !l.is_empty())
        .ok_or_else(|| {
            Box::new(
                PgError::error("invalid name syntax")
                    .with_sqlstate(types_error::ERRCODE_INVALID_NAME),
            )
        })?;
    let (catalogname, schemaname, relname) = match names.as_slice() {
        [r] => (None, None, r.as_str()),
        [s, r] => (None, Some(s.as_str()), r.as_str()),
        [c, s, r] => (Some(c.as_str()), Some(s.as_str()), r.as_str()),
        _ => {
            return Err(Box::new(
                PgError::error(format!(
                    "improper relation name (too many dotted names): {rawname}"
                ))
                .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            ))
        }
    };
    let rv = rel_vocab::RangeVar {
        catalogname,
        schemaname,
        relname,
        inh: true,
        relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    relation::relation_openrv(mcx, &rv, lockmode)
}

pub(crate) fn relation_is_other_temp(rel: &types_rel::RelationData<'_>) -> bool {
    rel.rd_rel.relpersistence == types_core::catalog::RELPERSISTENCE_TEMP && !rel.rd_islocaltemp
}

pub(crate) fn other_temp_err() -> Box<PgError> {
    Box::new(
        PgError::error("cannot access temporary tables of other sessions")
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

/// Copy one block of `rel` out under a share lock.
pub(crate) fn read_rel_page(
    rel: &types_rel::RelationData<'_>,
    blkno: types_core::BlockNumber,
) -> PgResult<RawPage> {
    let buf = bufmgr::ReadBufferExtended(
        rel,
        types_core::ForkNumber::MAIN_FORKNUM,
        blkno,
        types_storage::storage::ReadBufferMode::Normal,
        None,
    )?;
    bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;
    let ptr = bufmgr::BufferGetPagePtr(buf);
    let mut page = Box::new(AlignedPage([0u8; BLCKSZ]));
    // SAFETY: a locked, pinned buffer page is BLCKSZ readable.
    unsafe {
        core::ptr::copy_nonoverlapping(ptr.as_ptr(), page.0.as_mut_ptr(), BLCKSZ);
    }
    bufmgr::UnlockReleaseBuffer(buf)?;
    Ok(RawPage(page))
}

// ------------------------------------------------------------------ lookup

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "get_raw_page" => rawpage::fc_get_raw_page,
        "get_raw_page_1_9" => rawpage::fc_get_raw_page_1_9,
        "get_raw_page_fork" => rawpage::fc_get_raw_page_fork,
        "get_raw_page_fork_1_9" => rawpage::fc_get_raw_page_fork_1_9,
        "page_header" => rawpage::fc_page_header,
        "page_checksum" => rawpage::fc_page_checksum,
        "page_checksum_1_9" => rawpage::fc_page_checksum_1_9,
        "heap_page_items" => heapfuncs::fc_heap_page_items,
        "tuple_data_split" => heapfuncs::fc_tuple_data_split,
        "heap_tuple_infomask_flags" => heapfuncs::fc_heap_tuple_infomask_flags,
        "bt_metap" => btreefuncs::fc_bt_metap,
        "bt_page_stats" => btreefuncs::fc_bt_page_stats,
        "bt_page_stats_1_9" => btreefuncs::fc_bt_page_stats_1_9,
        "bt_multi_page_stats" => btreefuncs::fc_bt_multi_page_stats,
        "bt_page_items" => btreefuncs::fc_bt_page_items,
        "bt_page_items_1_9" => btreefuncs::fc_bt_page_items_1_9,
        "bt_page_items_bytea" => btreefuncs::fc_bt_page_items_bytea,
        "brin_page_type" => brinfuncs::fc_brin_page_type,
        "brin_page_items" => brinfuncs::fc_brin_page_items,
        "brin_metapage_info" => brinfuncs::fc_brin_metapage_info,
        "brin_revmap_data" => brinfuncs::fc_brin_revmap_data,
        "gin_metapage_info" => ginfuncs::fc_gin_metapage_info,
        "gin_page_opaque_info" => ginfuncs::fc_gin_page_opaque_info,
        "gin_leafpage_items" => ginfuncs::fc_gin_leafpage_items,
        "gist_page_opaque_info" => gistfuncs::fc_gist_page_opaque_info,
        "gist_page_items" => gistfuncs::fc_gist_page_items,
        "gist_page_items_bytea" => gistfuncs::fc_gist_page_items_bytea,
        "hash_page_type" => hashfuncs::fc_hash_page_type,
        "hash_page_stats" => hashfuncs::fc_hash_page_stats,
        "hash_page_items" => hashfuncs::fc_hash_page_items,
        "hash_bitmap_info" => hashfuncs::fc_hash_bitmap_info,
        "hash_metapage_info" => hashfuncs::fc_hash_metapage_info,
        "fsm_page_contents" => fsmfuncs::fc_fsm_page_contents,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // rawpage.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_storage::bufpage::{ItemIdData, PageHeaderData};

    fn synthetic_page() -> Box<AlignedPage> {
        let mut b = Box::new(AlignedPage([0u8; BLCKSZ]));
        let hdr = PageHeaderData {
            pd_lsn: types_storage::bufpage::PageXLogRecPtr::from_lsn(0x0000_0001_2345_6789),
            pd_checksum: 0xBEEF,
            pd_flags: 0x0004,
            pd_lower: (SizeOfPageHeaderData + 2 * 4) as u16,
            pd_upper: 8000,
            pd_special: (BLCKSZ - 16) as u16,
            pd_pagesize_version: (BLCKSZ as u16) | 4,
            pd_prune_xid: 731,
            pd_linp: [],
        };
        // SAFETY: repr(C), offset-asserted header into an aligned buffer.
        unsafe {
            core::ptr::copy_nonoverlapping(
                (&hdr as *const PageHeaderData).cast::<u8>(),
                b.0.as_mut_ptr(),
                SizeOfPageHeaderData,
            );
        }
        // ItemIdData bit layout: lp_off | lp_flags<<15 | lp_len<<17.
        let raw: u32 = 0x1f40 | (LP_NORMAL << 15) | (60 << 17);
        b.0[24..28].copy_from_slice(&raw.to_ne_bytes());
        let id = ItemIdData::new(0x1f40, LP_NORMAL, 60);
        assert_eq!((id.lp_off(), id.lp_flags(), id.lp_len()), (0x1f40, LP_NORMAL, 60));
        b
    }

    #[test]
    fn page_header_parity() {
        let p = synthetic_page();
        let b = &p.0;
        assert_eq!(page_lsn(b), 0x0000_0001_2345_6789);
        assert_eq!(pd_checksum(b), 0xBEEF);
        assert_eq!(pd_flags(b), 0x0004);
        assert_eq!(pd_lower(b) as usize, SizeOfPageHeaderData + 8);
        assert_eq!(pd_upper(b), 8000);
        assert_eq!(pd_special(b) as usize, BLCKSZ - 16);
        assert_eq!(page_size(b) as usize, BLCKSZ);
        assert_eq!(page_layout_version(b), 4);
        assert_eq!(pd_prune_xid(b), 731);
        assert_eq!(page_special_size(b), 16);
        assert_eq!(page_max_offset_number(b), 2);
        assert!(!page_is_new(b));
        assert!(page_is_new(&[0u8; BLCKSZ]));
    }

    #[test]
    fn item_id_bit_parity() {
        let p = synthetic_page();
        let id = page_item_id(&p.0, 1);
        assert_eq!((id.off, id.flags as u32, id.len), (0x1f40, LP_NORMAL, 60));
        assert!(id.is_normal() && id.is_used() && id.is_valid() && id.has_storage());
        let unused = page_item_id(&p.0, 2);
        assert!(!unused.is_used() && !unused.is_valid());
        // Beyond-buffer offsets decode as unused instead of overreading.
        let clamped = page_item_id(&p.0, BLCKSZ);
        assert!(!clamped.is_used());
    }

    #[test]
    fn checksum_c_corpus_golden() {
        // contrib/pageinspect/expected/checksum.out (BLCKSZ 8K, LE).
        let p01 = [0x01u8; BLCKSZ];
        assert_eq!(rawpage::pg_checksum_page(&p01, 0) as i16, 1175);
        assert_eq!(rawpage::pg_checksum_page(&p01, 50) as i16, 1225);
        assert_eq!(rawpage::pg_checksum_page(&p01, 100) as i16, 1139);
        let mut pabcd = [0u8; BLCKSZ];
        for (i, x) in pabcd.iter_mut().enumerate() {
            *x = if i % 2 == 0 { 0xab } else { 0xcd };
        }
        assert_eq!(rawpage::pg_checksum_page(&pabcd, 0) as i16, -30781);
    }
}
