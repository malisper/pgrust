use datum::Datum;
use mcx::{Mcx, PgVec};
use std::cell::Cell;
use types_core::{AttrNumber, InvalidOid, Oid, FLOAT4OID};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use types_tuple::HeapTupleData;

// pg_statistic.h
pub const STATISTIC_NUM_SLOTS: usize = 5;
pub const ANUM_PG_STATISTIC_STANUMBERS1: i32 = 22;
pub const ANUM_PG_STATISTIC_STAVALUES1: i32 = 27;
// lsyscache.h
pub const ATTSTATSSLOT_VALUES: i32 = 0x01;
pub const ATTSTATSSLOT_NUMBERS: i32 = 0x02;

pub type GetAttAvgWidthHook = fn(Oid, AttrNumber) -> i32;

thread_local! {
    static GET_ATTAVGWIDTH_HOOK: Cell<Option<GetAttAvgWidthHook>> = const { Cell::new(None) };
}

pub fn set_get_attavgwidth_hook(hook: Option<GetAttAvgWidthHook>) -> Option<GetAttAvgWidthHook> {
    GET_ATTAVGWIDTH_HOOK.with(|cell| cell.replace(hook))
}

pub fn get_attavgwidth(relid: Oid, attnum: AttrNumber) -> PgResult<i32> {
    if let Some(hook) = GET_ATTAVGWIDTH_HOOK.with(|cell| cell.get()) {
        let stawidth = hook(relid, attnum);
        if stawidth > 0 {
            return Ok(stawidth);
        }
    }
    if let Some(stawidth) = syscache_seams::pg_statistic_stawidth::call(relid, attnum, false)? {
        if stawidth > 0 {
            return Ok(stawidth);
        }
    }
    Ok(0)
}

// lsyscache.h AttStatsSlot. `values_arr` is C's detoasted stavalues array,
// kept only while by-reference `values` datums point into it; C's
// numbers_arr is subsumed by the owned `numbers` copy.
#[derive(Debug)]
pub struct AttStatsSlot<'mcx> {
    pub staop: Oid,
    pub stacoll: Oid,
    pub valuetype: Oid,
    pub values: PgVec<'mcx, Datum>,
    pub numbers: PgVec<'mcx, f32>,
    pub values_arr: Option<PgVec<'mcx, u8>>,
}

// lsyscache.c:3532 get_attstatsslot. `statstuple` is a pg_statistic tuple
// (from the syscache or a stats hook); the port reads its fixed columns and
// array columns through syscache_seams, everything else is C's shape.
pub fn get_attstatsslot<'mcx>(
    mcx: Mcx<'mcx>,
    statstuple: &HeapTupleData<'_>,
    reqkind: i32,
    reqop: Oid,
    flags: i32,
) -> PgResult<Option<AttStatsSlot<'mcx>>> {
    let stats = syscache_seams::pg_statistic_slot_shape::call(statstuple);
    let Some(i) = (0..STATISTIC_NUM_SLOTS).find(|&i| {
        stats.stakind[i] as i32 == reqkind && (reqop == InvalidOid || stats.staop[i] == reqop)
    }) else {
        return Ok(None);
    };
    let mut sslot = AttStatsSlot {
        staop: stats.staop[i],
        stacoll: stats.stacoll[i],
        valuetype: InvalidOid,
        values: PgVec::new_in(mcx),
        numbers: PgVec::new_in(mcx),
        values_arr: None,
    };

    if flags & ATTSTATSSLOT_VALUES != 0 {
        // SysCacheGetAttrNotNull(STATRELATTINH, statstuple, stavalues1 + i)
        // + DatumGetArrayTypePCopy: a detoasted copy owned by this slot.
        let statarray = syscache_seams::pg_statistic_slot_array_image::call(
            mcx,
            statstuple,
            ANUM_PG_STATISTIC_STAVALUES1 + i as i32,
        )?;
        // ARR_ELEMTYPE(statarray), passed back for the caller.
        let arrayelemtype = array_header_field(&statarray, ARR_ELEMTYPE_OFF)? as Oid;
        sslot.valuetype = arrayelemtype;
        // SearchSysCache1(TYPEOID, arrayelemtype) for the element type's
        // typlen/typbyval/typalign.
        let Some(type_form) = syscache_seams::lookup_pg_type_shape::call(arrayelemtype)? else {
            return Err(crate::typ::type_lookup_failed(arrayelemtype));
        };
        // deconstruct_array: NULLs not expected. By-reference datums point
        // into statarray.
        sslot.values = datum::array_build::deconstruct_array_image(
            mcx,
            &statarray,
            type_form.typlen,
            type_form.typbyval,
            type_form.typalign as u8,
        )?;
        // Keep the array while by-reference datums point into it; a by-value
        // element type leaves all the useful info in values[] (C pfrees).
        if !type_form.typbyval {
            sslot.values_arr = Some(statarray);
        } else {
            drop(statarray);
        }
    }

    if flags & ATTSTATSSLOT_NUMBERS != 0 {
        // SysCacheGetAttrNotNull(STATRELATTINH, statstuple, stanumbers1 + i)
        // + DatumGetArrayTypePCopy.
        let statarray = syscache_seams::pg_statistic_slot_array_image::call(
            mcx,
            statstuple,
            ANUM_PG_STATISTIC_STANUMBERS1 + i as i32,
        )?;
        // The array must be 1-D float4 without nulls (ARR_NDIM / ARR_DIMS[0]
        // / ARR_HASNULL = dataoffset != 0 / ARR_ELEMTYPE); C reads the header
        // words blindly, an image too short to hold them fails the same test.
        let ndim = array_header_field(&statarray, ARR_NDIM_OFF)?;
        let dataoffset = array_header_field(&statarray, ARR_DATAOFFSET_OFF)?;
        let elemtype = array_header_field(&statarray, ARR_ELEMTYPE_OFF)? as Oid;
        let narrayelem = array_header_field(&statarray, ARR_DIMS_OFF)?;
        if ndim != 1 || narrayelem <= 0 || dataoffset != 0 || elemtype != FLOAT4OID {
            return Err(stanumbers_not_float4_array());
        }
        // ARR_DATA_PTR of a 1-D no-nulls array: ARR_OVERHEAD_NONULLS(1) = 24.
        let data = statarray
            .get(ARR_1D_NONULLS_DATA_OFF..)
            .filter(|d| d.len() >= narrayelem as usize * 4)
            .ok_or_else(stanumbers_not_float4_array)?;
        let mut numbers: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(mcx, narrayelem as usize)?;
        numbers.extend(
            data[..narrayelem as usize * 4]
                .chunks_exact(4)
                .map(|b| f32::from_ne_bytes(b.try_into().unwrap())),
        );
        sslot.numbers = numbers;
        // C hands the caller a pointer into statarray and frees it in
        // free_attstatsslot; the owned copy above lets it go now.
        drop(statarray);
    }

    Ok(Some(sslot))
}

// ArrayType header word offsets inside a 4B-header varlena image
// (array.h: vl_len_, ndim, dataoffset, elemtype, dims[]).
const ARR_NDIM_OFF: usize = 4;
const ARR_DATAOFFSET_OFF: usize = 8;
const ARR_ELEMTYPE_OFF: usize = 12;
const ARR_DIMS_OFF: usize = 16;
const ARR_1D_NONULLS_DATA_OFF: usize = 24;

// One int32 header word of an array image. The image came from a catalog
// page, so an image too short for the word is a catchable error rather than
// an out-of-bounds read.
fn array_header_field(image: &[u8], off: usize) -> PgResult<i32> {
    match image.get(off..off + 4) {
        Some(b) => Ok(i32::from_ne_bytes(b.try_into().unwrap())),
        None => Err(Box::new(
            PgError::error("get_attstatsslot: malformed pg_statistic array image")
                .with_sqlstate(ERRCODE_DATA_CORRUPTED),
        )),
    }
}

// lsyscache.c:3610 elog(ERROR, ...).
#[cold]
#[inline(never)]
fn stanumbers_not_float4_array() -> Box<PgError> {
    Box::new(PgError::error("stanumbers is not a 1-D float4 array"))
}

// C pfrees the deconstructed arrays; dropping the slot's PgVecs is the mirror.
pub fn free_attstatsslot(sslot: AttStatsSlot<'_>) {
    drop(sslot);
}
