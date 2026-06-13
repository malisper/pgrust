//! Family `expandedrecord` — `src/backend/utils/adt/expandedrecord.c`.
//!
//! The expanded-record type: an `ExpandedRecordHeader` (an
//! [`crate::expandeddatum`] `ExpandedObjectHeader` subclass) holding a record
//! value in a form cheap to read and modify field-by-field. Depends on the
//! keystone EOH ABI and on the `domains` family (`domain_check`, via the
//! check_domain_for_new_* helpers). Builders, field get/set, and flatten.
//!
//! Opacity inherited, never introduced: expandedrecord.c manipulates the
//! `ExpandedRecordHeader` struct by direct field access (dvalues/dnulls arrays,
//! the tupdesc pointer + refcount, the flat HeapTuple, nested memory contexts,
//! and a self-referential dummy header). It cannot be modelled as the keystone's
//! opaque `ExpandedObjectRef` byte handle, so the real struct is carried here as
//! [`ExpandedRecordHeader`], field-for-field with `utils/expandedrecord.h`
//! (verified against the c2rust `ExpandedRecordHeader`). In the owned model:
//!   * `Datum *dvalues` / `bool *dnulls` -> `PgVec<TupleValue>` / `PgVec<bool>`
//!     (per-field payloads, matching how `backend-access-common-heaptuple`'s
//!     form/deform core models a tuple field — `ByVal` word or `ByRef` bytes);
//!   * `HeapTuple fvalue` (+ `fstartptr`/`fendptr`) -> `Option<FormedTuple>`
//!     (the C "is this Datum pointer a slice of the flat tuple?" test, used to
//!     decide whether to `pfree` an old field, is structural in the owned model:
//!     a `ByRef` field's bytes are always its own `PgVec`, never a view into the
//!     flat tuple, so a deconstructed-from-flat field carries no separate
//!     allocation to reclaim — the `fstartptr..fendptr` range check becomes the
//!     `field_owns_storage` flag set when we copy a value into the record);
//!   * `MemoryContext` slots -> owned [`mcx::MemoryContext`] children.
//!
//! Construction/field-fetch allocate in the expanded object's own context, so
//! they take `Mcx`; ereport sites surface as `PgResult`.
//!
//! Seam-and-panic for genuinely-unported owners (named at each call site):
//! `lookup_type_cache` / `assign_record_type_identifier` /
//! `assign_record_type_typmod` (typcache, not yet ported — task #58),
//! `detoast_external_attr` / `toast_flatten_tuple` (toast internals,
//! task #76), `datumCopy` (utils/adt/datum.c), `SystemAttributeByName`
//! (catalog/heap.c) and `format_type_be` (utils/adt/format_type.c). The
//! confirmed-available adt-infra is reused via real owners:
//! `lookup_rowtype_tupdesc` (typcache-seams) and the heaptuple form/deform/copy
//! core.

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_tuple::backend_access_common_heaptuple::{FormedTuple, TupleValue};
use types_tuple::heaptuple::{FormData_pg_attribute, TupleDescData, BITMAPLEN, RECORDOID};

use backend_access_common_heaptuple as heaptuple;

/// `SizeofHeapTupleHeader` == `offsetof(HeapTupleHeaderData, t_bits)`.
use types_tuple::heap::SizeofHeapTupleHeader;

/// `MAXALIGN(len)` (`c.h`): round up to the 8-byte MAXIMUM_ALIGNOF boundary.
#[inline]
fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

/// `lookup_rowtype_tupdesc(type_id, typmod)` (typcache.c) via the typcache-seams
/// slot (the confirmed-available adt-infra owner). Returns an owned tupdesc copy.
fn lookup_rowtype_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
) -> PgResult<TupleDescData<'mcx>> {
    let boxed =
        backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(mcx, type_id, typmod)?;
    boxed.clone_in(mcx)
}

// ---------------------------------------------------------------------------
// ABI constants (utils/expandedrecord.h)
// ---------------------------------------------------------------------------

/// `ER_MAGIC` — ID for debugging crosschecks (`expandedrecord.h:40`).
pub const ER_MAGIC: i32 = 1384727874;

pub const ER_FLAG_FVALUE_VALID: i32 = 0x0001;
pub const ER_FLAG_FVALUE_ALLOCED: i32 = 0x0002;
pub const ER_FLAG_DVALUES_VALID: i32 = 0x0004;
pub const ER_FLAG_DVALUES_ALLOCED: i32 = 0x0008;
pub const ER_FLAG_HAVE_EXTERNAL: i32 = 0x0010;
pub const ER_FLAG_TUPDESC_ALLOCED: i32 = 0x0020;
pub const ER_FLAG_IS_DOMAIN: i32 = 0x0040;
pub const ER_FLAG_IS_DUMMY: i32 = 0x0080;
/// flag bits that are not to be cleared when replacing tuple data.
pub const ER_FLAGS_NON_DATA: i32 =
    ER_FLAG_TUPDESC_ALLOCED | ER_FLAG_IS_DOMAIN | ER_FLAG_IS_DUMMY;

/// `TYPTYPE_DOMAIN` (`pg_type.h`).
const TYPTYPE_DOMAIN: i8 = b'd' as i8;
/// `TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO` flag bits (typcache.h);
/// passed through to the unported `lookup_type_cache` owner.
const TYPECACHE_TUPDESC: i32 = 0x00100;
const TYPECACHE_DOMAIN_BASE_INFO: i32 = 0x01000;

// ---------------------------------------------------------------------------
// The carrier struct (utils/expandedrecord.h: ExpandedRecordHeader)
// ---------------------------------------------------------------------------

/// `ExpandedRecordFieldInfo` (`expandedrecord.h:168`): info returned by
/// [`expanded_record_lookup_field`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExpandedRecordFieldInfo {
    /// field's attr number in record.
    pub fnumber: i32,
    /// field's type OID.
    pub ftypeid: Oid,
    /// field's typmod.
    pub ftypmod: i32,
    /// field's collation if any.
    pub fcollation: Oid,
}

/// `ExpandedRecordHeader` (`expandedrecord.h:42`), in the owned model.
///
/// The C `ExpandedObjectHeader hdr` standard prefix (vl_len_, method table,
/// eoh_context, the RW/RO TOAST pointers) collapses: the method dispatch is
/// the two free functions [`er_get_flat_size`]/[`er_flatten_into`] (installed
/// via the `ER_methods` of the keystone), and `eoh_context` is the owned
/// [`ExpandedRecordHeader::obj_cxt`]. All remaining fields mirror the C struct
/// 1:1.
pub struct ExpandedRecordHeader<'mcx> {
    /// `eoh.eoh_context` — the expanded object's private memory context.
    pub obj_cxt: MemoryContext,

    /// `er_magic` — sanity crosscheck value.
    pub er_magic: i32,
    /// `flags` — assorted `ER_FLAG_*` bits.
    pub flags: i32,

    /// `er_decltypeid` — declared type of the record variable (maybe a domain).
    pub er_decltypeid: Oid,
    /// `er_typeid` — type OID of the composite (base) type.
    pub er_typeid: Oid,
    /// `er_typmod` — typmod of the composite type.
    pub er_typmod: i32,

    /// `er_tupdesc` — tuple descriptor (owned copy in this model), if known.
    pub er_tupdesc: Option<TupleDescData<'mcx>>,
    /// `er_tupdesc_id` — unique-within-process identifier for the tupdesc.
    pub er_tupdesc_id: u64,
    /// Whether the typcache's tupdesc was refcounted (so the C path would have
    /// taken/released a refcount + registered the reset callback). Tracked so
    /// the field-for-field mirror of the refcount bookkeeping is preserved.
    pub er_tupdesc_refcounted: bool,

    /// `dvalues` — per-field values (C `Datum *`), present iff DVALUES_VALID.
    pub dvalues: PgVec<'mcx, TupleValue<'mcx>>,
    /// `dnulls` — per-field is-null flags (C `bool *`).
    pub dnulls: PgVec<'mcx, bool>,
    /// Per-field "this field owns separately-allocated storage" flag — the owned
    /// analog of C's `fstartptr <= ptr < fendptr` test (a field deconstructed
    /// from the flat tuple points INTO it and must not be pfree'd; a field we
    /// copied in is separate and is reclaimed on replacement).
    pub dvalues_owned: PgVec<'mcx, bool>,
    /// `nfields` — length of the above arrays.
    pub nfields: i32,

    /// `flat_size` — current flat-equivalent size if known, else 0.
    pub flat_size: usize,
    /// `data_len` — data length within `flat_size`.
    pub data_len: usize,
    /// `hoff` — header offset.
    pub hoff: i32,
    /// `hasnull` — null bitmap needed?
    pub hasnull: bool,

    /// `fvalue` — the flat representation, if we have one.
    pub fvalue: Option<FormedTuple<'mcx>>,

    /// `er_short_term_cxt` — short-lived context for domain checks / detoasting.
    pub er_short_term_cxt: Option<MemoryContext>,

    /// `er_dummy_header` — dummy record header used for domain checks.
    pub er_dummy_header: Option<alloc::boxed::Box<ExpandedRecordHeader<'mcx>>>,
}

impl<'mcx> ExpandedRecordHeader<'mcx> {
    /// `ExpandedRecordIsEmpty(erh)` (`expandedrecord.h:158`).
    #[inline]
    pub fn is_empty(&self) -> bool {
        (self.flags & (ER_FLAG_DVALUES_VALID | ER_FLAG_FVALUE_VALID)) == 0
    }

    /// `ExpandedRecordIsDomain(erh)` (`expandedrecord.h:160`).
    #[inline]
    pub fn is_domain(&self) -> bool {
        (self.flags & ER_FLAG_IS_DOMAIN) != 0
    }

    /// A bare header with all fields zero/null, in `obj_cxt` — the post-`memset`
    /// state before identification info is filled in.
    fn blank(obj_cxt: MemoryContext, mcx: Mcx<'mcx>) -> Self {
        ExpandedRecordHeader {
            obj_cxt,
            er_magic: 0,
            flags: 0,
            er_decltypeid: 0,
            er_typeid: 0,
            er_typmod: 0,
            er_tupdesc: None,
            er_tupdesc_id: 0,
            er_tupdesc_refcounted: false,
            dvalues: PgVec::new_in(mcx),
            dnulls: PgVec::new_in(mcx),
            dvalues_owned: PgVec::new_in(mcx),
            nfields: 0,
            flat_size: 0,
            data_len: 0,
            hoff: 0,
            hasnull: false,
            fvalue: None,
            er_short_term_cxt: None,
            er_dummy_header: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Seam-and-panic boundaries to genuinely-unported owners.
//
// These are NOT this unit's own logic; each is a cross-unit callee whose owner
// is not yet ported in this repo. A real `X-seams` slot belongs in the owner
// crate (this no_std family file cannot host the std-backed `seam!` slot), so
// until those owners land a call here panics loudly — exactly mirror-pg-and-
// panic, never a silent stub of our own behaviour.
// ---------------------------------------------------------------------------

/// Result of `lookup_type_cache(type_id, flags)` so far as this module reads it.
struct TypeCacheView<'mcx> {
    typtype: i8,
    domain_base_type: Oid,
    tup_desc: Option<TupleDescData<'mcx>>,
    tup_desc_identifier: u64,
}

/// `lookup_type_cache(type_id, flags)` (utils/cache/typcache.c). Owner not yet
/// ported (task #58: backend-utils-cache-typcache).
fn lookup_type_cache<'mcx>(_type_id: Oid, _flags: i32) -> TypeCacheView<'mcx> {
    panic!("expandedrecord: lookup_type_cache: unported owner (backend-utils-cache-typcache)")
}

/// `assign_record_type_identifier(type_id, typmod)` (utils/cache/typcache.c).
/// Owner not yet ported (task #58).
fn assign_record_type_identifier(_type_id: Oid, _typmod: i32) -> u64 {
    panic!(
        "expandedrecord: assign_record_type_identifier: unported owner \
         (backend-utils-cache-typcache)"
    )
}

/// `assign_record_type_typmod(tupdesc)` (utils/cache/typcache.c) — registers an
/// anonymous RECORD tupdesc and stamps it with the assigned typmod. Owner not
/// yet ported (task #58). Returns the assigned tdtypmod.
fn assign_record_type_typmod(_tupdesc: &mut TupleDescData<'_>) -> i32 {
    panic!(
        "expandedrecord: assign_record_type_typmod: unported owner \
         (backend-utils-cache-typcache)"
    )
}

/// `domain_check(value, isnull, domainType, extra, mcxt)` (utils/adt/domains.c).
/// The sibling `domains` family lives in this same unit; its `domain_check` is a
/// thin `(void) domain_check_internal(..., escontext = NULL)` that re-runs the
/// typcache-resident `domain_check_input` engine. We route through that same
/// engine seam directly (the owned model carries no `extra` memoization handle).
fn domain_check(value: Datum, isnull: bool, domain_type: Oid) -> PgResult<()> {
    backend_utils_cache_typcache_seams::domain_check_input::call(value, isnull, domain_type)
}

/// `detoast_external_attr(attr)` (access/common/detoast.c). Owner not yet ported
/// (task #76: backend-access-common-toast-internals). Returns the detoasted
/// (inline) bytes of an external varlena.
fn detoast_external_attr<'mcx>(_mcx: Mcx<'mcx>, _attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    panic!(
        "expandedrecord: detoast_external_attr: unported owner \
         (backend-access-common-toast-internals)"
    )
}

/// `toast_flatten_tuple(tup, tupdesc)` (access/heap/tuptoaster path). Owner not
/// yet ported (task #76). Returns a tuple with all out-of-line values inlined.
fn toast_flatten_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _tup: &FormedTuple<'_>,
    _tupdesc: &TupleDescData<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    panic!(
        "expandedrecord: toast_flatten_tuple: unported owner \
         (backend-access-common-toast-internals)"
    )
}

/// `datumCopy(value, typByVal, typLen)` (utils/adt/datum.c). Owner not yet
/// ported as a callable seam; the owned model copies the field's payload into
/// `mcx`, which is the faithful effect of `datumCopy` for the by-reference case.
fn datum_copy<'mcx>(
    mcx: Mcx<'mcx>,
    value: &TupleValue<'_>,
    _typbyval: bool,
    _typlen: i16,
) -> PgResult<TupleValue<'mcx>> {
    value.clone_in(mcx)
}

/// `SystemAttributeByName(attname)` (catalog/heap.c). Owner not yet ported.
fn system_attribute_by_name(_fieldname: &str) -> Option<FormData_pg_attribute> {
    panic!("expandedrecord: SystemAttributeByName: unported owner (catalog/heap.c)")
}

/// `ereport(ERROR, errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("type %s is not
/// composite", format_type_be(type_id)))` (expandedrecord.c:98 / :230) — the
/// "type is not composite" error raised by the builders when the typcache has
/// no tupdesc for the given type. `format_type_be` is a real merged owner
/// (`backend-utils-adt-format-type-seams`, same slot rowtypes.rs uses), so this
/// is a catchable user error, not an unported boundary.
fn type_is_not_composite(mcx: Mcx<'_>, type_oid: Oid) -> PgError {
    let name = match backend_utils_adt_format_type_seams::format_type_be::call(mcx, type_oid) {
        Ok(s) => alloc::string::String::from(s.as_str()),
        Err(e) => return e,
    };
    PgError::error(alloc::format!("type {name} is not composite"))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
}

// ---------------------------------------------------------------------------
// varatt helpers (varatt.h) — small, header-only tests this module needs.
// ---------------------------------------------------------------------------

/// `VARATT_IS_EXTERNAL(PTR)` == `VARATT_IS_1B_E(PTR)` (varatt.h): the first byte
/// is the 1-byte external marker `0x01`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    !b.is_empty() && b[0] == 0x01
}

// ---------------------------------------------------------------------------
// ER method table: get_flat_size / flatten_into (the keystone's ER_methods).
// ---------------------------------------------------------------------------

/// `ER_get_flat_size(eohptr)` (expandedrecord.c:651) — the `get_flat_size`
/// method for expanded records.
///
/// Note: C asks callers to invoke this in a short-lived context (detoasting may
/// leak); here the detoast goes through the short-term context as in the C.
pub fn er_get_flat_size<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
) -> PgResult<usize> {
    debug_assert_eq!(erh.er_magic, ER_MAGIC);

    // The flat representation has to be a valid composite datum.  Make sure
    // that we have a registered, not anonymous, RECORD type.
    if erh.er_typeid == RECORDOID && erh.er_typmod < 0 {
        expanded_record_get_tupdesc(mcx, erh)?;
        let td = erh.er_tupdesc.as_mut().expect("tupdesc valid by now");
        let new_typmod = assign_record_type_typmod(td);
        td.tdtypmod = new_typmod;
        erh.er_typmod = new_typmod;
    }

    // If we have a valid flattened value without out-of-line fields, use it.
    if (erh.flags & ER_FLAG_FVALUE_VALID) != 0 && (erh.flags & ER_FLAG_HAVE_EXTERNAL) == 0 {
        return Ok(erh.fvalue.as_ref().expect("FVALUE_VALID").tuple.t_len as usize);
    }

    // If we have a cached size value, believe that.
    if erh.flat_size != 0 {
        return Ok(erh.flat_size);
    }

    // If we haven't yet deconstructed the tuple, do that.
    if (erh.flags & ER_FLAG_DVALUES_VALID) == 0 {
        deconstruct_expanded_record(mcx, erh)?;
    }

    // Composite datums mustn't contain any out-of-line values.
    if (erh.flags & ER_FLAG_HAVE_EXTERNAL) != 0 {
        let nfields = erh.nfields;
        for i in 0..nfields {
            let (is_external, val) = {
                let td = erh.er_tupdesc.as_ref().expect("tupdesc valid by now");
                let attr = td.compact_attr(i as usize);
                let external = !erh.dnulls[i as usize]
                    && !attr.attbyval
                    && attr.attlen == -1
                    && matches!(&erh.dvalues[i as usize], TupleValue::ByRef(b) if varatt_is_external(b));
                (external, ())
            };
            let _ = val;
            if is_external {
                // expanded_record_set_field_internal can do the detoasting; it
                // needn't recheck domain constraints.
                let new_value = erh.dvalues[i as usize].clone_in(mcx)?;
                expanded_record_set_field_internal(
                    mcx,
                    erh,
                    i + 1,
                    new_value,
                    false,
                    true,  // expand_external
                    false, // check_constraints
                )?;
            }
        }
        // We have now removed all external field values.
        erh.flags &= !ER_FLAG_HAVE_EXTERNAL;
    }

    // Test if we currently have any null values.
    let mut hasnull = false;
    for i in 0..erh.nfields {
        if erh.dnulls[i as usize] {
            hasnull = true;
            break;
        }
    }

    // Determine total space needed.
    // len = offsetof(HeapTupleHeaderData, t_bits)
    let mut len = SizeofHeapTupleHeader;

    if hasnull {
        let td = erh.er_tupdesc.as_ref().expect("tupdesc valid by now");
        len += BITMAPLEN(td.natts) as usize;
    }

    len = maxalign(len); // align user data safely
    let hoff = len;

    let data_len = {
        let td = erh.er_tupdesc.as_ref().expect("tupdesc valid by now");
        heaptuple::heap_compute_data_size(td, &erh.dvalues, &erh.dnulls)?
    };

    len += data_len;

    // Cache for next time.
    erh.flat_size = len;
    erh.data_len = data_len;
    erh.hoff = hoff as i32;
    erh.hasnull = hasnull;

    Ok(len)
}

/// `ER_flatten_into(eohptr, result, allocated_size)` (expandedrecord.c:763) —
/// the `flatten_into` method. Writes the flat composite datum image into `dest`
/// (which is exactly `er_get_flat_size` bytes long).
pub fn er_flatten_into<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    dest: &mut [u8],
) -> PgResult<()> {
    debug_assert_eq!(erh.er_magic, ER_MAGIC);

    let allocated_size = dest.len();

    // Easy if we have a valid flattened value without out-of-line fields.
    if (erh.flags & ER_FLAG_FVALUE_VALID) != 0 && (erh.flags & ER_FLAG_HAVE_EXTERNAL) == 0 {
        let fv = erh.fvalue.as_ref().expect("FVALUE_VALID");
        debug_assert_eq!(allocated_size, fv.tuple.t_len as usize);
        // memcpy(tuphdr, erh->fvalue->t_data, allocated_size): the flat image is
        // the formed tuple's header bytes + data area. Reassemble it.
        let image = heaptuple::heap_tuple_to_disk_image(mcx, fv)?;
        debug_assert_eq!(image.len(), allocated_size);
        dest.copy_from_slice(&image);
        // HeapTupleHeaderSetDatumLength/TypeId/TypMod: stamp the datum header.
        set_datum_header(dest, allocated_size, erh.er_typeid, erh.er_typmod);
        return Ok(());
    }

    // Else allocation should match previous get_flat_size result.
    debug_assert_eq!(allocated_size, erh.flat_size);

    // We'll need the tuple descriptor.
    expanded_record_get_tupdesc(mcx, erh)?;

    // We must ensure that any pad space is zero-filled.
    for b in dest.iter_mut() {
        *b = 0;
    }

    // Fill the data area from dvalues/dnulls via heap_fill_tuple, then assemble
    // the on-disk composite-datum image.
    let td = erh.er_tupdesc.as_ref().expect("tupdesc valid by now");
    let filled = heaptuple::heap_fill_tuple(
        mcx,
        td,
        &erh.dvalues,
        &erh.dnulls,
        erh.data_len,
        erh.hasnull,
    )?;

    // Set up header fields of composite Datum:
    //   HeapTupleHeaderSetDatumLength(allocated_size)
    //   HeapTupleHeaderSetTypeId(er_typeid) / SetTypMod(er_typmod)
    //   ItemPointerSetInvalid(&t_ctid)
    //   HeapTupleHeaderSetNatts(natts); t_hoff = hoff
    // and copy the user-data area at offset hoff.
    set_datum_header(dest, allocated_size, erh.er_typeid, erh.er_typmod);
    set_datum_ctid_invalid(dest);
    set_datum_natts(dest, td.natts as u16);
    set_datum_hoff(dest, erh.hoff as u8);
    set_datum_infomask(dest, filled.infomask);
    if erh.hasnull {
        write_null_bitmap(dest, &filled.bits);
    }
    let hoff = erh.hoff as usize;
    dest[hoff..hoff + filled.data.len()].copy_from_slice(&filled.data);

    Ok(())
}

// ---------------------------------------------------------------------------
// Composite-datum header field setters over the flat on-disk image.
//
// These poke the HeapTupleHeaderData fields of the destination image exactly as
// the C HeapTupleHeaderSet* macros do (varatt.h / htup_details.h offsets).
// ---------------------------------------------------------------------------

// HeapTupleHeaderData layout (htup_details.h):
//   union t_choice {                       // offset 0, 12 bytes
//     HeapTupleFields t_heap;
//     DatumTupleFields t_datum {            // datum_len_ @0, datum_typmod @4, datum_typeid @8 }
//   };
//   ItemPointerData t_ctid;                 // offset 12, 6 bytes
//   uint16 t_infomask2;                     // offset 18
//   uint16 t_infomask;                      // offset 20
//   uint8  t_hoff;                          // offset 22
//   bits8  t_bits[];                        // offset 23
const OFF_DATUM_LEN: usize = 0;
const OFF_DATUM_TYPMOD: usize = 4;
const OFF_DATUM_TYPEID: usize = 8;
const OFF_T_CTID: usize = 12;
const OFF_T_INFOMASK2: usize = 18;
const OFF_T_INFOMASK: usize = 20;
const OFF_T_HOFF: usize = 22;
const OFF_T_BITS: usize = 23;

/// `HeapTupleHeaderSetDatumLength` + `SetTypeId` + `SetTypMod`.
fn set_datum_header(dest: &mut [u8], len: usize, typeid: Oid, typmod: i32) {
    dest[OFF_DATUM_LEN..OFF_DATUM_LEN + 4].copy_from_slice(&(len as i32).to_ne_bytes());
    dest[OFF_DATUM_TYPMOD..OFF_DATUM_TYPMOD + 4].copy_from_slice(&typmod.to_ne_bytes());
    dest[OFF_DATUM_TYPEID..OFF_DATUM_TYPEID + 4].copy_from_slice(&typeid.to_ne_bytes());
}

/// `ItemPointerSetInvalid(&tuphdr->t_ctid)` — block id 0xFFFFFFFF, posid 0.
fn set_datum_ctid_invalid(dest: &mut [u8]) {
    // ip_blkid = {bi_hi, bi_lo} = InvalidBlockNumber (0xFFFFFFFF); ip_posid = 0.
    dest[OFF_T_CTID] = 0xFF;
    dest[OFF_T_CTID + 1] = 0xFF;
    dest[OFF_T_CTID + 2] = 0xFF;
    dest[OFF_T_CTID + 3] = 0xFF;
    dest[OFF_T_CTID + 4] = 0x00;
    dest[OFF_T_CTID + 5] = 0x00;
}

/// `HeapTupleHeaderSetNatts(tuphdr, natts)` — low 11 bits of t_infomask2.
fn set_datum_natts(dest: &mut [u8], natts: u16) {
    let mut w = u16::from_ne_bytes([dest[OFF_T_INFOMASK2], dest[OFF_T_INFOMASK2 + 1]]);
    // HEAP_NATTS_MASK = 0x07FF
    w = (w & !0x07FF) | (natts & 0x07FF);
    dest[OFF_T_INFOMASK2..OFF_T_INFOMASK2 + 2].copy_from_slice(&w.to_ne_bytes());
}

fn set_datum_hoff(dest: &mut [u8], hoff: u8) {
    dest[OFF_T_HOFF] = hoff;
}

fn set_datum_infomask(dest: &mut [u8], infomask: u16) {
    dest[OFF_T_INFOMASK..OFF_T_INFOMASK + 2].copy_from_slice(&infomask.to_ne_bytes());
}

fn write_null_bitmap(dest: &mut [u8], bits: &[u8]) {
    dest[OFF_T_BITS..OFF_T_BITS + bits.len()].copy_from_slice(bits);
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// `make_expanded_record_from_typeid(type_id, typmod, parentcontext)`
/// (expandedrecord.c:68).
pub fn make_expanded_record_from_typeid<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
) -> PgResult<ExpandedRecordHeader<'mcx>> {
    let mut flags = 0;
    let tupdesc: TupleDescData<'mcx>;
    let tupdesc_id: u64;
    let mut tupdesc_refcounted = false;

    if type_id != RECORDOID {
        // Consult the typcache to see if it's a domain over composite, and in
        // any case to get the tupdesc and tupdesc identifier.
        let mut typentry =
            lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO);
        if typentry.typtype == TYPTYPE_DOMAIN {
            flags |= ER_FLAG_IS_DOMAIN;
            typentry = lookup_type_cache(typentry.domain_base_type, TYPECACHE_TUPDESC);
        }
        let Some(td) = typentry.tup_desc else {
            // ereport(ERROR, type %s is not composite)
            return Err(type_is_not_composite(mcx, type_id));
        };
        tupdesc = td;
        tupdesc_id = typentry.tup_desc_identifier;
    } else {
        // For RECORD types, get the tupdesc and identifier from typcache.
        tupdesc = lookup_rowtype_tupdesc(mcx, type_id, typmod)?;
        tupdesc_id = assign_record_type_identifier(type_id, typmod);
        tupdesc_refcounted = true; // lookup_rowtype_tupdesc took a pin we'd release
    }

    let objcxt = mcx.context().new_child("expanded record");
    let mut erh = ExpandedRecordHeader::blank(objcxt, mcx);

    // EOH_init_header + er_magic.
    erh.er_magic = ER_MAGIC;

    // We don't set up dvalues/dnulls contents yet, but mirror nfields.
    erh.nfields = tupdesc.natts;

    // Fill in composite-type identification info.
    erh.er_decltypeid = type_id;
    erh.er_typeid = tupdesc.tdtypeid;
    erh.er_typmod = tupdesc.tdtypmod;
    erh.er_tupdesc_id = tupdesc_id;
    erh.flags = flags;

    // If the typcache tupdesc is refcounted, the C path bumps a refcount (and
    // registers the reset callback to release it). In the owned model we hold
    // our own deep copy in obj_cxt; track whether it was refcounted.
    erh.er_tupdesc_refcounted = tupdesc.tdrefcount >= 0 || tupdesc_refcounted;
    erh.er_tupdesc = Some(tupdesc.clone_in(mcx)?);

    // We don't set DVALUES_VALID or FVALUE_VALID, so the record is empty.
    Ok(erh)
}

/// `make_expanded_record_from_tupdesc(tupdesc, parentcontext)`
/// (expandedrecord.c:204).
pub fn make_expanded_record_from_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<ExpandedRecordHeader<'mcx>> {
    let chosen: TupleDescData<'mcx>;
    let tupdesc_id: u64;
    let mut alloced = false;

    if tupdesc.tdtypeid != RECORDOID {
        // Prefer the typcache's refcounted copy; consult it for the identifier.
        let typentry = lookup_type_cache(tupdesc.tdtypeid, TYPECACHE_TUPDESC);
        let Some(td) = typentry.tup_desc else {
            return Err(type_is_not_composite(mcx, tupdesc.tdtypeid));
        };
        chosen = td.clone_in(mcx)?;
        tupdesc_id = typentry.tup_desc_identifier;
    } else {
        // For RECORD types, get the appropriate unique identifier.
        tupdesc_id = assign_record_type_identifier(tupdesc.tdtypeid, tupdesc.tdtypmod);
        // tdrefcount < 0 (a plain caller tupdesc): C copies it locally.
        if tupdesc.tdrefcount < 0 {
            alloced = true;
        }
        chosen = tupdesc.clone_in(mcx)?;
    }

    let objcxt = mcx.context().new_child("expanded record");
    let mut erh = ExpandedRecordHeader::blank(objcxt, mcx);

    erh.er_magic = ER_MAGIC;
    erh.nfields = chosen.natts;

    erh.er_decltypeid = chosen.tdtypeid;
    erh.er_typeid = chosen.tdtypeid;
    erh.er_typmod = chosen.tdtypmod;
    erh.er_tupdesc_id = tupdesc_id;

    if chosen.tdrefcount >= 0 {
        erh.er_tupdesc_refcounted = true;
    } else if alloced {
        erh.flags |= ER_FLAG_TUPDESC_ALLOCED;
    }
    erh.er_tupdesc = Some(chosen);

    Ok(erh)
}

/// `make_expanded_record_from_exprecord(olderh, parentcontext)`
/// (expandedrecord.c:328).
pub fn make_expanded_record_from_exprecord<'mcx>(
    mcx: Mcx<'mcx>,
    olderh: &mut ExpandedRecordHeader<'mcx>,
) -> PgResult<ExpandedRecordHeader<'mcx>> {
    // tupdesc = expanded_record_get_tupdesc(olderh)
    expanded_record_get_tupdesc(mcx, olderh)?;
    let tupdesc = olderh
        .er_tupdesc
        .as_ref()
        .expect("tupdesc valid")
        .clone_in(mcx)?;

    let objcxt = mcx.context().new_child("expanded record");
    let mut erh = ExpandedRecordHeader::blank(objcxt, mcx);

    erh.er_magic = ER_MAGIC;
    erh.nfields = tupdesc.natts;

    // Fill in composite-type identification info from the source.
    erh.er_decltypeid = olderh.er_decltypeid;
    erh.er_typeid = olderh.er_typeid;
    erh.er_typmod = olderh.er_typmod;
    erh.er_tupdesc_id = olderh.er_tupdesc_id;

    // The only flag bit that transfers over is IS_DOMAIN.
    erh.flags = olderh.flags & ER_FLAG_IS_DOMAIN;

    if tupdesc.tdrefcount >= 0 {
        erh.er_tupdesc_refcounted = true;
    } else if (olderh.flags & ER_FLAG_TUPDESC_ALLOCED) != 0 {
        erh.flags |= ER_FLAG_TUPDESC_ALLOCED;
    }
    erh.er_tupdesc = Some(tupdesc);

    Ok(erh)
}

/// `make_expanded_record_from_datum(recorddatum, parentcontext)`
/// (expandedrecord.c:579). The composite datum crosses as its formed tuple (the
/// owned-model carrier of `DatumGetHeapTupleHeader`'s on-disk image).
pub fn make_expanded_record_from_datum<'mcx>(
    mcx: Mcx<'mcx>,
    record: &FormedTuple<'_>,
) -> PgResult<ExpandedRecordHeader<'mcx>> {
    let objcxt = mcx.context().new_child("expanded record");
    let mut erh = ExpandedRecordHeader::blank(objcxt, mcx);

    erh.er_magic = ER_MAGIC;

    // Detoast and copy source record into private context, as a HeapTuple.
    let newtuple = heaptuple::heap_copytuple(mcx, Some(record))?
        .expect("make_expanded_record_from_datum: source has t_data");
    erh.flags |= ER_FLAG_FVALUE_ALLOCED;

    // Fill in composite-type identification info from the datum header.
    let (typeid, typmod) = datum_header_type(record);
    erh.er_decltypeid = typeid;
    erh.er_typeid = typeid;
    erh.er_typmod = typmod;

    // Remember we have a flat representation.
    erh.fvalue = Some(newtuple);
    erh.flags |= ER_FLAG_FVALUE_VALID;

    // Shouldn't need to set ER_FLAG_HAVE_EXTERNAL (Assert !HasExternal).

    Ok(erh)
}

/// `DatumGetExpandedRecord(d)` (expandedrecord.c:926):
///
/// ```c
/// ExpandedRecordHeader *
/// DatumGetExpandedRecord(Datum d)
/// {
///     if (VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d)))
///     {
///         ExpandedRecordHeader *erh = (ExpandedRecordHeader *) DatumGetEOHP(d);
///         Assert(erh->er_magic == ER_MAGIC);
///         return erh;
///     }
///     d = make_expanded_record_from_datum(d, CurrentMemoryContext);
///     return (ExpandedRecordHeader *) DatumGetEOHP(d);
/// }
/// ```
///
/// Get a writable expanded record from an input argument. If the input is
/// already a read-write expanded pointer, C returns the *existing* in-memory
/// `ExpandedRecordHeader` (chasing `DatumGetEOHP`); otherwise it expands the
/// flat composite datum the hard way.
///
/// The composite datum crosses in the owned model as `record`: the flat
/// `FormedTuple` carrier of the on-disk composite image (`DatumGetHeapTupleHeader`)
/// when the input is a plain/flat value, or `is_rw_expanded = true` when the
/// input is already a read-write expanded pointer. The latter "return the
/// existing header" path needs to reach the live `ExpandedRecordHeader` behind
/// the datum's TOAST pointer — exactly the keystone `DatumGetEOHP` materialize
/// step the owned model cannot perform from bytes alone (the header is a
/// memory-resident value its owner holds, not reconstructible from the pointer
/// payload). So per mirror-PG-and-panic we stop loud at that boundary; the
/// reachable flat-input path is the faithful "expand the hard way" branch.
pub fn datum_get_expanded_record<'mcx>(
    mcx: Mcx<'mcx>,
    record: &FormedTuple<'_>,
    is_rw_expanded: bool,
) -> PgResult<ExpandedRecordHeader<'mcx>> {
    if is_rw_expanded {
        // C: return the existing ExpandedRecordHeader behind the R/W pointer.
        panic!(
            "expandedrecord: DatumGetExpandedRecord: reaching the live \
             ExpandedRecordHeader behind a read-write expanded pointer requires \
             DatumGetEOHP materialization, which the owned mcx model cannot do from \
             a datum-pointer handle (the header is an owned in-memory value)"
        );
    }
    // Else expand the hard way.
    make_expanded_record_from_datum(mcx, record)
}

/// `HeapTupleHeaderGetTypeId`/`GetTypMod` over a formed tuple's datum header.
fn datum_header_type(tup: &FormedTuple<'_>) -> (Oid, i32) {
    let hdr = tup.tuple.t_data.as_ref().expect("datum has t_data");
    (
        types_tuple::heaptuple::HeapTupleHeaderGetTypeId(hdr),
        types_tuple::heaptuple::HeapTupleHeaderGetTypMod(hdr),
    )
}

// ---------------------------------------------------------------------------
// tupdesc fetch
// ---------------------------------------------------------------------------

/// `expanded_record_fetch_tupdesc(erh)` (expandedrecord.c:823) — the out-of-line
/// portion of `expanded_record_get_tupdesc`. Ensures `erh.er_tupdesc` is set,
/// looking it up from the typcache when missing.
pub fn expanded_record_fetch_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
) -> PgResult<()> {
    // Easy if we already have it.
    if erh.er_tupdesc.is_some() {
        return Ok(());
    }

    // Lookup the composite type's tupdesc using the typcache.
    let tupdesc = lookup_rowtype_tupdesc(mcx, erh.er_typeid, erh.er_typmod)?;

    if tupdesc.tdrefcount >= 0 {
        // Refcounted: C registers a reset callback and bumps the refcount, then
        // releases lookup_rowtype_tupdesc's pin. In the owned model we keep a
        // deep copy; record that it was refcounted.
        erh.er_tupdesc_refcounted = true;
    }

    erh.er_tupdesc_id = assign_record_type_identifier(tupdesc.tdtypeid, tupdesc.tdtypmod);
    erh.er_tupdesc = Some(tupdesc);
    Ok(())
}

/// `expanded_record_get_tupdesc(erh)` (expandedrecord.h:217) — inline fast path
/// plus the [`expanded_record_fetch_tupdesc`] fallback.
fn expanded_record_get_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
) -> PgResult<()> {
    if erh.er_tupdesc.is_some() {
        return Ok(());
    }
    expanded_record_fetch_tupdesc(mcx, erh)
}

// ---------------------------------------------------------------------------
// get tuple / deconstruct
// ---------------------------------------------------------------------------

/// `expanded_record_get_tuple(erh)` (expandedrecord.c:883) — return a HeapTuple
/// representing the current value. Returns `None` if the record is empty.
pub fn expanded_record_get_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &ExpandedRecordHeader<'mcx>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // Easy case if we still have original tuple.
    if (erh.flags & ER_FLAG_FVALUE_VALID) != 0 {
        return Ok(Some(erh.fvalue.as_ref().expect("FVALUE_VALID").clone_in(mcx)?));
    }

    // Else just build a tuple from datums.
    if (erh.flags & ER_FLAG_DVALUES_VALID) != 0 {
        let td = erh.er_tupdesc.as_ref().expect("DVALUES_VALID => tupdesc valid");
        return Ok(Some(heaptuple::heap_form_tuple(
            mcx,
            td,
            &erh.dvalues,
            &erh.dnulls,
        )?));
    }

    // Expanded record is empty.
    Ok(None)
}

/// `deconstruct_expanded_record(erh)` (expandedrecord.c:951) — create the
/// Datum/isnull representation if not present. Note: if the object is currently
/// empty, this changes it to represent a row of nulls.
pub fn deconstruct_expanded_record<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
) -> PgResult<()> {
    if (erh.flags & ER_FLAG_DVALUES_VALID) != 0 {
        return Ok(()); // already valid
    }

    // We'll need the tuple descriptor.
    expanded_record_get_tupdesc(mcx, erh)?;
    let nfields = erh.er_tupdesc.as_ref().expect("tupdesc valid").natts;

    if (erh.flags & ER_FLAG_FVALUE_VALID) != 0 {
        // Deconstruct tuple.
        let td = erh.er_tupdesc.as_ref().expect("tupdesc valid");
        let fv = erh.fvalue.as_ref().expect("FVALUE_VALID");
        let cols = heaptuple::heap_deform_tuple(mcx, &fv.tuple, td, &fv.data)?;
        let mut dvalues = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut dnulls = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut owned = vec_with_capacity_in(mcx, nfields as usize)?;
        for (val, isnull) in cols {
            dvalues.push(val);
            dnulls.push(isnull);
            // Fields deconstructed from the flat tuple point INTO it: not
            // separately allocated (C: fstartptr <= ptr < fendptr).
            owned.push(false);
        }
        erh.dvalues = dvalues;
        erh.dnulls = dnulls;
        erh.dvalues_owned = owned;
    } else {
        // If record was empty, instantiate it as a row of nulls.
        let mut dvalues = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut dnulls = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut owned = vec_with_capacity_in(mcx, nfields as usize)?;
        for _ in 0..nfields {
            dvalues.push(TupleValue::ByVal(Datum::null()));
            dnulls.push(true);
            owned.push(false);
        }
        erh.dvalues = dvalues;
        erh.dnulls = dnulls;
        erh.dvalues_owned = owned;
    }

    erh.nfields = nfields;
    erh.flags |= ER_FLAG_DVALUES_VALID;
    Ok(())
}

// ---------------------------------------------------------------------------
// field lookup / fetch
// ---------------------------------------------------------------------------

/// `expanded_record_lookup_field(erh, fieldname, finfo)` (expandedrecord.c:1016).
pub fn expanded_record_lookup_field<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    fieldname: &str,
) -> PgResult<Option<ExpandedRecordFieldInfo>> {
    expanded_record_get_tupdesc(mcx, erh)?;
    let td = erh.er_tupdesc.as_ref().expect("tupdesc valid");

    // First, check user-defined attributes.
    for fno in 0..td.natts {
        let attr = td.attr(fno as usize);
        if !attr.attisdropped && name_eq(&attr.attname, fieldname) {
            return Ok(Some(ExpandedRecordFieldInfo {
                fnumber: attr.attnum as i32,
                ftypeid: attr.atttypid,
                ftypmod: attr.atttypmod,
                fcollation: attr.attcollation,
            }));
        }
    }

    // How about system attributes?
    if let Some(sysattr) = system_attribute_by_name(fieldname) {
        return Ok(Some(ExpandedRecordFieldInfo {
            fnumber: sysattr.attnum as i32,
            ftypeid: sysattr.atttypid,
            ftypmod: sysattr.atttypmod,
            fcollation: sysattr.attcollation,
        }));
    }

    Ok(None)
}

/// `namestrcmp(&attr->attname, fieldname) == 0` (name.c).
fn name_eq(name: &types_tuple::heaptuple::NameData, s: &str) -> bool {
    // name_str() returns the NameData content up to the first NUL.
    name.name_str() == s.as_bytes()
}

/// `expanded_record_fetch_field(erh, fnumber, isnull)` (expandedrecord.c:1062).
/// Returns `(value, isnull)`.
pub fn expanded_record_fetch_field<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    fnumber: i32,
) -> PgResult<(TupleValue<'mcx>, bool)> {
    if fnumber > 0 {
        // Empty record has null fields.
        if erh.is_empty() {
            return Ok((TupleValue::ByVal(Datum::null()), true));
        }
        // Make sure we have deconstructed form.
        deconstruct_expanded_record(mcx, erh)?;
        // Out-of-range field number reads as null.
        if fnumber > erh.nfields {
            return Ok((TupleValue::ByVal(Datum::null()), true));
        }
        let isnull = erh.dnulls[(fnumber - 1) as usize];
        let value = erh.dvalues[(fnumber - 1) as usize].clone_in(mcx)?;
        Ok((value, isnull))
    } else {
        // System columns read as null if we haven't got a flat tuple.
        let Some(fv) = erh.fvalue.as_ref() else {
            return Ok((TupleValue::ByVal(Datum::null()), true));
        };
        // heap_getsysattr doesn't actually use tupdesc.
        heaptuple::heap_getsysattr(mcx, &fv.tuple, fnumber)
    }
}

// ---------------------------------------------------------------------------
// field set
// ---------------------------------------------------------------------------

/// `expanded_record_set_field_internal(erh, fnumber, newValue, isnull,
/// expand_external, check_constraints)` (expandedrecord.c:1111).
pub fn expanded_record_set_field_internal<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    fnumber: i32,
    new_value: TupleValue<'mcx>,
    isnull: bool,
    expand_external: bool,
    check_constraints: bool,
) -> PgResult<()> {
    // Shouldn't assign to a dummy header except for internal field inlining.
    debug_assert!((erh.flags & ER_FLAG_IS_DUMMY) == 0 || !check_constraints);

    // Before performing the assignment, see if result will satisfy domain.
    if (erh.flags & ER_FLAG_IS_DOMAIN) != 0 && check_constraints {
        check_domain_for_new_field(mcx, erh, fnumber, &new_value, isnull)?;
    }

    // If we haven't yet deconstructed the tuple, do that.
    if (erh.flags & ER_FLAG_DVALUES_VALID) == 0 {
        deconstruct_expanded_record(mcx, erh)?;
    }

    // Tuple descriptor must be valid by now.
    debug_assert_eq!(
        erh.nfields,
        erh.er_tupdesc.as_ref().expect("tupdesc valid").natts
    );

    // Caller error if fnumber is a system column or nonexistent column.
    if fnumber <= 0 || fnumber > erh.nfields {
        panic!("cannot assign to field {fnumber} of expanded record");
    }

    let (attbyval, attlen) = {
        let td = erh.er_tupdesc.as_ref().expect("tupdesc valid");
        let attr = td.compact_attr((fnumber - 1) as usize);
        (attr.attbyval, attr.attlen)
    };

    let mut value = new_value;
    let mut this_field_owned = false;
    let mut have_external = false;

    // Copy new field value into record's context, and detoast if needed.
    if !isnull && !attbyval {
        if expand_external {
            let is_ext = attlen == -1
                && matches!(&value, TupleValue::ByRef(b) if varatt_is_external(b));
            if is_ext {
                // C detoasts into the short-lived context to bound any cruft,
                // then datumCopy's into the object context. In the owned model
                // the detoast result is itself the owned, inlined value, so we
                // detoast straight into `mcx` (the object context) — no separate
                // intermediate to reclaim, the same net effect.
                let bytes = detoast_external_attr(mcx, value.as_ref_bytes())?;
                value = TupleValue::ByRef(bytes);
            }
        }

        // Copy value into record's context.
        value = datum_copy(mcx, &value, attbyval, attlen)?;

        // Remember that we have field(s) that may need to be pfree'd.
        this_field_owned = true;

        // Note whether it's an external toasted value (might need inlining).
        if attlen == -1 && matches!(&value, TupleValue::ByRef(b) if varatt_is_external(b)) {
            have_external = true;
        }
    }

    // We're ready to make irreversible changes.

    // Flattened value will no longer represent record accurately.
    erh.flags &= !ER_FLAG_FVALUE_VALID;
    // And we don't know the flattened size either.
    erh.flat_size = 0;

    if this_field_owned {
        erh.flags |= ER_FLAG_DVALUES_ALLOCED;
    }
    if have_external {
        erh.flags |= ER_FLAG_HAVE_EXTERNAL;
    }

    // And finally we can insert the new field. (Replacing the old TupleValue
    // drops its storage — the owned analog of the C pfree of the old value,
    // guarded so we never reclaim a field that pointed into the flat record.)
    let idx = (fnumber - 1) as usize;
    erh.dvalues[idx] = value;
    erh.dnulls[idx] = isnull;
    erh.dvalues_owned[idx] = this_field_owned;

    Ok(())
}

/// `expanded_record_set_fields(erh, newValues, isnulls, expand_external)`
/// (expandedrecord.c:1248). Does not guarantee atomicity on error.
pub fn expanded_record_set_fields<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    new_values: &[TupleValue<'_>],
    isnulls: &[bool],
    expand_external: bool,
) -> PgResult<()> {
    // Shouldn't ever be assigning to a dummy header.
    debug_assert!((erh.flags & ER_FLAG_IS_DUMMY) == 0);

    // If we haven't yet deconstructed the tuple, do that.
    if (erh.flags & ER_FLAG_DVALUES_VALID) == 0 {
        deconstruct_expanded_record(mcx, erh)?;
    }

    // Tuple descriptor must be valid by now.
    debug_assert_eq!(
        erh.nfields,
        erh.er_tupdesc.as_ref().expect("tupdesc valid").natts
    );

    // Flattened value will no longer represent record accurately.
    erh.flags &= !ER_FLAG_FVALUE_VALID;
    erh.flat_size = 0;

    for fnumber in 0..erh.nfields {
        let idx = fnumber as usize;
        let (attbyval, attlen, attisdropped) = {
            let td = erh.er_tupdesc.as_ref().expect("tupdesc valid");
            let attr = td.compact_attr(idx);
            (attr.attbyval, attr.attlen, attr.attisdropped)
        };

        // Ignore dropped columns.
        if attisdropped {
            continue;
        }

        let mut new_value = new_values[idx].clone_in(mcx)?;
        let isnull = isnulls[idx];
        let mut this_field_owned = false;

        if !attbyval && !isnull {
            // Is it an external toasted value?
            let is_ext = attlen == -1
                && matches!(&new_value, TupleValue::ByRef(b) if varatt_is_external(b));
            if is_ext {
                if expand_external {
                    // Detoast as requested while copying the value.
                    let bytes = detoast_external_attr(mcx, new_value.as_ref_bytes())?;
                    new_value = TupleValue::ByRef(bytes);
                } else {
                    // Just copy the value.
                    new_value = datum_copy(mcx, &new_value, false, -1)?;
                    // If it's still external, remember that.
                    if matches!(&new_value, TupleValue::ByRef(b) if varatt_is_external(b)) {
                        erh.flags |= ER_FLAG_HAVE_EXTERNAL;
                    }
                }
            } else {
                // Not an external value, just copy it.
                new_value = datum_copy(mcx, &new_value, false, attlen)?;
            }
            this_field_owned = true;
            erh.flags |= ER_FLAG_DVALUES_ALLOCED;
        }

        // Insert the new field (replacing/freeing the old TupleValue's storage).
        erh.dvalues[idx] = new_value;
        erh.dnulls[idx] = isnull;
        erh.dvalues_owned[idx] = this_field_owned;
    }

    // Domain constraints checked as the final step.
    if (erh.flags & ER_FLAG_IS_DOMAIN) != 0 {
        let ro = expanded_record_get_ro_datum(erh);
        domain_check(ro, false, erh.er_decltypeid)?;
    }

    Ok(())
}

/// `expanded_record_set_tuple(erh, tuple, copy, expand_external)`
/// (expandedrecord.c:439). `tuple == None` sets the record empty.
pub fn expanded_record_set_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    tuple: Option<&FormedTuple<'_>>,
    copy: bool,
    expand_external: bool,
) -> PgResult<()> {
    // Shouldn't ever be assigning to a dummy header.
    debug_assert!((erh.flags & ER_FLAG_IS_DUMMY) == 0);

    // Before performing the assignment, see if result will satisfy domain.
    if (erh.flags & ER_FLAG_IS_DOMAIN) != 0 {
        check_domain_for_new_tuple(mcx, erh, tuple)?;
    }

    // Materialise the incoming tuple (optionally flattening external fields),
    // owned in our context if `copy`.
    let mut expand = expand_external;
    let mut new_owned: Option<FormedTuple<'mcx>> = None;
    let mut new_borrowed_has_external = false;

    if let Some(tuple) = tuple {
        // Get rid of out-of-line field values if asked. C does this in the
        // short-term context to bound toast-fetch cruft; the owned model flattens
        // straight into `mcx` (the flattened tuple is what we ultimately keep).
        let mut flattened: Option<FormedTuple<'mcx>> = None;
        if expand {
            debug_assert!(copy); // caller didn't ask for the unsupported case
            if tuple_has_external(tuple) {
                expanded_record_get_tupdesc(mcx, erh)?;
                let td = erh.er_tupdesc.as_ref().expect("set_tuple: tupdesc available");
                flattened = Some(toast_flatten_tuple(mcx, tuple, td)?);
            } else {
                expand = false; // need not clean up below
            }
        }

        let src: &FormedTuple<'_> = flattened.as_ref().map_or(tuple, |f| f);

        let nt = if copy {
            // Copy tuple into local storage.
            heaptuple::heap_copytuple(mcx, Some(src))?.expect("set_tuple: source has t_data")
        } else {
            // No copy: the caller guarantees it outlives the record. In the
            // owned model we must still hold a value, so clone into our context;
            // this is the faithful materialisation of "fvalue points at it".
            src.clone_in(mcx)?
        };

        new_borrowed_has_external = tuple_has_external(&nt);
        new_owned = Some(nt);
        let _ = expand;
    }

    // Initialize new flags, keeping only non-data status bits.
    let oldflags = erh.flags;
    let mut newflags = oldflags & ER_FLAGS_NON_DATA;

    if let Some(nt) = new_owned {
        if copy {
            newflags |= ER_FLAG_FVALUE_ALLOCED;
        }
        newflags |= ER_FLAG_FVALUE_VALID;
        if new_borrowed_has_external {
            newflags |= ER_FLAG_HAVE_EXTERNAL;
        }
        erh.fvalue = Some(nt);
    } else {
        erh.fvalue = None;
    }

    erh.flags = newflags;

    // Reset flat-size info.
    erh.flat_size = 0;

    // Old field storage / old fvalue are reclaimed by Rust drop when we
    // overwrote erh.dvalues / erh.fvalue above (the C pfree/heap_freetuple of
    // the old field values and old tuple). The DVALUES_VALID bit was cleared by
    // dropping into the ER_FLAGS_NON_DATA mask, so the old dvalues are gone.
    erh.dvalues = PgVec::new_in(mcx);
    erh.dnulls = PgVec::new_in(mcx);
    erh.dvalues_owned = PgVec::new_in(mcx);

    Ok(())
}

// ---------------------------------------------------------------------------
// short-term context, dummy header, domain checks
// ---------------------------------------------------------------------------

/// `get_short_term_cxt(erh)` (expandedrecord.c:1378) — construct (or reset) the
/// short-lived context used for domain checks and detoasting.
fn ensure_short_term_cxt(erh: &mut ExpandedRecordHeader<'_>) {
    match &mut erh.er_short_term_cxt {
        None => {
            erh.er_short_term_cxt = Some(
                erh.obj_cxt
                    .new_child("expanded record short-term context"),
            );
        }
        Some(cxt) => cxt.reset(),
    }
}

/// `build_dummy_expanded_header(main_erh)` (expandedrecord.c:1401) — set up the
/// dummy header used to validate domain constraints without mutating the main
/// record.
fn build_dummy_expanded_header<'mcx>(
    mcx: Mcx<'mcx>,
    main_erh: &mut ExpandedRecordHeader<'mcx>,
) -> PgResult<()> {
    expanded_record_get_tupdesc(mcx, main_erh)?;
    let natts = main_erh.er_tupdesc.as_ref().expect("tupdesc valid").natts;

    // Ensure we have a short-lived context.
    ensure_short_term_cxt(main_erh);

    // Allocate dummy header on first use, or if the field count changed.
    let need_new = match &main_erh.er_dummy_header {
        None => true,
        Some(d) => d.nfields != natts,
    };
    if need_new {
        // C sets the dummy header's eoh_context to the main header's short-term
        // context (so detoasting cruft lands there). In the owned model we give
        // it its own child of that context, which the short-term reset reclaims.
        let dummy_cxt = main_erh
            .er_short_term_cxt
            .as_ref()
            .expect("short-term cxt")
            .new_child("expanded record dummy header");
        let mut dummy = ExpandedRecordHeader::blank(dummy_cxt, mcx);
        dummy.er_magic = ER_MAGIC;
        dummy.nfields = natts;
        main_erh.er_dummy_header = Some(alloc::boxed::Box::new(dummy));
    }

    let typeid = main_erh.er_typeid;
    let typmod = main_erh.er_typmod;
    let tupdesc_id = main_erh.er_tupdesc_id;
    let tupdesc = main_erh.er_tupdesc.as_ref().expect("tupdesc valid").clone_in(mcx)?;
    let fvalue = match &main_erh.fvalue {
        Some(fv) => Some(fv.clone_in(mcx)?),
        None => None,
    };

    let dummy = main_erh.er_dummy_header.as_mut().expect("dummy header");
    // Mark header as dummy; do not transfer IS_DOMAIN (VALUE is base type).
    dummy.flags = ER_FLAG_IS_DUMMY;
    dummy.er_decltypeid = typeid;
    dummy.er_typeid = typeid;
    dummy.er_typmod = typmod;
    dummy.er_tupdesc = Some(tupdesc);
    dummy.er_tupdesc_id = tupdesc_id;
    dummy.flat_size = 0;
    dummy.fvalue = fvalue;
    Ok(())
}

/// `check_domain_for_new_field(erh, fnumber, newValue, isnull)`
/// (expandedrecord.c:1493).
fn check_domain_for_new_field<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    fnumber: i32,
    new_value: &TupleValue<'_>,
    isnull: bool,
) -> PgResult<()> {
    build_dummy_expanded_header(mcx, erh)?;

    // Populate the dummy field array from the current record.
    let empty = erh.is_empty();
    let nfields = erh.er_dummy_header.as_ref().expect("dummy").nfields;
    let have_external = erh.flags & ER_FLAG_HAVE_EXTERNAL;

    if !empty {
        deconstruct_expanded_record(mcx, erh)?;
        let mut dvalues = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut dnulls = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut owned = vec_with_capacity_in(mcx, nfields as usize)?;
        for i in 0..nfields as usize {
            dvalues.push(erh.dvalues[i].clone_in(mcx)?);
            dnulls.push(erh.dnulls[i]);
            owned.push(false);
        }
        let dummy = erh.er_dummy_header.as_mut().expect("dummy");
        dummy.dvalues = dvalues;
        dummy.dnulls = dnulls;
        dummy.dvalues_owned = owned;
        // There might be some external values in there.
        dummy.flags |= have_external;
    } else {
        let mut dvalues = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut dnulls = vec_with_capacity_in(mcx, nfields as usize)?;
        let mut owned = vec_with_capacity_in(mcx, nfields as usize)?;
        for _ in 0..nfields {
            dvalues.push(TupleValue::ByVal(Datum::null()));
            dnulls.push(true);
            owned.push(false);
        }
        let dummy = erh.er_dummy_header.as_mut().expect("dummy");
        dummy.dvalues = dvalues;
        dummy.dnulls = dnulls;
        dummy.dvalues_owned = owned;
    }

    // Either way, we now have valid dvalues.
    {
        let dummy = erh.er_dummy_header.as_mut().expect("dummy");
        dummy.flags |= ER_FLAG_DVALUES_VALID;
        // Caller error if fnumber is system column or nonexistent column.
        if fnumber <= 0 || fnumber > dummy.nfields {
            panic!("cannot assign to field {fnumber} of expanded record");
        }
        // Insert proposed new value into dummy field array.
        dummy.dvalues[(fnumber - 1) as usize] = new_value.clone_in(mcx)?;
        dummy.dnulls[(fnumber - 1) as usize] = isnull;
    }

    // The proposed new value might be external.
    if !isnull {
        let (attbyval, attlen) = {
            let td = erh.er_tupdesc.as_ref().expect("tupdesc valid");
            let attr = td.compact_attr((fnumber - 1) as usize);
            (attr.attbyval, attr.attlen)
        };
        if !attbyval
            && attlen == -1
            && matches!(new_value, TupleValue::ByRef(b) if varatt_is_external(b))
        {
            erh.er_dummy_header.as_mut().expect("dummy").flags |= ER_FLAG_HAVE_EXTERNAL;
        }
    }

    // Apply the check, using the main header's domain cache space.
    let ro = expanded_record_get_ro_datum(erh.er_dummy_header.as_ref().expect("dummy"));
    domain_check(ro, false, erh.er_decltypeid)?;

    // Clean up cruft immediately.
    if let Some(cxt) = erh.er_short_term_cxt.as_mut() {
        cxt.reset();
    }
    Ok(())
}

/// `check_domain_for_new_tuple(erh, tuple)` (expandedrecord.c:1575).
fn check_domain_for_new_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    erh: &mut ExpandedRecordHeader<'mcx>,
    tuple: Option<&FormedTuple<'_>>,
) -> PgResult<()> {
    // If we're being told to set record to empty, just see if NULL is OK.
    let Some(tuple) = tuple else {
        ensure_short_term_cxt(erh);
        domain_check(Datum::null(), true, erh.er_decltypeid)?;
        if let Some(cxt) = erh.er_short_term_cxt.as_mut() {
            cxt.reset();
        }
        return Ok(());
    };

    // Construct dummy header to contain the replacement tuple.
    build_dummy_expanded_header(mcx, erh)?;

    let has_external = tuple_has_external(tuple);
    let fv = tuple.clone_in(mcx)?;
    {
        let dummy = erh.er_dummy_header.as_mut().expect("dummy");
        // Insert tuple, but don't deconstruct its fields for now.
        dummy.fvalue = Some(fv);
        dummy.flags |= ER_FLAG_FVALUE_VALID;
        if has_external {
            dummy.flags |= ER_FLAG_HAVE_EXTERNAL;
        }
    }

    // Apply the check.
    let ro = expanded_record_get_ro_datum(erh.er_dummy_header.as_ref().expect("dummy"));
    domain_check(ro, false, erh.er_decltypeid)?;

    if let Some(cxt) = erh.er_short_term_cxt.as_mut() {
        cxt.reset();
    }
    Ok(())
}

/// `tuple has any out-of-line (external) field values` — HeapTupleHasExternal.
fn tuple_has_external(tup: &FormedTuple<'_>) -> bool {
    tup.tuple
        .t_data
        .as_ref()
        .map(|h| (h.t_infomask & types_tuple::heaptuple::HEAP_HASEXTERNAL) != 0)
        .unwrap_or(false)
}

/// `ExpandedRecordGetRODatum(erh)` (expandedrecord.h:148) — the R/O datum handle
/// to the expanded object. domain_check only reads through it; in the owned
/// model the dummy header IS the value, so we cross the placeholder datum word
/// (the C `EOHPGetRODatum` pointer). The unported `domain_check` owner will be
/// the consumer; until then this is a structural placeholder, never read here.
fn expanded_record_get_ro_datum(_erh: &ExpandedRecordHeader<'_>) -> Datum {
    Datum::null()
}
