use datum::expandeddatum::{
    datum_is_external_expanded_rw, eoh_init_header, eohp_get_ro_datum, eohp_get_rw_datum,
    ExpandedObjectHeader, ExpandedObjectMethods,
};

#[cfg(test)]
mod tests;
use datum::Datum;
use mcx::{vec_from_elem_in, Mcx, MemoryContext, PgVec};
use types_core::catalog::RECORDOID;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_tuple::varatt::{varatt_is_1b, varatt_is_1b_e, varsize_any};
use types_tuple::{
    heap_deform_tuple, heap_getsysattr, HeapTupleData, HeapTupleHeaderData, ItemPointerSetInvalid,
    SizeofHeapTupleHeader, TupleDescData, BITMAPLEN,
};

use heaptuple::{
    heap_compute_data_size, heap_copytuple, heap_fill_tuple, heap_form_tuple, HeapTuple,
};

pub const ER_MAGIC: i32 = 1384727874;

pub const ER_FLAG_FVALUE_VALID: i32 = 0x0001;
pub const ER_FLAG_FVALUE_ALLOCED: i32 = 0x0002;
pub const ER_FLAG_DVALUES_VALID: i32 = 0x0004;
pub const ER_FLAG_DVALUES_ALLOCED: i32 = 0x0008;
pub const ER_FLAG_HAVE_EXTERNAL: i32 = 0x0010;
pub const ER_FLAG_TUPDESC_ALLOCED: i32 = 0x0020;
pub const ER_FLAG_IS_DOMAIN: i32 = 0x0040;
pub const ER_FLAG_IS_DUMMY: i32 = 0x0080;
pub const ER_FLAGS_NON_DATA: i32 =
    ER_FLAG_TUPDESC_ALLOCED | ER_FLAG_IS_DOMAIN | ER_FLAG_IS_DUMMY;

const TYPTYPE_DOMAIN: i8 = b'd' as i8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExpandedRecordFieldInfo {
    pub fnumber: i32,
    pub ftypeid: Oid,
    pub ftypmod: i32,
    pub fcollation: Oid,
}

enum FlatValue {
    Owned(HeapTuple<'static>),
    Borrowed(HeapTupleData<'static>),
}

impl FlatValue {
    #[inline]
    fn tuple(&self) -> &HeapTupleData<'static> {
        match self {
            FlatValue::Owned(t) => t.as_tuple(),
            FlatValue::Borrowed(t) => t,
        }
    }
}

// hdr must sit at offset 0 (the EOH protocol casts it to the full struct).
// C divergences: er_tupdesc is always an owned copy (no refcounted-tupdesc
// pin in this typcache — tdrefcount branch + ER_mc_callback collapse into
// TUPDESC_ALLOCED); er_domaininfo absent (the check engine memoizes
// per-domain); the dummy header owns its tupdesc copy.
#[repr(C)]
pub struct ExpandedRecordHeader {
    pub hdr: ExpandedObjectHeader,
    pub er_magic: i32,
    pub flags: i32,
    pub er_decltypeid: Oid,
    pub er_typeid: Oid,
    pub er_typmod: i32,
    pub er_tupdesc_id: u64,
    pub nfields: i32,
    flat_size: usize,
    data_len: usize,
    hoff: i32,
    hasnull: bool,
    er_tupdesc: Option<TupleDescData<'static>>,
    dvalues: PgVec<'static, Datum>,
    dnulls: PgVec<'static, bool>,
    fvalue: Option<FlatValue>,
    fstartptr: *const u8,
    fendptr: *const u8,
    er_short_term_cxt: *mut MemoryContext,
    er_dummy_header: *mut ExpandedRecordHeader,
    // For the dummy header this aliases the owner's short-term context.
    ctx: *mut MemoryContext,
}

const _: () = assert!(core::mem::offset_of!(ExpandedRecordHeader, hdr) == 0);

static ER_METHODS: ExpandedObjectMethods = ExpandedObjectMethods {
    get_flat_size: er_get_flat_size_method,
    flatten_into: er_flatten_into_method,
};

impl ExpandedRecordHeader {
    fn blank(mcx: Mcx<'static>, ctx: *mut MemoryContext) -> Self {
        ExpandedRecordHeader {
            hdr: ExpandedObjectHeader::empty(),
            er_magic: ER_MAGIC,
            flags: 0,
            er_decltypeid: 0,
            er_typeid: 0,
            er_typmod: -1,
            er_tupdesc_id: typcache::INVALID_TUPLEDESC_IDENTIFIER,
            nfields: 0,
            flat_size: 0,
            data_len: 0,
            hoff: 0,
            hasnull: false,
            er_tupdesc: None,
            dvalues: mcx::vec_new_in(mcx),
            dnulls: mcx::vec_new_in(mcx),
            fvalue: None,
            fstartptr: core::ptr::null(),
            fendptr: core::ptr::null(),
            er_short_term_cxt: core::ptr::null_mut(),
            er_dummy_header: core::ptr::null_mut(),
            ctx,
        }
    }

    // SAFETY(lifetime launder): everything allocated through this handle is
    // stored back into self and freed by free_header before *ctx drops.
    #[inline]
    fn obj_mcx(&self) -> Mcx<'static> {
        debug_assert!(!self.ctx.is_null());
        unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>((*self.ctx).mcx()) }
    }

    fn get_short_term_cxt(&mut self) -> *mut MemoryContext {
        if self.er_short_term_cxt.is_null() {
            // SAFETY: ctx is live for the header's lifetime (free_header order).
            let child = unsafe { (*self.ctx).new_child("expanded record short-term context") };
            self.er_short_term_cxt = Box::into_raw(Box::new(child));
        } else {
            // SAFETY: pointer from Box::into_raw above; no aliases outstanding.
            unsafe { (*self.er_short_term_cxt).reset() };
        }
        self.er_short_term_cxt
    }

    #[inline]
    fn short_mcx(&mut self) -> Mcx<'static> {
        let p = self.get_short_term_cxt();
        // SAFETY(lifetime launder): allocations either drop or are reclaimed
        // by the next reset before the context is freed in free_header.
        unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>((*p).mcx()) }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        (self.flags & (ER_FLAG_DVALUES_VALID | ER_FLAG_FVALUE_VALID)) == 0
    }

    #[inline]
    pub fn is_domain(&self) -> bool {
        (self.flags & ER_FLAG_IS_DOMAIN) != 0
    }

    pub fn dvalues(&self) -> (&[Datum], &[bool]) {
        debug_assert!(self.flags & ER_FLAG_DVALUES_VALID != 0);
        (&self.dvalues, &self.dnulls)
    }
}

/// # Safety
/// `erh` is a live installed expanded record. Datum consumers re-enter the
/// header with &mut through the image, so mint images only from the root
/// pointer, never from a shared borrow.
pub unsafe fn expanded_record_rw_datum(erh: *mut ExpandedRecordHeader) -> Datum {
    eohp_get_rw_datum(&raw const (*erh).hdr)
}

/// # Safety
/// As [`expanded_record_rw_datum`].
pub unsafe fn expanded_record_ro_datum(erh: *mut ExpandedRecordHeader) -> Datum {
    eohp_get_ro_datum(&raw const (*erh).hdr)
}

#[inline]
const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

#[cold]
#[inline(never)]
fn method_failed(surface: &str, e: &PgError) -> ! {
    // The EOH methods table cannot carry PgResult (C ereports through here).
    panic!("expandedrecord {surface}: {}", e.message());
}

#[track_caller]
#[cold]
fn not_composite(type_id: Oid) -> Box<PgError> {
    let name = format_type::format_type_be(type_id).unwrap_or_else(|_| type_id.to_string());
    Box::new(
        PgError::error(format!("type {name} is not composite"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    )
}

#[track_caller]
#[cold]
fn bad_fnumber(fnumber: i32) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cannot assign to field {fnumber} of expanded record"
    )))
}

fn new_blank_header(parentcontext: &MemoryContext) -> *mut ExpandedRecordHeader {
    let ctx = Box::into_raw(Box::new(parentcontext.new_child("expanded record")));
    // SAFETY: fresh context pointer, live until free_header.
    let mcx = unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>((*ctx).mcx()) };
    let p = Box::into_raw(Box::new(ExpandedRecordHeader::blank(mcx, ctx)));
    // SAFETY: p is the header's final heap address; ctx outlives it.
    unsafe { eoh_init_header(&raw mut (*p).hdr, &ER_METHODS, (*p).ctx as *const _) };
    p
}

/// # Safety
/// Sole release of `p` and its contexts; nothing may reference them after.
unsafe fn free_header(p: *mut ExpandedRecordHeader) {
    let b = Box::from_raw(p);
    let ctx = b.ctx;
    let short = b.er_short_term_cxt;
    let dummy = b.er_dummy_header;
    if !dummy.is_null() {
        free_dummy(dummy);
    }
    drop(b);
    if !short.is_null() {
        drop(Box::from_raw(short));
    }
    if !ctx.is_null() {
        drop(Box::from_raw(ctx));
    }
}

/// # Safety
/// Sole release of `dp`; its `ctx` aliases the owner's short-term context and
/// is not freed here.
unsafe fn free_dummy(dp: *mut ExpandedRecordHeader) {
    let d = Box::from_raw(dp);
    let short = d.er_short_term_cxt;
    drop(d);
    if !short.is_null() {
        drop(Box::from_raw(short));
    }
}

fn install(
    p: *mut ExpandedRecordHeader,
    parentcontext: &MemoryContext,
) -> *mut ExpandedRecordHeader {
    // SAFETY: sole release of p (C: objcxt is a child freed by the parent's
    // reset/delete); nothing dereferences it after the parent resets.
    parentcontext.register_reset_callback(move || unsafe { free_header(p) });
    p
}

fn alloc_field_arrays(erh: &mut ExpandedRecordHeader, natts: i32) {
    let mcx = erh.obj_mcx();
    erh.dvalues = vec_from_elem_in(mcx, Datum::null(), natts as usize);
    erh.dnulls = vec_from_elem_in(mcx, false, natts as usize);
    erh.nfields = natts;
}

pub fn make_expanded_record_from_typeid(
    type_id: Oid,
    typmod: i32,
    parentcontext: &MemoryContext,
) -> PgResult<*mut ExpandedRecordHeader> {
    let p = new_blank_header(parentcontext);
    // SAFETY: p fresh and unshared until install.
    let erh = unsafe { &mut *p };
    match init_from_typeid(erh, type_id, typmod) {
        Ok(()) => Ok(install(p, parentcontext)),
        Err(e) => {
            // SAFETY: p never escaped; no callback registered yet.
            unsafe { free_header(p) };
            Err(e)
        }
    }
}

fn init_from_typeid(erh: &mut ExpandedRecordHeader, type_id: Oid, typmod: i32) -> PgResult<()> {
    let mut lookup_type = type_id;
    let mut lookup_typmod = typmod;
    if type_id != RECORDOID {
        let entry = typcache::lookup_type_cache(type_id, typcache::TYPECACHE_DOMAIN_BASE_INFO)?;
        if entry.typtype() == TYPTYPE_DOMAIN {
            erh.flags |= ER_FLAG_IS_DOMAIN;
            lookup_type = entry.domain_base_type();
        }
        lookup_typmod = -1;
    }
    let tupdesc = typcache::lookup_rowtype_tupdesc_copy(erh.obj_mcx(), lookup_type, lookup_typmod)
        .map_err(|e| {
            if e.sqlstate() == ERRCODE_WRONG_OBJECT_TYPE {
                not_composite(type_id)
            } else {
                e
            }
        })?;
    let tupdesc_id = typcache::assign_record_type_identifier(lookup_type, lookup_typmod)?;
    erh.er_decltypeid = type_id;
    erh.er_typeid = tupdesc.tdtypeid;
    erh.er_typmod = tupdesc.tdtypmod;
    erh.er_tupdesc_id = tupdesc_id;
    erh.flags |= ER_FLAG_TUPDESC_ALLOCED;
    alloc_field_arrays(erh, tupdesc.natts);
    erh.er_tupdesc = Some(tupdesc);
    Ok(())
}

pub fn make_expanded_record_from_tupdesc(
    src: &TupleDescData<'_>,
    parentcontext: &MemoryContext,
) -> PgResult<*mut ExpandedRecordHeader> {
    let p = new_blank_header(parentcontext);
    // SAFETY: p fresh and unshared until install.
    let erh = unsafe { &mut *p };
    match init_from_tupdesc(erh, src) {
        Ok(()) => Ok(install(p, parentcontext)),
        Err(e) => {
            // SAFETY: p never escaped; no callback registered yet.
            unsafe { free_header(p) };
            Err(e)
        }
    }
}

fn init_from_tupdesc(erh: &mut ExpandedRecordHeader, src: &TupleDescData<'_>) -> PgResult<()> {
    let mcx = erh.obj_mcx();
    let tupdesc = if src.tdtypeid != RECORDOID {
        // C prefers the typcache's canonical descriptor over the given one.
        typcache::lookup_rowtype_tupdesc_copy(mcx, src.tdtypeid, -1).map_err(|e| {
            if e.sqlstate() == ERRCODE_WRONG_OBJECT_TYPE {
                not_composite(src.tdtypeid)
            } else {
                e
            }
        })?
    } else {
        tupdesc::CreateTupleDescCopy(mcx, src)?
    };
    let tupdesc_id = typcache::assign_record_type_identifier(tupdesc.tdtypeid, tupdesc.tdtypmod)?;
    erh.er_decltypeid = tupdesc.tdtypeid;
    erh.er_typeid = tupdesc.tdtypeid;
    erh.er_typmod = tupdesc.tdtypmod;
    erh.er_tupdesc_id = tupdesc_id;
    erh.flags |= ER_FLAG_TUPDESC_ALLOCED;
    alloc_field_arrays(erh, tupdesc.natts);
    erh.er_tupdesc = Some(tupdesc);
    Ok(())
}

/// # Safety
/// `olderh` references a live expanded record.
pub unsafe fn make_expanded_record_from_exprecord(
    olderh: *mut ExpandedRecordHeader,
    parentcontext: &MemoryContext,
) -> PgResult<*mut ExpandedRecordHeader> {
    let old = &mut *olderh;
    expanded_record_get_tupdesc(old)?;
    let p = new_blank_header(parentcontext);
    let erh = &mut *p;
    let r = (|| -> PgResult<()> {
        let td_copy =
            tupdesc::CreateTupleDescCopy(erh.obj_mcx(), old.er_tupdesc.as_ref().unwrap())?;
        erh.er_decltypeid = old.er_decltypeid;
        erh.er_typeid = old.er_typeid;
        erh.er_typmod = old.er_typmod;
        erh.er_tupdesc_id = old.er_tupdesc_id;
        erh.flags |= (old.flags & ER_FLAG_IS_DOMAIN) | ER_FLAG_TUPDESC_ALLOCED;
        alloc_field_arrays(erh, td_copy.natts);
        erh.er_tupdesc = Some(td_copy);
        Ok(())
    })();
    match r {
        Ok(()) => Ok(install(p, parentcontext)),
        Err(e) => {
            free_header(p);
            Err(e)
        }
    }
}

/// # Safety
/// With `copy == false` the caller guarantees `tuple`'s image outlives the
/// expanded record (C's contract).
pub unsafe fn expanded_record_set_tuple(
    erh: &mut ExpandedRecordHeader,
    tuple: Option<&HeapTupleData<'_>>,
    copy: bool,
    expand_external: bool,
) -> PgResult<()> {
    debug_assert!(erh.flags & ER_FLAG_IS_DUMMY == 0);

    if erh.flags & ER_FLAG_IS_DOMAIN != 0 {
        check_domain_for_new_tuple(erh, tuple)?;
    }

    let mut expand_external = expand_external;
    let mut flat_holder: Option<HeapTuple<'static>> = None;
    if expand_external {
        if let Some(t) = tuple {
            // copy = false with expand_external = true is unsupported (C).
            debug_assert!(copy);
            if t.has_external() {
                let short = erh.short_mcx();
                let td = erh.er_tupdesc.as_ref().expect("expanded record has no tupdesc");
                flat_holder = Some(heaptoast::toast_flatten_tuple(short, t, td)?);
            } else {
                expand_external = false;
            }
        } else {
            expand_external = false;
        }
    }
    let src: Option<&HeapTupleData<'_>> = match &flat_holder {
        Some(f) => Some(f.as_tuple()),
        None => tuple,
    };

    let oldflags = erh.flags;
    let mut newflags = oldflags & ER_FLAGS_NON_DATA;

    let newvalue: Option<FlatValue> = match src {
        Some(t) if copy => {
            let copied = heap_copytuple(erh.obj_mcx(), t)?;
            newflags |= ER_FLAG_FVALUE_ALLOCED;
            Some(FlatValue::Owned(copied))
        }
        Some(t) => Some(FlatValue::Borrowed(HeapTupleData::from_raw_parts(
            t.header_ptr(),
            t.t_len,
            t.t_self,
            t.t_tableOid,
        ))),
        None => None,
    };
    drop(flat_holder);
    if expand_external {
        (*erh.er_short_term_cxt).reset();
    }

    match &newvalue {
        Some(f) => {
            let t = f.tuple();
            erh.fstartptr = t.header_ptr();
            erh.fendptr = t.header_ptr().add(t.t_len as usize);
            newflags |= ER_FLAG_FVALUE_VALID;
            if t.has_external() {
                newflags |= ER_FLAG_HAVE_EXTERNAL;
            }
        }
        None => {
            erh.fstartptr = core::ptr::null();
            erh.fendptr = core::ptr::null();
        }
    }
    // C pfrees old field values/tuple here; mcx has no by-pointer pfree, so
    // that storage lives until the record's context drops (bloat-only).
    erh.fvalue = newvalue;
    erh.flags = newflags;
    erh.flat_size = 0;
    Ok(())
}

pub fn make_expanded_record_from_datum(
    recorddatum: Datum,
    parentcontext: &MemoryContext,
) -> PgResult<Datum> {
    let p = new_blank_header(parentcontext);
    // SAFETY: p fresh and unshared until install.
    let erh = unsafe { &mut *p };
    match init_from_datum(erh, recorddatum) {
        Ok(()) => Ok(unsafe { expanded_record_rw_datum(install(p, parentcontext)) }),
        Err(e) => {
            // SAFETY: p never escaped; no callback registered yet.
            unsafe { free_header(p) };
            Err(e)
        }
    }
}

fn init_from_datum(erh: &mut ExpandedRecordHeader, recorddatum: Datum) -> PgResult<()> {
    let mcx = erh.obj_mcx();
    let src = recorddatum.as_usize() as *const u8;
    // DatumGetHeapTupleHeader: detoast if the composite datum is extended.
    // SAFETY: caller passes a live composite datum.
    let (hdrp, detoasted) = unsafe {
        if varatt_is_1b(src) || varatt_is_1b_e(src) || !types_tuple::varatt::varatt_is_4b_u(src) {
            let image = core::slice::from_raw_parts(src, varsize_any(src));
            let flat = detoast::detoast_attr(mcx, image)?;
            (flat.as_ptr(), Some(flat))
        } else {
            (src, None)
        }
    };
    // SAFETY: hdrp is a plain composite varlena image; its datum length
    // covers the whole tuple body.
    let newtuple = unsafe {
        let hdr: HeapTupleHeaderData = core::ptr::read_unaligned(hdrp.cast());
        let tmp = HeapTupleData::from_raw_parts(
            hdrp,
            hdr.datum_length(),
            types_tuple::ItemPointerData::invalid(),
            types_core::InvalidOid,
        );
        erh.er_decltypeid = hdr.type_id();
        erh.er_typeid = hdr.type_id();
        erh.er_typmod = hdr.typmod();
        heap_copytuple(mcx, &tmp)?
    };
    drop(detoasted);
    erh.flags |= ER_FLAG_FVALUE_ALLOCED | ER_FLAG_FVALUE_VALID;
    erh.fstartptr = newtuple.as_tuple().header_ptr();
    // SAFETY: in-bounds one-past-the-end of the owned image.
    erh.fendptr = unsafe { erh.fstartptr.add(newtuple.as_tuple().t_len as usize) };
    debug_assert!(!newtuple.as_tuple().has_external());
    erh.fvalue = Some(FlatValue::Owned(newtuple));
    Ok(())
}

fn er_get_flat_size(erh: &mut ExpandedRecordHeader) -> PgResult<usize> {
    debug_assert_eq!(erh.er_magic, ER_MAGIC);

    // The flat representation must be a registered, not anonymous, RECORD.
    if erh.er_typeid == RECORDOID && erh.er_typmod < 0 {
        expanded_record_get_tupdesc(erh)?;
        let tdtypmod = {
            let td = erh.er_tupdesc.as_mut().unwrap();
            typcache::assign_record_type_typmod(td)?;
            td.tdtypmod
        };
        erh.er_typmod = tdtypmod;
    }

    if erh.flags & ER_FLAG_FVALUE_VALID != 0 && erh.flags & ER_FLAG_HAVE_EXTERNAL == 0 {
        return Ok(erh.fvalue.as_ref().unwrap().tuple().t_len as usize);
    }
    if erh.flat_size != 0 {
        return Ok(erh.flat_size);
    }

    if erh.flags & ER_FLAG_DVALUES_VALID == 0 {
        deconstruct_expanded_record(erh)?;
    }

    if erh.flags & ER_FLAG_HAVE_EXTERNAL != 0 {
        for i in 0..erh.nfields {
            let td = erh.er_tupdesc.as_ref().unwrap();
            let attr = td.compact_attr(i as usize);
            let value = erh.dvalues[i as usize];
            let external = !erh.dnulls[i as usize]
                && !attr.attbyval
                && attr.attlen == -1
                // SAFETY: non-null by-ref varlena datum.
                && unsafe { varatt_is_1b_e(value.as_usize() as *const u8) };
            if external {
                expanded_record_set_field_internal(erh, i + 1, value, false, true, false)?;
            }
        }
        erh.flags &= !ER_FLAG_HAVE_EXTERNAL;
    }

    let hasnull = erh.dnulls.iter().any(|&n| n);
    let td = erh.er_tupdesc.as_ref().unwrap();
    let mut len = SizeofHeapTupleHeader;
    if hasnull {
        len += BITMAPLEN(td.natts) as usize;
    }
    len = maxalign(len);
    let hoff = len;
    let data_len = heap_compute_data_size(td, &erh.dvalues, &erh.dnulls);
    len += data_len;

    erh.flat_size = len;
    erh.data_len = data_len;
    erh.hoff = hoff as i32;
    erh.hasnull = hasnull;
    Ok(len)
}

unsafe fn er_get_flat_size_method(eohptr: *mut ExpandedObjectHeader) -> usize {
    let erh = &mut *(eohptr as *mut ExpandedRecordHeader);
    assert_eq!(erh.er_magic, ER_MAGIC);
    match er_get_flat_size(erh) {
        Ok(n) => n,
        Err(e) => method_failed("ER_get_flat_size", &e),
    }
}

unsafe fn er_flatten_into_method(
    eohptr: *mut ExpandedObjectHeader,
    result: *mut u8,
    allocated_size: usize,
) {
    let erh = &mut *(eohptr as *mut ExpandedRecordHeader);
    assert_eq!(erh.er_magic, ER_MAGIC);

    if erh.flags & ER_FLAG_FVALUE_VALID != 0 && erh.flags & ER_FLAG_HAVE_EXTERNAL == 0 {
        let t = erh.fvalue.as_ref().unwrap().tuple();
        assert_eq!(allocated_size, t.t_len as usize);
        core::ptr::copy_nonoverlapping(t.header_ptr(), result, allocated_size);
        let tuphdr = &mut *(result as *mut HeapTupleHeaderData);
        // The original flat value might not have datum header fields.
        tuphdr.set_datum_length(allocated_size as u32);
        tuphdr.set_type_id(erh.er_typeid);
        tuphdr.set_typmod(erh.er_typmod);
        return;
    }

    assert_eq!(allocated_size, erh.flat_size);
    if let Err(e) = expanded_record_get_tupdesc(erh) {
        method_failed("ER_flatten_into", &e);
    }
    let td = erh.er_tupdesc.as_ref().unwrap();

    core::ptr::write_bytes(result, 0, allocated_size);
    let tuphdr = &mut *(result as *mut HeapTupleHeaderData);
    tuphdr.set_datum_length(allocated_size as u32);
    tuphdr.set_type_id(erh.er_typeid);
    tuphdr.set_typmod(erh.er_typmod);
    ItemPointerSetInvalid(&mut tuphdr.t_ctid);
    tuphdr.set_natts(td.natts as u16);
    tuphdr.t_hoff = erh.hoff as u8;
    let mut infomask = tuphdr.t_infomask;

    let bits = erh
        .hasnull
        .then(|| result.add(SizeofHeapTupleHeader) as *mut types_tuple::bits8);
    // SAFETY: data area pre-zeroed, sized by the preceding er_get_flat_size.
    heap_fill_tuple(
        td,
        &erh.dvalues,
        &erh.dnulls,
        result.add(erh.hoff as usize),
        erh.data_len,
        &mut infomask,
        bits,
    );
    (*(result as *mut HeapTupleHeaderData)).t_infomask = infomask;
}

pub fn expanded_record_fetch_tupdesc(
    erh: &mut ExpandedRecordHeader,
) -> PgResult<&TupleDescData<'static>> {
    if erh.er_tupdesc.is_some() {
        return Ok(erh.er_tupdesc.as_ref().unwrap());
    }
    let tupdesc =
        typcache::lookup_rowtype_tupdesc_copy(erh.obj_mcx(), erh.er_typeid, erh.er_typmod)?;
    erh.er_tupdesc_id =
        typcache::assign_record_type_identifier(tupdesc.tdtypeid, tupdesc.tdtypmod)?;
    erh.flags |= ER_FLAG_TUPDESC_ALLOCED;
    erh.er_tupdesc = Some(tupdesc);
    Ok(erh.er_tupdesc.as_ref().unwrap())
}

#[inline]
pub fn expanded_record_get_tupdesc(
    erh: &mut ExpandedRecordHeader,
) -> PgResult<&TupleDescData<'static>> {
    if erh.er_tupdesc.is_some() {
        Ok(erh.er_tupdesc.as_ref().unwrap())
    } else {
        expanded_record_fetch_tupdesc(erh)
    }
}

pub enum RecordTuple<'a, 'mcx> {
    Borrowed(&'a HeapTupleData<'static>),
    Formed(HeapTuple<'mcx>),
}

impl RecordTuple<'_, '_> {
    #[inline]
    pub fn tuple(&self) -> &HeapTupleData<'_> {
        match self {
            RecordTuple::Borrowed(t) => t,
            RecordTuple::Formed(t) => t.as_tuple(),
        }
    }
}

/// C returns the stored tuple when valid (caller must not scribble on it),
/// else forms a fresh one in `mcx`; None when the record is empty.
pub fn expanded_record_get_tuple<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    erh: &'a ExpandedRecordHeader,
) -> PgResult<Option<RecordTuple<'a, 'mcx>>> {
    if erh.flags & ER_FLAG_FVALUE_VALID != 0 {
        return Ok(Some(RecordTuple::Borrowed(erh.fvalue.as_ref().unwrap().tuple())));
    }
    if erh.flags & ER_FLAG_DVALUES_VALID != 0 {
        let td = erh.er_tupdesc.as_ref().expect("expanded record has no tupdesc");
        return Ok(Some(RecordTuple::Formed(heap_form_tuple(
            mcx,
            td,
            &erh.dvalues,
            &erh.dnulls,
        )?)));
    }
    Ok(None)
}

/// # Safety
/// `d` is a live composite or expanded-record datum. A read/write input is
/// returned as-is (C's caution about corrupting it applies).
pub unsafe fn datum_get_expanded_record(
    d: Datum,
    parentcontext: &MemoryContext,
) -> PgResult<*mut ExpandedRecordHeader> {
    if datum_is_external_expanded_rw(d) {
        let erh = datum::expandeddatum::datum_get_eohp(d) as *mut ExpandedRecordHeader;
        assert_eq!((*erh).er_magic, ER_MAGIC);
        return Ok(erh);
    }
    let d = make_expanded_record_from_datum(d, parentcontext)?;
    Ok(datum::expandeddatum::datum_get_eohp(d) as *mut ExpandedRecordHeader)
}

pub fn deconstruct_expanded_record(erh: &mut ExpandedRecordHeader) -> PgResult<()> {
    if erh.flags & ER_FLAG_DVALUES_VALID != 0 {
        return Ok(());
    }
    expanded_record_get_tupdesc(erh)?;
    let natts = erh.er_tupdesc.as_ref().unwrap().natts;
    if erh.dvalues.len() != natts as usize {
        alloc_field_arrays(erh, natts);
    }
    if erh.flags & ER_FLAG_FVALUE_VALID != 0 {
        let td = erh.er_tupdesc.as_ref().unwrap();
        heap_deform_tuple(
            erh.fvalue.as_ref().unwrap().tuple(),
            td,
            &mut erh.dvalues,
            &mut erh.dnulls,
        );
    } else {
        // Empty record instantiates as a row of nulls.
        erh.dvalues.fill(Datum::null());
        erh.dnulls.fill(true);
    }
    erh.flags |= ER_FLAG_DVALUES_VALID;
    Ok(())
}

pub fn expanded_record_lookup_field(
    erh: &mut ExpandedRecordHeader,
    fieldname: &str,
) -> PgResult<Option<ExpandedRecordFieldInfo>> {
    let tupdesc = expanded_record_get_tupdesc(erh)?;
    for fno in 0..tupdesc.natts as usize {
        let attr = tupdesc.attr(fno);
        if attr.attname.name_str() == fieldname.as_bytes() && !attr.attisdropped {
            return Ok(Some(ExpandedRecordFieldInfo {
                fnumber: attr.attnum as i32,
                ftypeid: attr.atttypid,
                ftypmod: attr.atttypmod,
                fcollation: attr.attcollation,
            }));
        }
    }
    if let Some(sysattr) = catalog_heap::SystemAttributeByName(fieldname) {
        return Ok(Some(ExpandedRecordFieldInfo {
            fnumber: sysattr.attnum as i32,
            ftypeid: sysattr.atttypid,
            ftypmod: sysattr.atttypmod,
            fcollation: sysattr.attcollation,
        }));
    }
    Ok(None)
}

pub fn expanded_record_fetch_field(
    erh: &mut ExpandedRecordHeader,
    fnumber: i32,
) -> PgResult<(Datum, bool)> {
    if fnumber > 0 {
        if erh.is_empty() {
            return Ok((Datum::null(), true));
        }
        deconstruct_expanded_record(erh)?;
        if fnumber > erh.nfields {
            return Ok((Datum::null(), true));
        }
        Ok((erh.dvalues[(fnumber - 1) as usize], erh.dnulls[(fnumber - 1) as usize]))
    } else {
        // System columns read as null without a flat tuple.
        match &erh.fvalue {
            None => Ok((Datum::null(), true)),
            Some(f) => {
                let mut isnull = false;
                let d = heap_getsysattr(f.tuple(), fnumber, &mut isnull);
                Ok((d, isnull))
            }
        }
    }
}

#[inline]
pub fn expanded_record_get_field(
    erh: &mut ExpandedRecordHeader,
    fnumber: i32,
) -> PgResult<(Datum, bool)> {
    if erh.flags & ER_FLAG_DVALUES_VALID != 0 && fnumber > 0 && fnumber <= erh.nfields {
        Ok((erh.dvalues[(fnumber - 1) as usize], erh.dnulls[(fnumber - 1) as usize]))
    } else {
        expanded_record_fetch_field(erh, fnumber)
    }
}

pub fn expanded_record_set_field_internal(
    erh: &mut ExpandedRecordHeader,
    fnumber: i32,
    mut new_value: Datum,
    isnull: bool,
    mut expand_external: bool,
    check_constraints: bool,
) -> PgResult<()> {
    debug_assert!(erh.flags & ER_FLAG_IS_DUMMY == 0 || !check_constraints);

    if erh.flags & ER_FLAG_IS_DOMAIN != 0 && check_constraints {
        check_domain_for_new_field(erh, fnumber, new_value, isnull)?;
    }

    deconstruct_expanded_record(erh)?;
    let td = erh.er_tupdesc.as_ref().unwrap();
    debug_assert_eq!(erh.nfields, td.natts);
    if fnumber <= 0 || fnumber > erh.nfields {
        return Err(bad_fnumber(fnumber));
    }
    let attr = td.compact_attr((fnumber - 1) as usize);
    let (attbyval, attlen) = (attr.attbyval, attr.attlen);

    if !isnull && !attbyval {
        if expand_external {
            // SAFETY: non-null by-ref datum points at a live varlena.
            let is_ext =
                attlen == -1 && unsafe { varatt_is_1b_e(new_value.as_usize() as *const u8) };
            if is_ext {
                let short = erh.short_mcx();
                // SAFETY: external varlena image is varsize_any bytes.
                let image = unsafe {
                    let p = new_value.as_usize() as *const u8;
                    core::slice::from_raw_parts(p, varsize_any(p))
                };
                let flat = detoast::detoast_external_attr(short, image)?;
                new_value = Datum::from_usize(flat.as_ptr() as usize);
                // Reclaimed by the next short-context reset.
                core::mem::forget(flat);
            } else {
                expand_external = false;
            }
        }

        new_value = adt_scalar::datum_copy(erh.obj_mcx(), new_value, false, attlen)?;
        if expand_external {
            // SAFETY: short context exists (created just above); the forgotten
            // detoast image is the only content being reclaimed.
            unsafe { (*erh.er_short_term_cxt).reset() };
        }
        erh.flags |= ER_FLAG_DVALUES_ALLOCED;

        // datumCopy could itself have made the value non-external.
        if attlen == -1
            // SAFETY: fresh by-ref copy is a live varlena.
            && unsafe { varatt_is_1b_e(new_value.as_usize() as *const u8) }
        {
            erh.flags |= ER_FLAG_HAVE_EXTERNAL;
        }
    }

    erh.flags &= !ER_FLAG_FVALUE_VALID;
    erh.flat_size = 0;
    // C pfrees the replaced separately-palloc'd value here; see set_tuple.
    erh.dvalues[(fnumber - 1) as usize] = new_value;
    erh.dnulls[(fnumber - 1) as usize] = isnull;
    Ok(())
}

#[inline]
pub fn expanded_record_set_field(
    erh: &mut ExpandedRecordHeader,
    fnumber: i32,
    new_value: Datum,
    isnull: bool,
    expand_external: bool,
) -> PgResult<()> {
    expanded_record_set_field_internal(erh, fnumber, new_value, isnull, expand_external, true)
}

/// Not guaranteed atomic on error (C contract); meant for initializing a
/// freshly built record.
pub fn expanded_record_set_fields(
    erh: &mut ExpandedRecordHeader,
    new_values: &[Datum],
    isnulls: &[bool],
    expand_external: bool,
) -> PgResult<()> {
    debug_assert!(erh.flags & ER_FLAG_IS_DUMMY == 0);

    deconstruct_expanded_record(erh)?;
    debug_assert_eq!(erh.nfields, erh.er_tupdesc.as_ref().unwrap().natts);

    erh.flags &= !ER_FLAG_FVALUE_VALID;
    erh.flat_size = 0;

    let mcx = erh.obj_mcx();
    for fnumber in 0..erh.nfields as usize {
        let attr = erh.er_tupdesc.as_ref().unwrap().compact_attr(fnumber);
        let (attbyval, attlen, dropped) = (attr.attbyval, attr.attlen, attr.attisdropped);
        if dropped {
            continue;
        }
        let mut new_value = new_values[fnumber];
        let isnull = isnulls[fnumber];

        if !attbyval && !isnull {
            // SAFETY: non-null by-ref datum points at a live value.
            let is_ext =
                attlen == -1 && unsafe { varatt_is_1b_e(new_value.as_usize() as *const u8) };
            if is_ext {
                if expand_external {
                    // SAFETY: external varlena image is varsize_any bytes.
                    let image = unsafe {
                        let p = new_value.as_usize() as *const u8;
                        core::slice::from_raw_parts(p, varsize_any(p))
                    };
                    let flat = detoast::detoast_external_attr(mcx, image)?;
                    new_value = Datum::from_usize(flat.as_ptr() as usize);
                    // Owned by the record's context until it drops.
                    core::mem::forget(flat);
                } else {
                    new_value = adt_scalar::datum_copy(mcx, new_value, false, -1)?;
                    // SAFETY: fresh by-ref copy is a live varlena.
                    if unsafe { varatt_is_1b_e(new_value.as_usize() as *const u8) } {
                        erh.flags |= ER_FLAG_HAVE_EXTERNAL;
                    }
                }
            } else {
                new_value = adt_scalar::datum_copy(mcx, new_value, false, attlen)?;
            }
            erh.flags |= ER_FLAG_DVALUES_ALLOCED;
            // C pfrees a replaced old value here; see set_tuple.
        }

        erh.dvalues[fnumber] = new_value;
        erh.dnulls[fnumber] = isnull;
    }

    if erh.flags & ER_FLAG_IS_DOMAIN != 0 {
        // C checks the record's own RO datum; self-flatten under the live
        // &mut violates aliasing, so the dummy carries the same values.
        check_domain_on_current_fields(erh)?;
    }
    Ok(())
}

fn check_domain_on_current_fields(erh: &mut ExpandedRecordHeader) -> PgResult<()> {
    let dp = build_dummy_expanded_header(erh)?;
    // SAFETY: dp is a live separate allocation created above.
    let dummy = unsafe { &mut *dp };
    dummy.dvalues.copy_from_slice(&erh.dvalues);
    dummy.dnulls.copy_from_slice(&erh.dnulls);
    dummy.flags |= (erh.flags & ER_FLAG_HAVE_EXTERNAL) | ER_FLAG_DVALUES_VALID;
    // SAFETY: dp live; root-derived image (the check flattens through it).
    let r = adt_domains::domain_check(
        unsafe { expanded_record_ro_datum(dp) },
        false,
        erh.er_decltypeid,
    );
    // SAFETY: short context exists (build_dummy created it).
    unsafe { (*erh.er_short_term_cxt).reset() };
    r
}

fn build_dummy_expanded_header(
    erh: &mut ExpandedRecordHeader,
) -> PgResult<*mut ExpandedRecordHeader> {
    expanded_record_get_tupdesc(erh)?;
    let natts = erh.er_tupdesc.as_ref().unwrap().natts;
    let short = erh.get_short_term_cxt();

    // SAFETY: er_dummy_header is null or a live Box::into_raw pointer.
    let rebuild =
        erh.er_dummy_header.is_null() || unsafe { (*erh.er_dummy_header).nfields } != natts;
    if rebuild {
        let mcx = erh.obj_mcx();
        // C aliases the main tupdesc pointer; owned copy here (rebuilt only
        // when the field count changes, so the reuse cache is preserved).
        let td_copy = tupdesc::CreateTupleDescCopy(mcx, erh.er_tupdesc.as_ref().unwrap())?;
        // Dummy object context IS the short-term context (C, c:1439).
        let mut dummy = Box::new(ExpandedRecordHeader::blank(mcx, short));
        dummy.er_tupdesc = Some(td_copy);
        dummy.dvalues = vec_from_elem_in(mcx, Datum::null(), natts as usize);
        dummy.dnulls = vec_from_elem_in(mcx, false, natts as usize);
        dummy.nfields = natts;
        let dp = Box::into_raw(dummy);
        // SAFETY: dp is the dummy's final heap address.
        unsafe { eoh_init_header(&raw mut (*dp).hdr, &ER_METHODS, short as *const _) };
        if !erh.er_dummy_header.is_null() {
            // SAFETY: replacing the sole owner (C leaks the old one instead).
            unsafe { free_dummy(erh.er_dummy_header) };
        }
        erh.er_dummy_header = dp;
    }

    let dp = erh.er_dummy_header;
    // SAFETY: dp is a live separate allocation; no other &mut outstanding.
    let dummy = unsafe { &mut *dp };
    // The dummy reports the composite base type, never the domain (VALUE in
    // a domain check constraint is of the base type).
    dummy.flags = ER_FLAG_IS_DUMMY;
    dummy.er_decltypeid = erh.er_typeid;
    dummy.er_typeid = erh.er_typeid;
    dummy.er_typmod = erh.er_typmod;
    dummy.er_tupdesc_id = erh.er_tupdesc_id;
    dummy.flat_size = 0;
    dummy.fvalue = erh.fvalue.as_ref().map(|f| {
        let t = f.tuple();
        // SAFETY: view of the main record's flat image, live for the check.
        FlatValue::Borrowed(unsafe {
            HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid)
        })
    });
    dummy.fstartptr = erh.fstartptr;
    dummy.fendptr = erh.fendptr;
    Ok(dp)
}

#[inline(never)]
fn check_domain_for_new_field(
    erh: &mut ExpandedRecordHeader,
    fnumber: i32,
    new_value: Datum,
    isnull: bool,
) -> PgResult<()> {
    let dp = build_dummy_expanded_header(erh)?;

    if !erh.is_empty() {
        deconstruct_expanded_record(erh)?;
    }
    // SAFETY: dp is a live separate allocation created above.
    let dummy = unsafe { &mut *dp };
    if !erh.is_empty() {
        dummy.dvalues.copy_from_slice(&erh.dvalues);
        dummy.dnulls.copy_from_slice(&erh.dnulls);
        dummy.flags |= erh.flags & ER_FLAG_HAVE_EXTERNAL;
    } else {
        dummy.dvalues.fill(Datum::null());
        dummy.dnulls.fill(true);
    }
    dummy.flags |= ER_FLAG_DVALUES_VALID;

    if fnumber <= 0 || fnumber > dummy.nfields {
        return Err(bad_fnumber(fnumber));
    }
    dummy.dvalues[(fnumber - 1) as usize] = new_value;
    dummy.dnulls[(fnumber - 1) as usize] = isnull;

    if !isnull {
        let attr = erh.er_tupdesc.as_ref().unwrap().compact_attr((fnumber - 1) as usize);
        if !attr.attbyval
            && attr.attlen == -1
            // SAFETY: non-null by-ref datum points at a live varlena.
            && unsafe { varatt_is_1b_e(new_value.as_usize() as *const u8) }
        {
            dummy.flags |= ER_FLAG_HAVE_EXTERNAL;
        }
    }

    // SAFETY: dp live; root-derived image (the check flattens through it).
    let r = adt_domains::domain_check(
        unsafe { expanded_record_ro_datum(dp) },
        false,
        erh.er_decltypeid,
    );
    // SAFETY: short context exists (build_dummy created it).
    unsafe { (*erh.er_short_term_cxt).reset() };
    r
}

#[inline(never)]
fn check_domain_for_new_tuple(
    erh: &mut ExpandedRecordHeader,
    tuple: Option<&HeapTupleData<'_>>,
) -> PgResult<()> {
    let Some(tuple) = tuple else {
        erh.get_short_term_cxt();
        let r = adt_domains::domain_check(Datum::null(), true, erh.er_decltypeid);
        // SAFETY: created just above.
        unsafe { (*erh.er_short_term_cxt).reset() };
        return r;
    };

    let dp = build_dummy_expanded_header(erh)?;
    // SAFETY: dp is a live separate allocation created above.
    let dummy = unsafe { &mut *dp };
    // SAFETY: view of the caller's tuple image, live for the check.
    dummy.fvalue = Some(FlatValue::Borrowed(unsafe {
        HeapTupleData::from_raw_parts(
            tuple.header_ptr(),
            tuple.t_len,
            tuple.t_self,
            tuple.t_tableOid,
        )
    }));
    dummy.fstartptr = tuple.header_ptr();
    // SAFETY: one-past-the-end of the caller's image.
    dummy.fendptr = unsafe { tuple.header_ptr().add(tuple.t_len as usize) };
    dummy.flags |= ER_FLAG_FVALUE_VALID;
    if tuple.has_external() {
        dummy.flags |= ER_FLAG_HAVE_EXTERNAL;
    }

    // SAFETY: dp live; root-derived image (the check flattens through it).
    let r = adt_domains::domain_check(
        unsafe { expanded_record_ro_datum(dp) },
        false,
        erh.er_decltypeid,
    );
    // SAFETY: short context exists (build_dummy created it).
    unsafe { (*erh.er_short_term_cxt).reset() };
    r
}
