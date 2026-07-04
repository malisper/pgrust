use ::datum::expandeddatum::{
    datum_get_eohp, datum_is_external_expanded, datum_is_external_expanded_rw, eoh_flatten_into,
    eoh_get_flat_size, eoh_init_header, eohp_get_rw_datum, ExpandedObjectHeader,
    ExpandedObjectMethods,
};
use ::datum::Datum;
use ::mcx::{slice_in, vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;

use crate::construct::{copy_array_els, deconstruct_array};
use crate::foundation::{
    arr_data_offset, arr_elemtype, arr_hasnull, arr_overhead_nonulls, arr_overhead_withnulls,
    arr_size, att_align_nominal, read_dims_lbounds, varsize_any, MAXDIM, MAX_ALLOC_SIZE,
};

pub const EA_MAGIC: i32 = 689375833;

pub struct ArrayMetaState {
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
}

impl ArrayMetaState {
    pub const fn invalid() -> Self {
        ArrayMetaState { element_type: 0, typlen: 0, typbyval: false, typalign: 0 }
    }
}

// hdr must sit at offset 0: the expanded-datum protocol hands back
// *mut ExpandedObjectHeader and EA methods cast it to the full struct.
// ctx is declared last so the PgVecs (allocated in ctx) drop first.
#[repr(C)]
pub struct ExpandedArrayHeader {
    pub hdr: ExpandedObjectHeader,
    pub ea_magic: i32,
    pub ndims: i32,
    pub dims: [i32; MAXDIM],
    pub lbound: [i32; MAXDIM],
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub dvalueslen: i32,
    pub nelems: i32,
    pub flat_size: usize,
    dvalues: Option<PgVec<'static, Datum>>,
    dnulls: Option<PgVec<'static, bool>>,
    fvalue: Option<PgVec<'static, u8>>,
    ctx: MemoryContext,
}

const _: () = assert!(core::mem::offset_of!(ExpandedArrayHeader, hdr) == 0);

static EA_METHODS: ExpandedObjectMethods = ExpandedObjectMethods {
    get_flat_size: ea_get_flat_size,
    flatten_into: ea_flatten_into,
};

impl ExpandedArrayHeader {
    fn empty(ctx: MemoryContext) -> Self {
        ExpandedArrayHeader {
            hdr: ExpandedObjectHeader::empty(),
            ea_magic: EA_MAGIC,
            ndims: 0,
            dims: [0; MAXDIM],
            lbound: [0; MAXDIM],
            element_type: 0,
            typlen: 0,
            typbyval: false,
            typalign: 0,
            dvalueslen: 0,
            nelems: 0,
            flat_size: 0,
            dvalues: None,
            dnulls: None,
            fvalue: None,
            ctx,
        }
    }

    // SAFETY(lifetime launder): allocations handed back into self's vec
    // fields die before ctx (field order above); nothing else escapes.
    fn obj_mcx(&self) -> Mcx<'static> {
        unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(self.ctx.mcx()) }
    }

    pub fn fvalue(&self) -> Option<&[u8]> {
        self.fvalue.as_deref()
    }

    pub fn dvalues(&self) -> Option<(&[Datum], Option<&[bool]>)> {
        self.dvalues.as_ref().map(|v| (v.as_slice(), self.dnulls.as_deref()))
    }
}

pub fn expand_array(
    arraydatum: Datum,
    parentcontext: &MemoryContext,
    mut metacache: Option<&mut ArrayMetaState>,
) -> PgResult<Datum> {
    let ctx = parentcontext.new_child("expanded array");
    let mut eah = Box::new(ExpandedArrayHeader::empty(ctx));

    // SAFETY: arraydatum is by-ref; expanded images embed a live header.
    let src_expanded = unsafe { datum_is_external_expanded(arraydatum) };
    if src_expanded {
        let old = unsafe { &*(datum_get_eohp(arraydatum) as *const ExpandedArrayHeader) };
        assert_eq!(old.ea_magic, EA_MAGIC);
        let mut fakecache = ArrayMetaState::invalid();
        let cache: &mut ArrayMetaState = match metacache.as_deref_mut() {
            Some(m) => m,
            None => &mut fakecache,
        };
        cache.element_type = old.element_type;
        cache.typlen = old.typlen;
        cache.typbyval = old.typbyval;
        cache.typalign = old.typalign;

        if old.typbyval && old.dvalues.is_some() {
            copy_byval_expanded_array(&mut eah, old)?;
            return Ok(install(eah, parentcontext));
        }
        let flat = flatten_source(&mut eah, arraydatum)?;
        fill_from_flat(&mut eah, flat, Some(cache))?;
        return Ok(install(eah, parentcontext));
    }

    let flat = copy_flat_source(&mut eah, arraydatum)?;
    fill_from_flat(&mut eah, flat, metacache.as_deref_mut())?;
    Ok(install(eah, parentcontext))
}

fn install(eah: Box<ExpandedArrayHeader>, parentcontext: &MemoryContext) -> Datum {
    let p = Box::into_raw(eah);
    // SAFETY: p is the header's final address (Box); both TOAST images embed
    // it. The parent reset callback is the sole owner-release (C: objcxt is a
    // child of parentcontext, freed by the parent's reset/delete).
    unsafe {
        eoh_init_header(&raw mut (*p).hdr, &EA_METHODS, &raw const (*p).ctx);
        // SAFETY: sole release of p; nothing dereferences it after the parent resets.
        parentcontext.register_reset_callback(move || unsafe { drop(Box::from_raw(p)) });
        eohp_get_rw_datum(&raw const (*p).hdr)
    }
}

fn copy_byval_expanded_array(
    eah: &mut ExpandedArrayHeader,
    oldeah: &ExpandedArrayHeader,
) -> PgResult<()> {
    let mcx = eah.obj_mcx();
    eah.ndims = oldeah.ndims;
    eah.dims = oldeah.dims;
    eah.lbound = oldeah.lbound;
    eah.element_type = oldeah.element_type;
    eah.typlen = oldeah.typlen;
    eah.typbyval = oldeah.typbyval;
    eah.typalign = oldeah.typalign;
    eah.dvalues = Some(slice_in(mcx, oldeah.dvalues.as_ref().unwrap())?);
    eah.dnulls = match &oldeah.dnulls {
        Some(old) => Some(slice_in(mcx, old)?),
        None => None,
    };
    eah.dvalueslen = oldeah.dvalueslen;
    eah.nelems = oldeah.nelems;
    eah.flat_size = oldeah.flat_size;
    eah.fvalue = None;
    Ok(())
}

fn flatten_source(
    eah: &mut ExpandedArrayHeader,
    arraydatum: Datum,
) -> PgResult<PgVec<'static, u8>> {
    let mcx = eah.obj_mcx();
    // SAFETY: source is a live expanded object (caller checked the image tag).
    unsafe {
        let old = datum_get_eohp(arraydatum);
        let n = eoh_get_flat_size(old);
        let mut buf: PgVec<'static, u8> = vec_with_capacity_in(mcx, n)?;
        eoh_flatten_into(old, buf.as_mut_ptr(), n);
        buf.set_len(n);
        Ok(buf)
    }
}

fn copy_flat_source(
    eah: &mut ExpandedArrayHeader,
    arraydatum: Datum,
) -> PgResult<PgVec<'static, u8>> {
    let mcx = eah.obj_mcx();
    let p = arraydatum.as_usize() as *const u8;
    // SAFETY: by-ref array datum points at a varlena image.
    unsafe {
        if (*p & 0x03) != 0 {
            let image = core::slice::from_raw_parts(p, varsize_any(p));
            detoast_seams::detoast_attr::call(mcx, image)
        } else {
            let n = arr_size(core::slice::from_raw_parts(p, 4));
            slice_in(mcx, core::slice::from_raw_parts(p, n))
        }
    }
}

fn fill_from_flat(
    eah: &mut ExpandedArrayHeader,
    flat: PgVec<'static, u8>,
    metacache: Option<&mut ArrayMetaState>,
) -> PgResult<()> {
    let (ndims, dims, lbound) = read_dims_lbounds(&flat);
    eah.ndims = ndims;
    eah.dims = dims;
    eah.lbound = lbound;
    eah.element_type = arr_elemtype(&flat);
    match metacache {
        Some(m) if m.element_type == eah.element_type => {
            eah.typlen = m.typlen;
            eah.typbyval = m.typbyval;
            eah.typalign = m.typalign;
        }
        other => {
            let (typlen, typbyval, typalign) =
                ::lsyscache::get_typlenbyvalalign(eah.element_type)?;
            eah.typlen = typlen;
            eah.typbyval = typbyval;
            eah.typalign = typalign as u8;
            if let Some(m) = other {
                m.element_type = eah.element_type;
                m.typlen = eah.typlen;
                m.typbyval = eah.typbyval;
                m.typalign = eah.typalign;
            }
        }
    }
    eah.dvalues = None;
    eah.dnulls = None;
    eah.dvalueslen = 0;
    eah.nelems = 0;
    eah.flat_size = 0;
    eah.fvalue = Some(flat);
    Ok(())
}

fn att_addlength_datum(cur: usize, typlen: i16, typbyval: bool, value: Datum) -> usize {
    if typlen > 0 {
        cur + typlen as usize
    } else if typlen == -1 {
        debug_assert!(!typbyval);
        // SAFETY: by-ref varlena datum points at a live image.
        cur + unsafe { varsize_any(value.as_usize() as *const u8) }
    } else {
        debug_assert_eq!(typlen, -2);
        // SAFETY: cstring datum points at a live NUL-terminated string.
        cur + unsafe {
            let mut p = value.as_usize() as *const u8;
            let mut n = 1usize;
            while *p != 0 {
                n += 1;
                p = p.add(1);
            }
            n
        }
    }
}

unsafe fn ea_get_flat_size(eohptr: *mut ExpandedObjectHeader) -> usize {
    let eah = &mut *(eohptr as *mut ExpandedArrayHeader);
    assert_eq!(eah.ea_magic, EA_MAGIC);
    if let Some(f) = &eah.fvalue {
        return arr_size(f);
    }
    if eah.flat_size != 0 {
        return eah.flat_size;
    }
    let nelems = eah.nelems as usize;
    let dvalues = eah.dvalues.as_ref().expect("expanded array has neither fvalue nor dvalues");
    let dnulls = eah.dnulls.as_deref();
    let mut nbytes = 0usize;
    for i in 0..nelems {
        if dnulls.is_some_and(|n| n[i]) {
            continue;
        }
        nbytes = att_addlength_datum(nbytes, eah.typlen, eah.typbyval, dvalues[i]);
        nbytes = att_align_nominal(nbytes, eah.typalign);
        if nbytes > MAX_ALLOC_SIZE {
            // The methods table cannot carry PgResult (C ereports here);
            // reachable only for >1GB element payloads.
            panic!("array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})");
        }
    }
    nbytes += if dnulls.is_some() {
        arr_overhead_withnulls(eah.ndims, eah.nelems)
    } else {
        arr_overhead_nonulls(eah.ndims)
    };
    eah.flat_size = nbytes;
    nbytes
}

unsafe fn ea_flatten_into(
    eohptr: *mut ExpandedObjectHeader,
    result: *mut u8,
    allocated_size: usize,
) {
    let eah = &*(eohptr as *const ExpandedArrayHeader);
    assert_eq!(eah.ea_magic, EA_MAGIC);
    if let Some(f) = &eah.fvalue {
        assert_eq!(allocated_size, arr_size(f));
        core::ptr::copy_nonoverlapping(f.as_ptr(), result, allocated_size);
        return;
    }
    assert_eq!(allocated_size, eah.flat_size);
    let nelems = eah.nelems;
    let ndims = eah.ndims;
    let dvalues = eah.dvalues.as_ref().unwrap();
    let dnulls = eah.dnulls.as_deref();
    let dataoffset: i32 = if dnulls.is_some() {
        arr_overhead_withnulls(ndims, nelems) as i32
    } else {
        0
    };
    // Pad space must be zero-filled (C memsets the whole result).
    core::ptr::write_bytes(result, 0, allocated_size);
    let image = core::slice::from_raw_parts_mut(result, allocated_size);
    image[0..4].copy_from_slice(&::datum::varlena::set_varsize_4b(allocated_size));
    image[4..8].copy_from_slice(&ndims.to_ne_bytes());
    image[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
    image[12..16].copy_from_slice(&eah.element_type.to_ne_bytes());
    let mut off = 16usize;
    for i in 0..ndims as usize {
        image[off..off + 4].copy_from_slice(&eah.dims[i].to_ne_bytes());
        off += 4;
    }
    for i in 0..ndims as usize {
        image[off..off + 4].copy_from_slice(&eah.lbound[i].to_ne_bytes());
        off += 4;
    }
    copy_array_els(
        image,
        dvalues,
        dnulls,
        nelems as usize,
        eah.typlen as i32,
        eah.typbyval,
        eah.typalign,
    );
}

/// C DatumGetExpandedArray; `parentcontext` stands in for C's
/// CurrentMemoryContext (no ambient context here).
///
/// # Safety
/// `d` is a live array datum; the returned pointer aliases the object the
/// caller's datum references and follows C's caution about corrupting it.
pub unsafe fn datum_get_expanded_array(
    d: Datum,
    parentcontext: &MemoryContext,
) -> PgResult<*mut ExpandedArrayHeader> {
    datum_get_expanded_array_x(d, parentcontext, None)
}

/// C DatumGetExpandedArrayX.
///
/// # Safety
/// Same contract as [`datum_get_expanded_array`].
pub unsafe fn datum_get_expanded_array_x(
    d: Datum,
    parentcontext: &MemoryContext,
    mut metacache: Option<&mut ArrayMetaState>,
) -> PgResult<*mut ExpandedArrayHeader> {
    if datum_is_external_expanded_rw(d) {
        let eah = datum_get_eohp(d) as *mut ExpandedArrayHeader;
        assert_eq!((*eah).ea_magic, EA_MAGIC);
        if let Some(m) = metacache.as_deref_mut() {
            m.element_type = (*eah).element_type;
            m.typlen = (*eah).typlen;
            m.typbyval = (*eah).typbyval;
            m.typalign = (*eah).typalign;
        }
        return Ok(eah);
    }
    let d = expand_array(d, parentcontext, metacache)?;
    Ok(datum_get_eohp(d) as *mut ExpandedArrayHeader)
}

pub fn deconstruct_expanded_array(eah: &mut ExpandedArrayHeader) -> PgResult<()> {
    if eah.dvalues.is_some() {
        return Ok(());
    }
    let mcx = eah.obj_mcx();
    let fvalue = eah.fvalue.as_ref().expect("expanded array has neither fvalue nor dvalues");
    // SAFETY(lifetime launder): dvalues' by-ref element datums point into
    // fvalue, which this object owns and never frees before them.
    let flat: &'static [u8] = unsafe { core::mem::transmute(fvalue.as_slice()) };
    let hasnull = arr_hasnull(flat);
    let (dvalues, dnulls) =
        deconstruct_array(mcx, flat, eah.typlen as i32, eah.typbyval, eah.typalign, true)?;
    let nelems = dvalues.len() as i32;
    eah.dvalues = Some(dvalues);
    eah.dnulls = if hasnull { Some(dnulls) } else { None };
    eah.dvalueslen = nelems;
    eah.nelems = nelems;
    Ok(())
}

pub fn expanded_array_data_bounds(eah: &ExpandedArrayHeader) -> Option<(usize, usize)> {
    eah.fvalue.as_ref().map(|f| (arr_data_offset(f), arr_size(f)))
}
