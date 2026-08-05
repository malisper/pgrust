//! Array-op step state + eval bodies: EEOP_ARRAYEXPR multidims,
//! EEOP_SBSREF_* (execExprInterp.c + arraysubs.c execution halves).

use alloc::boxed::Box;
use alloc::format;
use core::ptr::NonNull;

use ::datum::{Datum, NullableDatum};
use ::mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

pub const MAXDIM: usize = 6;

// Lifetime-erased armed result context (fcinfo.result_mcx precedent).
pub type ResMcx = Option<NonNull<MemoryContext>>;

#[inline]
pub(crate) fn res_mcx<'a>(slot: &ResMcx) -> Mcx<'a> {
    match slot {
        // SAFETY: arm_result_mcx's contract — the context outlives evaluation.
        Some(p) => unsafe { p.as_ref() }.mcx(),
        None => res_mcx_unarmed(),
    }
}

#[cold]
#[inline(never)]
fn res_mcx_unarmed() -> ! {
    panic!("execexpr array op: result mcx not armed (allocating array step in an unarmed context)")
}

// DatumGetArrayTypeP: borrow the flat image in place when it already has an
// uncompressed 4-byte header, else detoast a copy into `slot`'s context.
fn datum_array_image<'a>(d: Datum, slot: &ResMcx) -> PgResult<&'a [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null array datum addresses a live varlena.
    unsafe {
        if ::types_tuple::varatt::varatt_is_4b_u(p) {
            let total = ::types_tuple::varatt::varsize_any(p);
            Ok(core::slice::from_raw_parts(p, total))
        } else {
            let mcx = res_mcx(slot);
            let total = ::types_tuple::varatt::varsize_any(p);
            let raw = core::slice::from_raw_parts(p, total);
            let img = ::detoast_seams::detoast_attr::call(mcx, raw)?;
            Ok(&*(img.leak() as *const [u8]))
        }
    }
}

pub struct ArrayExprState {
    pub elemtype: Oid,
    pub elemlength: i32,
    pub elembyval: bool,
    pub elemalign: u8,
    pub multidims: bool,
    pub nelems: u32,
    // nelems eval targets + split scratch for construct_md_array.
    pub elemvalues: NonNull<NullableDatum>,
    pub scratch_values: NonNull<Datum>,
    pub scratch_nulls: NonNull<bool>,
    pub resmcx: ResMcx,
}

#[track_caller]
#[cold]
fn array_merge_error(want: Oid, got: Oid) -> Box<PgError> {
    let w = format_type::format_type_be(want).unwrap_or_else(|_| want.to_string());
    let g = format_type::format_type_be(got).unwrap_or_else(|_| got.to_string());
    Box::new(
        PgError::error("cannot merge incompatible arrays".to_string())
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
            .with_detail(format!(
                "Array with element type {g} cannot be included in ARRAY construct \
                 with element type {w}."
            )),
    )
}

#[track_caller]
#[cold]
fn dims_mismatch_error() -> Box<PgError> {
    Box::new(
        PgError::error(
            "multidimensional arrays must have array expressions with matching dimensions"
                .to_string(),
        )
        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

pub fn eval_array_expr(st: &mut ArrayExprState) -> PgResult<NullableDatum> {
    let mcx = res_mcx(&st.resmcx);
    let nelems = st.nelems as usize;
    // SAFETY: compile allocated nelems slots in each array; single-threaded.
    let elems = unsafe { core::slice::from_raw_parts(st.elemvalues.as_ptr(), nelems) };

    if !st.multidims {
        let values = unsafe { core::slice::from_raw_parts_mut(st.scratch_values.as_ptr(), nelems) };
        let nulls = unsafe { core::slice::from_raw_parts_mut(st.scratch_nulls.as_ptr(), nelems) };
        for i in 0..nelems {
            values[i] = elems[i].value;
            nulls[i] = elems[i].isnull;
        }
        let dims = [nelems as i32];
        let lbs = [1i32];
        let img = arrayfuncs::construct_md_array(
            mcx,
            values,
            Some(nulls),
            1,
            &dims,
            &lbs,
            st.elemtype,
            st.elemlength,
            st.elembyval,
            st.elemalign,
        )?;
        return Ok(NullableDatum {
            value: Datum::from_usize(img.leak().as_ptr() as usize),
            isnull: false,
        });
    }

    // Nested sub-arrays: concatenate into an (n+1)-D array (C's multidims arm).
    let mut ndims = 0i32;
    let mut elem_ndims = 0i32;
    let mut elem_dims = [0i32; MAXDIM];
    let mut elem_lbs = [0i32; MAXDIM];
    let mut firstone = true;
    let mut havenulls = false;
    let mut haveempty = false;
    let mut nbytes = 0usize;
    let mut outer_nelems = 0usize;

    struct Sub<'a> {
        img: &'a [u8],
        nitems: i32,
    }
    let mut subs: PgVec<'_, Sub<'_>> = vec_with_capacity_in(mcx, nelems)?;

    for e in elems.iter() {
        if e.isnull {
            haveempty = true;
            continue;
        }
        let arr = datum_array_image(e.value, &st.resmcx)?;
        if st.elemtype != arrayfuncs::arr_elemtype(arr) {
            return Err(array_merge_error(st.elemtype, arrayfuncs::arr_elemtype(arr)));
        }
        let this_ndims = arrayfuncs::arr_ndim(arr);
        if this_ndims <= 0 {
            haveempty = true;
            continue;
        }
        if firstone {
            elem_ndims = this_ndims;
            ndims = elem_ndims + 1;
            if ndims <= 0 || ndims as usize > MAXDIM {
                return Err(Box::new(
                    PgError::error(format!(
                        "number of array dimensions ({ndims}) exceeds the maximum allowed \
                         ({MAXDIM})"
                    ))
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                ));
            }
            for i in 0..elem_ndims as usize {
                elem_dims[i] = arrayfuncs::arr_dim(arr, i);
                elem_lbs[i] = arrayfuncs::arr_lbound(arr, i);
            }
            firstone = false;
        } else {
            let mut same = elem_ndims == this_ndims;
            if same {
                for i in 0..elem_ndims as usize {
                    if elem_dims[i] != arrayfuncs::arr_dim(arr, i)
                        || elem_lbs[i] != arrayfuncs::arr_lbound(arr, i)
                    {
                        same = false;
                        break;
                    }
                }
            }
            if !same {
                return Err(dims_mismatch_error());
            }
        }
        let mut this_dims = [0i32; MAXDIM];
        for i in 0..this_ndims as usize {
            this_dims[i] = arrayfuncs::arr_dim(arr, i);
        }
        let nitems = arrayutils::array_get_n_items(this_ndims, &this_dims)?;
        nbytes += arrayfuncs::arr_size(arr) - arrayfuncs::arr_data_offset(arr);
        if nbytes > arrayfuncs::foundation::MAX_ALLOC_SIZE {
            return Err(Box::new(
                PgError::error(format!(
                    "array size exceeds the maximum allowed ({})",
                    arrayfuncs::foundation::MAX_ALLOC_SIZE
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ));
        }
        havenulls |= arrayfuncs::arr_hasnull(arr);
        subs.push(Sub { img: arr, nitems });
        outer_nelems += 1;
    }

    if haveempty {
        if ndims == 0 {
            let img = arrayfuncs::construct_empty_array(mcx, st.elemtype)?;
            return Ok(NullableDatum {
                value: Datum::from_usize(img.leak().as_ptr() as usize),
                isnull: false,
            });
        }
        return Err(dims_mismatch_error());
    }

    let mut dims = [0i32; MAXDIM];
    let mut lbs = [0i32; MAXDIM];
    dims[0] = outer_nelems as i32;
    lbs[0] = 1;
    for i in 1..ndims as usize {
        dims[i] = elem_dims[i - 1];
        lbs[i] = elem_lbs[i - 1];
    }

    let nitems = arrayutils::array_get_n_items(ndims, &dims)?;
    arrayutils::array_check_bounds(ndims, &dims, &lbs)?;

    let dataoffset;
    let mut total = nbytes;
    if havenulls {
        dataoffset = arrayfuncs::foundation::arr_overhead_withnulls(ndims, nitems);
        total += dataoffset;
    } else {
        dataoffset = 0;
        total += arrayfuncs::foundation::arr_overhead_nonulls(ndims);
    }

    let mut out: PgVec<'_, u8> = vec_with_capacity_in(mcx, total)?;
    out.resize(total, 0);
    out[0..4].copy_from_slice(&::datum::varlena::set_varsize_4b(total));
    out[4..8].copy_from_slice(&ndims.to_ne_bytes());
    out[8..12].copy_from_slice(&(dataoffset as i32).to_ne_bytes());
    out[12..16].copy_from_slice(&(st.elemtype as u32).to_ne_bytes());
    let mut off = 16usize;
    for i in 0..ndims as usize {
        out[off..off + 4].copy_from_slice(&dims[i].to_ne_bytes());
        off += 4;
    }
    for i in 0..ndims as usize {
        out[off..off + 4].copy_from_slice(&lbs[i].to_ne_bytes());
        off += 4;
    }

    let dest_bitmap_off = arrayfuncs::foundation::arr_nullbitmap_off(&out);
    let mut dat = arrayfuncs::arr_data_offset(&out);
    let mut iitem: i32 = 0;
    for sub in subs.iter() {
        let data_off = arrayfuncs::arr_data_offset(sub.img);
        let sub_bytes = arrayfuncs::arr_size(sub.img) - data_off;
        out[dat..dat + sub_bytes].copy_from_slice(&sub.img[data_off..data_off + sub_bytes]);
        dat += sub_bytes;
        if let Some(dbo) = dest_bitmap_off {
            arrayfuncs::element::array_bitmap_copy(
                &mut out,
                dbo,
                iitem,
                arrayfuncs::foundation::arr_nullbitmap_off(sub.img).map(|bo| (sub.img, bo)),
                0,
                sub.nitems,
            );
        }
        iitem += sub.nitems;
    }

    Ok(NullableDatum { value: Datum::from_usize(out.leak().as_ptr() as usize), isnull: false })
}

// SubscriptingRefState + ArraySubWorkspace, one flat struct (the handler set
// is closed on arrays; jsonb gets its own state when it lands).
pub struct SbsRefState {
    pub isassignment: bool,
    pub numupper: u8,
    pub numlower: u8,
    pub upperprovided: [bool; MAXDIM],
    pub lowerprovided: [bool; MAXDIM],
    pub upperindex: [NullableDatum; MAXDIM],
    pub lowerindex: [NullableDatum; MAXDIM],
    pub replace: NullableDatum,
    pub prev: NullableDatum,
    pub refelemtype: Oid,
    pub refattrlength: i32,
    pub refelemlength: i32,
    pub refelembyval: bool,
    pub refelemalign: u8,
    pub upperidx: [i32; MAXDIM],
    pub loweridx: [i32; MAXDIM],
    pub resmcx: ResMcx,
}

#[track_caller]
#[cold]
fn null_subscript_error() -> Box<PgError> {
    Box::new(
        PgError::error("array subscript in assignment must not be null".to_string())
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

// array_subscript_check_subscripts: convert evaluated subscripts to ints.
// Ok(false) = some fetch subscript was NULL (caller jumps, result NULL).
pub fn sbsref_check_subscripts(st: &mut SbsRefState) -> PgResult<bool> {
    for i in 0..st.numupper as usize {
        if st.upperprovided[i] {
            if st.upperindex[i].isnull {
                if st.isassignment {
                    return Err(null_subscript_error());
                }
                return Ok(false);
            }
            st.upperidx[i] = st.upperindex[i].value.as_i32();
        }
    }
    for i in 0..st.numlower as usize {
        if st.lowerprovided[i] {
            if st.lowerindex[i].isnull {
                if st.isassignment {
                    return Err(null_subscript_error());
                }
                return Ok(false);
            }
            st.loweridx[i] = st.lowerindex[i].value.as_i32();
        }
    }
    Ok(true)
}

pub fn sbsref_fetch(st: &mut SbsRefState, cur: NullableDatum) -> PgResult<NullableDatum> {
    debug_assert!(!cur.isnull);
    let arr = if st.refattrlength > 0 {
        // Fixed-length container: raw bytes, no varlena header.
        let p = cur.value.as_usize() as *const u8;
        // SAFETY: fixed-length by-ref datum addresses refattrlength bytes.
        unsafe { core::slice::from_raw_parts(p, st.refattrlength as usize) }
    } else {
        datum_array_image(cur.value, &st.resmcx)?
    };
    let (value, isnull) = arrayfuncs::array_get_element(
        arr,
        &st.upperidx[..st.numupper as usize],
        st.refattrlength,
        st.refelemlength,
        st.refelembyval,
        st.refelemalign,
    );
    Ok(NullableDatum { value, isnull })
}

pub fn sbsref_fetch_slice(st: &mut SbsRefState, cur: NullableDatum) -> PgResult<NullableDatum> {
    debug_assert!(!cur.isnull);
    let mcx = res_mcx(&st.resmcx);
    let arr = datum_array_image(cur.value, &st.resmcx)?;
    let img = arrayfuncs::array_get_slice(
        mcx,
        arr,
        st.numupper as i32,
        &mut st.upperidx,
        &mut st.loweridx,
        &st.upperprovided,
        &st.lowerprovided,
        st.refattrlength,
        st.refelemlength,
        st.refelemalign,
    )?;
    Ok(NullableDatum { value: Datum::from_usize(img.leak().as_ptr() as usize), isnull: false })
}

pub fn sbsref_assign(st: &mut SbsRefState, cur: NullableDatum) -> PgResult<NullableDatum> {
    let mcx = res_mcx(&st.resmcx);

    if st.refattrlength > 0 && (cur.isnull || st.replace.isnull) {
        // Fixed-length arrays: punt and return the original (C shape).
        return Ok(cur);
    }

    let empty;
    let arr: &[u8] = if cur.isnull {
        empty = arrayfuncs::construct_empty_array(mcx, st.refelemtype)?;
        &empty
    } else if st.refattrlength > 0 {
        let p = cur.value.as_usize() as *const u8;
        // SAFETY: fixed-length by-ref datum addresses refattrlength bytes.
        unsafe { core::slice::from_raw_parts(p, st.refattrlength as usize) }
    } else {
        datum_array_image(cur.value, &st.resmcx)?
    };

    // C detoasts a varlena replacement value before insertion.
    let mut replace = st.replace;
    if st.refelemlength == -1 && !replace.isnull {
        let img = datum_array_image(replace.value, &st.resmcx)?;
        replace.value = Datum::from_usize(img.as_ptr() as usize);
    }

    let img = arrayfuncs::array_set_element(
        mcx,
        arr,
        &st.upperidx[..st.numupper as usize],
        replace.value,
        replace.isnull,
        st.refattrlength,
        st.refelemlength,
        st.refelembyval,
        st.refelemalign,
    )?;
    Ok(NullableDatum { value: Datum::from_usize(img.leak().as_ptr() as usize), isnull: false })
}

pub fn sbsref_assign_slice(st: &mut SbsRefState, cur: NullableDatum) -> PgResult<NullableDatum> {
    let mcx = res_mcx(&st.resmcx);

    if st.refattrlength > 0 && (cur.isnull || st.replace.isnull) {
        // Fixed-length arrays: punt and return the original (C shape).
        return Ok(cur);
    }

    let arr: &[u8] = if cur.isnull {
        arrayfuncs::construct_empty_array(mcx, st.refelemtype)?.leak()
    } else if st.refattrlength > 0 {
        let p = cur.value.as_usize() as *const u8;
        // SAFETY: fixed-length by-ref datum addresses refattrlength bytes.
        unsafe { core::slice::from_raw_parts(p, st.refattrlength as usize) }
    } else {
        datum_array_image(cur.value, &st.resmcx)?
    };

    // array_set_slice: NULL-source no-op returns the (possibly empty-substituted) input.
    if st.replace.isnull {
        if cur.isnull {
            return Ok(NullableDatum {
                value: Datum::from_usize(arr.as_ptr() as usize),
                isnull: false,
            });
        }
        return Ok(cur);
    }

    let src = datum_array_image(st.replace.value, &st.resmcx)?;
    let img = arrayfuncs::array_set_slice(
        mcx,
        arr,
        st.numupper as i32,
        &mut st.upperidx,
        &mut st.loweridx,
        &st.upperprovided,
        &st.lowerprovided,
        src,
        st.refattrlength,
        st.refelemlength,
        st.refelembyval,
        st.refelemalign,
    )?;
    Ok(NullableDatum { value: Datum::from_usize(img.leak().as_ptr() as usize), isnull: false })
}

pub fn sbsref_fetch_old(st: &mut SbsRefState, cur: NullableDatum) -> PgResult<()> {
    if cur.isnull {
        st.prev = NullableDatum { value: Datum::null(), isnull: true };
    } else if st.numlower != 0 {
        // Slices of non-null arrays are never null.
        st.prev = sbsref_fetch_slice(st, cur)?;
    } else {
        st.prev = sbsref_fetch(st, cur)?;
    }
    Ok(())
}

// `elem == None` is C's NULL elemexprstate (header relabel only); inp_* is
// the runtime-keyed get_typlenbyvalalign memo (C amstate.inp_extra).
pub struct ArrayCoerceState {
    pub resultelemtype: Oid,
    pub ret_typlen: i16,
    pub ret_typbyval: bool,
    pub ret_typalign: u8,
    pub inp_elemtype: Oid,
    pub inp_typlen: i16,
    pub inp_typbyval: bool,
    pub inp_typalign: u8,
    pub elem: Option<ArrayCoerceElem>,
    pub resmcx: ResMcx,
}

pub struct ArrayCoerceElem {
    pub slot: NonNull<NullableDatum>,
    // Compile-mcx ExprState restamped 'static; outlives every eval.
    pub state: NonNull<crate::steps::ExprState<'static>>,
}

// Caller has handled the NULL-array case.
pub fn eval_array_coerce(st: &mut ArrayCoerceState, arrd: Datum) -> PgResult<NullableDatum> {
    let mcx = res_mcx(&st.resmcx);
    let img = datum_array_image(arrd, &st.resmcx)?;

    let Some(elem) = &st.elem else {
        // DatumGetArrayTypePCopy + ARR_ELEMTYPE overwrite.
        let mut copy = ::mcx::slice_in(mcx, img)?;
        copy[12..16].copy_from_slice(&(st.resultelemtype as u32).to_ne_bytes());
        return Ok(NullableDatum {
            value: Datum::from_usize(copy.leak().as_ptr() as usize),
            isnull: false,
        });
    };

    let elemtype = arrayfuncs::arr_elemtype(img);
    if elemtype != st.inp_elemtype {
        let (l, bv, al) = lsyscache::get_typlenbyvalalign(elemtype)?;
        st.inp_elemtype = elemtype;
        st.inp_typlen = l;
        st.inp_typbyval = bv;
        st.inp_typalign = al as u8;
    }

    let (ndim, dims, lbs) = arrayfuncs::read_dims_lbounds(img);
    let (values, nulls) = arrayfuncs::deconstruct_array(
        mcx,
        img,
        st.inp_typlen as i32,
        st.inp_typbyval,
        st.inp_typalign,
        true,
    )?;
    let n = values.len();
    if n == 0 {
        let empty = arrayfuncs::construct_empty_array(mcx, st.resultelemtype)?;
        return Ok(NullableDatum {
            value: Datum::from_usize(empty.leak().as_ptr() as usize),
            isnull: false,
        });
    }

    // SAFETY: compile-allocated program + slot, sole live access (elemexpr
    // cannot contain another reference to this step's state).
    let sub = unsafe { &mut *elem.state.as_ptr() };
    sub.arm_result_mcx(mcx);

    let mut out_values: PgVec<'_, Datum> = vec_with_capacity_in(mcx, n)?;
    let mut out_nulls: PgVec<'_, bool> = vec_with_capacity_in(mcx, n)?;
    let mut hasnulls = false;
    for i in 0..n {
        // SAFETY: slot is a live compile-mcx cell owned by this step.
        unsafe {
            elem.slot.write(NullableDatum { value: values[i], isnull: nulls[i] });
        }
        let mut slots = crate::interp::EvalSlots::default();
        let r = crate::interp::exec_eval_expr(sub, &mut slots)?;
        // By-ref results are copied out: builtin out/in scratch buffers
        // (fc_textin's OutBuf) are overwritten by the next call through the
        // same flinfo, and this loop accumulates before consuming.
        let v = if r.isnull {
            hasnulls = true;
            Datum::null()
        } else if st.ret_typbyval {
            r.value
        } else if st.ret_typlen == -1 {
            // PG_DETOAST_DATUM on the per-element result.
            let p = r.value.as_usize() as *const u8;
            // SAFETY: non-null by-ref varlena result.
            unsafe {
                if ::types_tuple::varatt::varatt_is_4b_u(p) {
                    ::adt_scalar::datum_copy(mcx, r.value, false, -1)?
                } else {
                    let raw =
                        core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p));
                    let flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
                    Datum::from_usize(flat.leak().as_ptr() as usize)
                }
            }
        } else {
            ::adt_scalar::datum_copy(mcx, r.value, false, st.ret_typlen)?
        };
        out_values.push(v);
        out_nulls.push(r.isnull);
    }

    // Source dims/lbounds preserved; construct_md_array owns the size ceiling.
    let result = arrayfuncs::construct_md_array(
        mcx,
        &out_values,
        if hasnulls { Some(&out_nulls) } else { Option::None },
        ndim,
        &dims[..ndim as usize],
        &lbs[..ndim as usize],
        st.resultelemtype,
        st.ret_typlen as i32,
        st.ret_typbyval,
        st.ret_typalign,
    )?;
    Ok(NullableDatum {
        value: Datum::from_usize(result.leak().as_ptr() as usize),
        isnull: false,
    })
}
