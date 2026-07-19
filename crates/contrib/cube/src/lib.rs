//! contrib/cube — the N-dimensional cube type: flex/bison-equivalent text
//! parser, binary I/O, btree/R-tree operators, constructors, and the
//! gist_cube_ops opclass with KNN distance ordering.
//!
//! Functions dispatch through the dfmgr builtin-library registry
//! (hstore/ltree precedent). The on-disk NDBOX image (including the
//! point-compression flag that halves point storage) is byte-for-byte C's —
//! see `repr`.

pub mod gist;
pub mod ops;
pub mod parse;
pub mod repr;

use datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_ELEMENT_ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use repr::{build_image, build_image_auto, cube_size, point_size, CubeView, CUBE_MAX_DIM};

const LIBRARY: &str = "cube";

const TOO_MANY_DIMS_DETAIL: &str = "A cube cannot have more than 100 dimensions.";

fn cant_extend() -> Box<PgError> {
    Box::new(
        PgError::error("can't extend cube")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(TOO_MANY_DIMS_DETAIL),
    )
}

fn array_too_long() -> Box<PgError> {
    Box::new(
        PgError::error("array is too long")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(TOO_MANY_DIMS_DETAIL),
    )
}

fn nulls_in_array() -> Box<PgError> {
    Box::new(
        PgError::error("cannot work with arrays containing NULLs")
            .with_sqlstate(ERRCODE_ARRAY_ELEMENT_ERROR),
    )
}

// PG_GETARG_NDBOX_P: view over the detoasted cube arg's payload.
// SAFETY wrapper: callers assert arg i is a non-null cube varlena.
unsafe fn arg_cube<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<CubeView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(CubeView::from_payload(v.data()))
}

fn ret_cube(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

fn ret_null(fcinfo: &mut Fcinfo) -> Datum {
    fcinfo.isnull = true;
    Datum::null()
}

/// Deconstruct a builtin-element array argument into (datums, any_nulls).
/// The count matches C's `ARRNELEMS` (all dims flattened).
fn arg_array_elems(
    fcinfo: &Fcinfo,
    i: usize,
    elmtype: types_core::Oid,
) -> PgResult<(Vec<Datum>, bool)> {
    // SAFETY: strict catalog fn; arg i is a non-null array varlena.
    let payload = unsafe { fcinfo.arg_varlena_packed(i)? };
    let data = payload.data();
    let mut full = Vec::with_capacity(4 + data.len());
    full.extend_from_slice(&datum::varlena::set_varsize_4b(4 + data.len()));
    full.extend_from_slice(data);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (elems, nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, &full, elmtype, true)?;
    let has_null = nulls.iter().any(|&n| n);
    Ok((elems.iter().copied().collect(), has_null))
}

// ===========================================================================
// Input/output
// ===========================================================================

fn fc_cube_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes().to_vec();
    match parse::parse_cube(&s) {
        Ok(img) => ret_cube(fcinfo, &img),
        Err(e) => {
            // SAFETY: context, if set, rides per the ErrorSaveNode contract.
            let esc = unsafe { fcinfo.soft_error_context() };
            types_error::ereturn(esc, ret_null(fcinfo), *e)
        }
    }
}

fn fc_cube_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let cube = unsafe { arg_cube(fcinfo, 0)? };
    let dim = cube.dim();

    let mut out: Vec<u8> = Vec::new();
    let mut numbuf = [0u8; 40];

    out.push(b'(');
    for i in 0..dim {
        if i > 0 {
            out.extend_from_slice(b", ");
        }
        let n = adt_float::io::float8out_internal(cube.ll(i), &mut numbuf);
        out.extend_from_slice(&numbuf[..n]);
    }
    out.push(b')');

    if !cube.is_point_internal() {
        out.extend_from_slice(b",(");
        for i in 0..dim {
            if i > 0 {
                out.extend_from_slice(b", ");
            }
            let n = adt_float::io::float8out_internal(cube.ur(i), &mut numbuf);
            out.extend_from_slice(&numbuf[..n]);
        }
        out.push(b')');
    }

    out.push(0);
    let mcx = fcinfo.result_mcx();
    let mut buf: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, out.len())?;
    mcx::vec_append_bytes(&mut buf, &out)?;
    Ok(cstring_result(buf))
}

fn fc_cube_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let cube = unsafe { arg_cube(fcinfo, 0)? };
    let mut nitems = cube.dim();
    if !cube.is_point() {
        nitems += nitems;
    }

    let mut buf: Vec<u8> = Vec::with_capacity(4 + 8 * nitems);
    buf.extend_from_slice(&cube.header().to_be_bytes());
    // for symmetry with cube_recv, raw x[] slots (no LL/UR normalization)
    for i in 0..nitems {
        buf.extend_from_slice(&cube.x(i).to_bits().to_be_bytes());
    }
    Ok(varlena_result(varlena::cstring_to_text(
        fcinfo.result_mcx(),
        &buf,
    )?))
}

fn fc_cube_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let header = pqformat::pq_getmsgint(buf, 4)?;
    let mut nitems = (header & repr::DIM_MASK) as usize;
    if nitems > CUBE_MAX_DIM {
        return Err(Box::new(
            PgError::error("cube dimension is too large")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_detail(TOO_MANY_DIMS_DETAIL),
        ));
    }
    if header & repr::POINT_BIT == 0 {
        nitems += nitems;
    }

    let size = repr::NDBOX_HDR + 8 * nitems;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(size));
    // the header word is stored as received (C copies it verbatim)
    img.extend_from_slice(&header.to_ne_bytes());
    for _ in 0..nitems {
        let x = pqformat::pq_getmsgfloat8(buf)?;
        img.extend_from_slice(&x.to_ne_bytes());
    }
    ret_cube(fcinfo, &img)
}

// ===========================================================================
// Constructors
// ===========================================================================

/// `cube(float8[], float8[])` — cube_a_f8_f8.
fn fc_cube_a_f8_f8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (ur, ur_nulls) = arg_array_elems(fcinfo, 0, types_core::FLOAT8OID)?;
    let (ll, ll_nulls) = arg_array_elems(fcinfo, 1, types_core::FLOAT8OID)?;
    if ur_nulls || ll_nulls {
        return Err(nulls_in_array());
    }
    let dim = ur.len();
    if dim > CUBE_MAX_DIM {
        return Err(cant_extend());
    }
    if ll.len() != dim {
        return Err(Box::new(
            PgError::error("UR and LL arrays must be of same length")
                .with_sqlstate(ERRCODE_ARRAY_ELEMENT_ERROR),
        ));
    }

    let mut coords = Vec::with_capacity(2 * dim);
    for d in &ur {
        coords.push(d.as_f64());
    }
    for d in &ll {
        coords.push(d.as_f64());
    }
    ret_cube(fcinfo, &build_image_auto(dim, &coords))
}

/// `cube(float8[])` — cube_a_f8, a zero-volume cube (point).
fn fc_cube_a_f8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (ur, ur_nulls) = arg_array_elems(fcinfo, 0, types_core::FLOAT8OID)?;
    if ur_nulls {
        return Err(nulls_in_array());
    }
    let dim = ur.len();
    if dim > CUBE_MAX_DIM {
        return Err(array_too_long());
    }
    let coords: Vec<f64> = ur.iter().map(|d| d.as_f64()).collect();
    ret_cube(fcinfo, &build_image(dim, true, &coords))
}

fn fc_cube_subset(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let c = unsafe { arg_cube(fcinfo, 0)? };
    let (idx, idx_nulls) = arg_array_elems(fcinfo, 1, types_core::INT4OID)?;
    if idx_nulls {
        return Err(nulls_in_array());
    }
    let dim = idx.len();
    if dim > CUBE_MAX_DIM {
        return Err(array_too_long());
    }

    let point = c.is_point();
    let mut coords = vec![0f64; if point { dim } else { 2 * dim }];
    for (i, d) in idx.iter().enumerate() {
        let dx = d.as_i32();
        if dx <= 0 || dx > c.dim() as i32 {
            return Err(Box::new(
                PgError::error("Index out of bounds").with_sqlstate(ERRCODE_ARRAY_ELEMENT_ERROR),
            ));
        }
        coords[i] = c.x(dx as usize - 1);
        if !point {
            coords[i + dim] = c.x(dx as usize + c.dim() - 1);
        }
    }
    ret_cube(fcinfo, &build_image(dim, point, &coords))
}

/// `cube(float8)` — cube_f8.
fn fc_cube_f8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let x = fcinfo.arg(0).as_f64();
    ret_cube(fcinfo, &build_image(1, true, &[x]))
}

/// `cube(float8, float8)` — cube_f8_f8.
fn fc_cube_f8_f8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let x0 = fcinfo.arg(0).as_f64();
    let x1 = fcinfo.arg(1).as_f64();
    let img = if x0 == x1 {
        build_image(1, true, &[x0])
    } else {
        build_image(1, false, &[x0, x1])
    };
    ret_cube(fcinfo, &img)
}

/// `cube(cube, float8)` — cube_c_f8.
fn fc_cube_c_f8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let c = unsafe { arg_cube(fcinfo, 0)? };
    let x = fcinfo.arg(1).as_f64();
    let dim = c.dim();
    if dim + 1 > CUBE_MAX_DIM {
        return Err(cant_extend());
    }

    let img = if c.is_point() {
        let mut coords = Vec::with_capacity(dim + 1);
        for i in 0..dim {
            coords.push(c.x(i));
        }
        coords.push(x);
        build_image(dim + 1, true, &coords)
    } else {
        let rdim = dim + 1;
        let mut coords = vec![0f64; 2 * rdim];
        for i in 0..dim {
            coords[i] = c.x(i);
            coords[rdim + i] = c.x(dim + i);
        }
        coords[rdim - 1] = x;
        coords[2 * rdim - 1] = x;
        build_image(rdim, false, &coords)
    };
    ret_cube(fcinfo, &img)
}

/// `cube(cube, float8, float8)` — cube_c_f8_f8.
fn fc_cube_c_f8_f8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let c = unsafe { arg_cube(fcinfo, 0)? };
    let x1 = fcinfo.arg(1).as_f64();
    let x2 = fcinfo.arg(2).as_f64();
    let dim = c.dim();
    if dim + 1 > CUBE_MAX_DIM {
        return Err(cant_extend());
    }

    let img = if c.is_point() && x1 == x2 {
        let mut coords = Vec::with_capacity(dim + 1);
        for i in 0..dim {
            coords.push(c.x(i));
        }
        coords.push(x1);
        build_image(dim + 1, true, &coords)
    } else {
        let rdim = dim + 1;
        let mut coords = vec![0f64; 2 * rdim];
        for i in 0..dim {
            coords[i] = c.ll(i);
            coords[rdim + i] = c.ur(i);
        }
        coords[rdim - 1] = x1;
        coords[2 * rdim - 1] = x2;
        build_image(rdim, false, &coords)
    };
    ret_cube(fcinfo, &img)
}

// ===========================================================================
// btree comparisons
// ===========================================================================

fn cmp_args(fcinfo: &Fcinfo) -> PgResult<i32> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    Ok(ops::cube_cmp_v0(
        &a,
        &b,
    ))
}

fn fc_cube_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(cmp_args(fcinfo)?))
}
fn fc_cube_eq(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? == 0))
}
fn fc_cube_ne(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? != 0))
}
fn fc_cube_lt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? < 0))
}
fn fc_cube_gt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? > 0))
}
fn fc_cube_le(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? <= 0))
}
fn fc_cube_ge(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? >= 0))
}

// ===========================================================================
// R-tree operators
// ===========================================================================

fn fc_cube_contains(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    Ok(Datum::from_bool(ops::cube_contains_v0(
        &a,
        &b,
    )))
}

fn fc_cube_contained(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    Ok(Datum::from_bool(ops::cube_contains_v0(
        &b,
        &a,
    )))
}

fn fc_cube_overlap(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    Ok(Datum::from_bool(ops::cube_overlap_v0(
        &a,
        &b,
    )))
}

fn fc_cube_union(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // C's cube_union_v0 trivial case: identical pointers return the input
    // unchanged (matters for unnormalized cubes, which stay unnormalized).
    if fcinfo.arg(0).as_usize() == fcinfo.arg(1).as_usize() {
        return Ok(fcinfo.arg(0));
    }
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    let img = ops::cube_union_v0(
        &a,
        &b,
    );
    ret_cube(fcinfo, &img)
}

fn fc_cube_inter(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    let img = ops::cube_inter_v0(
        &a,
        &b,
    );
    ret_cube(fcinfo, &img)
}

fn fc_cube_size(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    Ok(Datum::from_f64(ops::rt_cube_size(
        &a,
    )))
}

// ===========================================================================
// Distances
// ===========================================================================

fn fc_cube_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    Ok(Datum::from_f64(ops::cube_distance(
        &a,
        &b,
    )))
}

fn fc_distance_taxicab(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    Ok(Datum::from_f64(ops::distance_taxicab(
        &a,
        &b,
    )))
}

fn fc_distance_chebyshev(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null cube args.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let b = unsafe { arg_cube(fcinfo, 1)? };
    Ok(Datum::from_f64(ops::distance_chebyshev(
        &a,
        &b,
    )))
}

// ===========================================================================
// Introspection
// ===========================================================================

fn fc_cube_is_point(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    Ok(Datum::from_bool(
        a.is_point_internal(),
    ))
}

fn fc_cube_dim(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    Ok(Datum::from_i32(a.dim() as i32))
}

fn fc_cube_ll_coord(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let c = unsafe { arg_cube(fcinfo, 0)? };
    let n = fcinfo.arg(1).as_i32();
    let result = if n > 0 && c.dim() as i32 >= n {
        let i = (n - 1) as usize;
        repr::c_min(c.ll(i), c.ur(i))
    } else {
        0.0
    };
    Ok(Datum::from_f64(result))
}

fn fc_cube_ur_coord(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let c = unsafe { arg_cube(fcinfo, 0)? };
    let n = fcinfo.arg(1).as_i32();
    let result = if n > 0 && c.dim() as i32 >= n {
        let i = (n - 1) as usize;
        repr::c_max(c.ll(i), c.ur(i))
    } else {
        0.0
    };
    Ok(Datum::from_f64(result))
}

/// `cube -> int` — cube_coord, the raw-slot getter.
fn fc_cube_coord(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let c = unsafe { arg_cube(fcinfo, 0)? };
    let coord = fcinfo.arg(1).as_i32();

    if coord <= 0 || coord > 2 * c.dim() as i32 {
        return Err(Box::new(
            PgError::error(format!("cube index {coord} is out of bounds"))
                .with_sqlstate(ERRCODE_ARRAY_ELEMENT_ERROR),
        ));
    }
    let result = if c.is_point() {
        c.x((coord as usize - 1) % c.dim())
    } else {
        c.x(coord as usize - 1)
    };
    Ok(Datum::from_f64(result))
}

/// `cube ~> int` — cube_coord_llur, the KNN-compatible getter.
fn fc_cube_coord_llur(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let c = unsafe { arg_cube(fcinfo, 0)? };
    let coord = fcinfo.arg(1).as_i32();
    if coord == 0 {
        return Err(Box::new(
            PgError::error("zero cube index is not defined")
                .with_sqlstate(ERRCODE_ARRAY_ELEMENT_ERROR),
        ));
    }
    Ok(Datum::from_f64(ops::coord_llur(&c, coord, false)))
}

fn fc_cube_enlarge(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null cube arg.
    let a = unsafe { arg_cube(fcinfo, 0)? };
    let r = fcinfo.arg(1).as_f64();
    let mut n = fcinfo.arg(2).as_i32();

    if n > CUBE_MAX_DIM as i32 {
        n = CUBE_MAX_DIM as i32;
    }
    let mut dim = 0usize;
    if r > 0.0 && n > 0 {
        dim = n as usize;
    }
    if a.dim() > dim {
        dim = a.dim();
    }

    let mut coords = vec![0f64; 2 * dim];
    for i in 0..a.dim() {
        let j = dim + i;
        if a.ll(i) >= a.ur(i) {
            coords[i] = a.ur(i) - r;
            coords[j] = a.ll(i) + r;
        } else {
            coords[i] = a.ll(i) - r;
            coords[j] = a.ur(i) + r;
        }
        if coords[i] > coords[j] {
            coords[i] = (coords[i] + coords[j]) / 2.0;
            coords[j] = coords[i];
        }
    }
    for i in a.dim()..dim {
        coords[i] = -r;
        coords[dim + i] = r;
    }

    ret_cube(fcinfo, &build_image_auto(dim, &coords))
}

// ===========================================================================
// dfmgr dispatch
// ===========================================================================

fn lookup(name: &str) -> Option<PGFunction> {
    Some(match name {
        "cube_in" => fc_cube_in,
        "cube_out" => fc_cube_out,
        "cube_recv" => fc_cube_recv,
        "cube_send" => fc_cube_send,
        "cube_a_f8_f8" => fc_cube_a_f8_f8,
        "cube_a_f8" => fc_cube_a_f8,
        "cube_f8" => fc_cube_f8,
        "cube_f8_f8" => fc_cube_f8_f8,
        "cube_c_f8" => fc_cube_c_f8,
        "cube_c_f8_f8" => fc_cube_c_f8_f8,
        "cube_subset" => fc_cube_subset,
        "cube_cmp" => fc_cube_cmp,
        "cube_eq" => fc_cube_eq,
        "cube_ne" => fc_cube_ne,
        "cube_lt" => fc_cube_lt,
        "cube_gt" => fc_cube_gt,
        "cube_le" => fc_cube_le,
        "cube_ge" => fc_cube_ge,
        "cube_contains" => fc_cube_contains,
        "cube_contained" => fc_cube_contained,
        "cube_overlap" => fc_cube_overlap,
        "cube_union" => fc_cube_union,
        "cube_inter" => fc_cube_inter,
        "cube_size" => fc_cube_size,
        "cube_distance" => fc_cube_distance,
        "distance_taxicab" => fc_distance_taxicab,
        "distance_chebyshev" => fc_distance_chebyshev,
        "cube_is_point" => fc_cube_is_point,
        "cube_dim" => fc_cube_dim,
        "cube_ll_coord" => fc_cube_ll_coord,
        "cube_ur_coord" => fc_cube_ur_coord,
        "cube_coord" => fc_cube_coord,
        "cube_coord_llur" => fc_cube_coord_llur,
        "cube_enlarge" => fc_cube_enlarge,
        "g_cube_consistent" => gist::fc_g_cube_consistent,
        "g_cube_compress" => gist::fc_g_cube_compress,
        "g_cube_decompress" => gist::fc_g_cube_decompress,
        "g_cube_penalty" => gist::fc_g_cube_penalty,
        "g_cube_picksplit" => gist::fc_g_cube_picksplit,
        "g_cube_union" => gist::fc_g_cube_union,
        "g_cube_same" => gist::fc_g_cube_same,
        "g_cube_distance" => gist::fc_g_cube_distance,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

// Silence the unused-import warning for point_size/cube_size, which exist
// for parity documentation and unit assertions.
#[allow(unused)]
fn _size_parity() {
    let _ = point_size(1);
    let _ = cube_size(1);
}
