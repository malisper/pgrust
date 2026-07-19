//! gist_cube_ops (cube.c g_cube_*) over the GISTENTRY fmgr protocol
//! (hstore/ltree precedent): consistent, union, compress/decompress
//! (identity — dropped from the opclass by the 1.3--1.4 upgrade anyway),
//! penalty, Guttman poly-time picksplit, same, and the KNN distance proc
//! (strategies 15 `~>` / 16 `<#>` / 17 `<->` / 18 `<=>`).

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_ARRAY_ELEMENT_ERROR};
use types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};
use types_tuple::varatt;

use crate::ops;
use crate::repr::CubeView;

// access/stratnum.h RT* strategy numbers used by gist_cube_ops.
const RT_OVERLAP: u16 = 3;
const RT_SAME: u16 = 6;
const RT_CONTAINS: u16 = 7;
const RT_CONTAINED_BY: u16 = 8;
const RT_OLD_CONTAINS: u16 = 13;
const RT_OLD_CONTAINED_BY: u16 = 14;
// cubedata.h KNN strategies.
const KNN_COORD: u16 = 15;
const KNN_TAXICAB: u16 = 16;
const KNN_EUCLID: u16 = 17;
const KNN_CHEBYSHEV: u16 = 18;

// SAFETY: gist fmgr protocol — arg i is a live GISTENTRY pointer.
pub(crate) unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

/// Payload (post-varlena-header) bytes of a cube datum. cube has
/// `STORAGE = plain`, so images are 4-byte-header everywhere (heap, index
/// keys, expression results); short/toasted forms are canonicalized
/// defensively anyway.
pub(crate) unsafe fn cube_payload<'a>(d: Datum) -> std::borrow::Cow<'a, [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller passes a live non-null cube datum.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            std::borrow::Cow::Borrowed(core::slice::from_raw_parts(
                p.add(4),
                varatt::varsize_4b(p) - 4,
            ))
        } else {
            let total = varatt::varsize_any(p);
            let hdr = if varatt::varatt_is_1b(p) { 1 } else { 4 };
            std::borrow::Cow::Owned(
                core::slice::from_raw_parts(p.add(hdr), total - hdr).to_vec(),
            )
        }
    }
}

fn image_result(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

/// `rt_cube_size` on an image (never the C NULL special case here).
fn size_of_image(img: &[u8]) -> f64 {
    ops::rt_cube_size(&CubeView::from_payload(&img[4..]))
}

/// `cube_union_v0` on images, honoring C's `a == b` pointer-equality
/// shortcut via an explicit same-image check at the call sites that need it
/// (picksplit seeds).
fn union_images(a: &[u8], b: &[u8]) -> Vec<u8> {
    ops::cube_union_v0(
        &CubeView::from_payload(&a[4..]),
        &CubeView::from_payload(&b[4..]),
    )
}

/// `g_cube_consistent`.
pub fn fc_g_cube_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    // All cases served by this function are exact.
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = false };

    // SAFETY: non-null cube datums per the strict catalog signature.
    let key_img = unsafe { cube_payload(entry.key) };
    let query_img = unsafe { cube_payload(fcinfo.arg(1)) };
    let key = CubeView::from_payload(&key_img);
    let query = CubeView::from_payload(&query_img);

    let res = if entry.page_is_leaf {
        // g_cube_leaf_consistent
        match strategy {
            RT_OVERLAP => ops::cube_overlap_v0(&key, &query),
            RT_SAME => ops::cube_cmp_v0(&key, &query) == 0,
            RT_CONTAINS | RT_OLD_CONTAINS => ops::cube_contains_v0(&key, &query),
            RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => ops::cube_contains_v0(&query, &key),
            _ => false,
        }
    } else {
        // g_cube_internal_consistent
        match strategy {
            RT_OVERLAP => ops::cube_overlap_v0(&key, &query),
            RT_SAME | RT_CONTAINS | RT_OLD_CONTAINS => ops::cube_contains_v0(&key, &query),
            RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => ops::cube_overlap_v0(&key, &query),
            _ => false,
        }
    };

    Ok(Datum::from_bool(res))
}

/// `g_cube_union`.
pub fn fc_g_cube_union(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let sizep = fcinfo.arg(1).as_usize() as *mut i32;

    let n = entryvec.n as usize;
    // SAFETY: non-null cube key datums.
    let mut out: Vec<u8> = unsafe { cube_payload(entryvec.vector[0].key) }.to_vec();
    // Rebuild the full image (header + payload) for the union loop.
    let mut cur = {
        let mut img = Vec::with_capacity(4 + out.len());
        img.extend_from_slice(&datum::varlena::set_varsize_4b(4 + out.len()));
        img.append(&mut out);
        img
    };
    for i in 1..n {
        // SAFETY: non-null cube key datums.
        let other = unsafe { cube_payload(entryvec.vector[i].key) };
        let mut full = Vec::with_capacity(4 + other.len());
        full.extend_from_slice(&datum::varlena::set_varsize_4b(4 + other.len()));
        full.extend_from_slice(&other);
        cur = union_images(&cur, &full);
    }
    // SAFETY: size out-param live in the caller frame.
    unsafe { *sizep = cur.len() as i32 };
    image_result(fcinfo, &cur)
}

/// `g_cube_compress` — identity.
pub fn fc_g_cube_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

/// `g_cube_decompress` — identity (cube is STORAGE = plain, never toasted).
pub fn fc_g_cube_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

/// `g_cube_penalty`: change in area.
pub fn fc_g_cube_penalty(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let result = fcinfo.arg(2).as_usize() as *mut f32;

    // SAFETY: non-null cube key datums.
    let orig = unsafe { cube_payload(origentry.key) };
    let new = unsafe { cube_payload(newentry.key) };
    let ud = ops::cube_union_v0(
        &CubeView::from_payload(&orig),
        &CubeView::from_payload(&new),
    );
    let tmp1 = size_of_image(&ud);
    let tmp2 = ops::rt_cube_size(&CubeView::from_payload(&orig));
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *result = (tmp1 - tmp2) as f32 };
    Ok(fcinfo.arg(2))
}

/// `g_cube_picksplit`: Guttman's poly-time split, ported loop-for-loop
/// (including the `maxoff = n - 2` seed-search bound and the C pointer
/// trick `cube_union_v0(a, a) == a` for the seed datums).
pub fn fc_g_cube_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };

    let n = entryvec.n as usize;
    // Full images (header restored) for every valid offset 1..n-1.
    let mut imgs: Vec<Vec<u8>> = Vec::with_capacity(n);
    imgs.push(Vec::new()); // offset 0 unused
    for i in 1..n {
        // SAFETY: non-null cube key datums.
        let payload = unsafe { cube_payload(entryvec.vector[i].key) };
        let mut full = Vec::with_capacity(4 + payload.len());
        full.extend_from_slice(&datum::varlena::set_varsize_4b(4 + payload.len()));
        full.extend_from_slice(&payload);
        imgs.push(full);
    }

    let seed_search_maxoff = n - 2;
    let mut firsttime = true;
    let mut waste = 0.0f64;
    let (mut seed_1, mut seed_2) = (1usize, 2usize);

    for i in 1..seed_search_maxoff {
        for j in (i + 1)..=seed_search_maxoff {
            // size_waste = size_union - size_inter
            let union_d = union_images(&imgs[i], &imgs[j]);
            let size_union = size_of_image(&union_d);
            let inter_d = ops::cube_inter_v0(
                &CubeView::from_payload(&imgs[i][4..]),
                &CubeView::from_payload(&imgs[j][4..]),
            );
            let size_inter = size_of_image(&inter_d);
            let size_waste = size_union - size_inter;
            if size_waste > waste || firsttime {
                waste = size_waste;
                seed_1 = i;
                seed_2 = j;
                firsttime = false;
            }
        }
    }

    let mut spl_left: Vec<u16> = Vec::new();
    let mut spl_right: Vec<u16> = Vec::new();

    // cube_union_v0(a, a) returns `a` itself (C pointer-equality shortcut):
    // the seed page keys start as the seed entries, unnormalized.
    let mut datum_l = imgs[seed_1].clone();
    let mut size_l = size_of_image(&datum_l);
    let mut datum_r = imgs[seed_2].clone();
    let mut size_r = size_of_image(&datum_r);

    let maxoff = n - 1;
    for i in 1..=maxoff {
        if i == seed_1 {
            spl_left.push(i as u16);
            continue;
        }
        if i == seed_2 {
            spl_right.push(i as u16);
            continue;
        }

        // which page needs least enlargement?
        let union_dl = union_images(&datum_l, &imgs[i]);
        let union_dr = union_images(&datum_r, &imgs[i]);
        let size_alpha = size_of_image(&union_dl);
        let size_beta = size_of_image(&union_dr);

        if size_alpha - size_l < size_beta - size_r {
            datum_l = union_dl;
            size_l = size_alpha;
            spl_left.push(i as u16);
        } else {
            datum_r = union_dr;
            size_r = size_beta;
            spl_right.push(i as u16);
        }
    }

    v.spl_left = spl_left;
    v.spl_right = spl_right;
    v.spl_ldatum = image_result(fcinfo, &datum_l)?;
    v.spl_rdatum = image_result(fcinfo, &datum_r)?;
    Ok(fcinfo.arg(1))
}

/// `g_cube_same`.
pub fn fc_g_cube_same(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: non-null cube datums per the catalog signature.
    let a_img = unsafe { cube_payload(fcinfo.arg(0)) };
    let b_img = unsafe { cube_payload(fcinfo.arg(1)) };
    let same = ops::cube_cmp_v0(
        &CubeView::from_payload(&a_img),
        &CubeView::from_payload(&b_img),
    ) == 0;
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = same };
    Ok(fcinfo.arg(2))
}

/// `g_cube_distance` — the KNN ordering proc.
pub fn fc_g_cube_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    // SAFETY: non-null cube key datum.
    let key_img = unsafe { cube_payload(entry.key) };
    let cube = CubeView::from_payload(&key_img);

    let retval = if strategy == KNN_COORD {
        // ordering by the ~> coordinate getter
        let coord = fcinfo.arg(1).as_i32();
        if coord == 0 {
            return Err(Box::new(
                PgError::error("zero cube index is not defined")
                    .with_sqlstate(ERRCODE_ARRAY_ELEMENT_ERROR),
            ));
        }
        ops::coord_llur(&cube, coord, !entry.page_is_leaf)
    } else {
        // SAFETY: non-null cube query datum.
        let query_img = unsafe { cube_payload(fcinfo.arg(1)) };
        let query = CubeView::from_payload(&query_img);
        match strategy {
            KNN_TAXICAB => ops::distance_taxicab(&cube, &query),
            KNN_EUCLID => ops::cube_distance(&cube, &query),
            KNN_CHEBYSHEV => ops::distance_chebyshev(&cube, &query),
            _ => {
                return Err(Box::new(PgError::error(format!(
                    "unrecognized cube strategy number: {strategy}"
                ))))
            }
        }
    };
    Ok(Datum::from_f64(retval))
}
