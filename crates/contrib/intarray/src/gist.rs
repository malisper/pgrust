//! _int_gist.c: the gist__int_ops support procs over the GISTENTRY fmgr
//! protocol (pg_trgm/ltree precedent). Keys are sorted-unique int4 arrays,
//! range-compressed above 2*numranges elements.

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};

use crate::boolquery;
use crate::tool::{inner_int_contains, inner_int_overlap, inner_int_union, internal_size, IntArray};
use crate::{
    detoasted_image, entry_arg, entry_result, image_result, opclass_option_i32,
    RT_CONTAINED_BY, RT_CONTAINS, RT_OLD_CONTAINED_BY, RT_OLD_CONTAINS, RT_OVERLAP, RT_SAME,
    BOOLEAN_SEARCH_STRATEGY,
};

pub(crate) const NUMRANGES_DEFAULT: i32 = 100;
const GIST_MAX_INDEX_KEY_SIZE: i32 = 2024;
pub(crate) const NUMRANGES_MAX: i32 = (GIST_MAX_INDEX_KEY_SIZE - 4) / 8;
const MAX_ALLOC_SIZE: i64 = 0x3fffffff;
const MAXNUMELTS: i32 = {
    let a = MAX_ALLOC_SIZE / 8;
    let b = (MAX_ALLOC_SIZE - 24) / 4;
    ((if a < b { a } else { b }) / 2) as i32
};

fn get_num_ranges(f: &Option<&mut FmgrInfo>) -> i32 {
    opclass_option_i32(f, NUMRANGES_DEFAULT)
}

fn same_core(a: &IntArray, b: &IntArray) -> PgResult<bool> {
    a.check_valid()?;
    b.check_valid()?;
    if a.nelems() != b.nelems() {
        return Ok(false);
    }
    Ok(a.elems() == b.elems())
}

pub(crate) fn fc_g_int_consistent(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let _ = f;
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = strategy == RT_SAME };
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let query_img = detoasted_image(mcx, fcinfo.arg(1))?;
    let key = IntArray::from_image(crate::key_image(entry.key));

    if strategy == BOOLEAN_SEARCH_STRATEGY {
        let items = boolquery::parse_items(query_img);
        let retval = boolquery::execconsistent(&items, &key, entry.page_is_leaf)?;
        return Ok(Datum::from_bool(retval));
    }

    let mut query = IntArray::from_image(query_img);
    query.check_valid()?;
    query.prepare();

    let retval = match strategy {
        RT_OVERLAP => inner_int_overlap(key.elems(), query.elems()),
        RT_SAME => {
            if entry.page_is_leaf {
                same_core(&key, &query)?
            } else {
                inner_int_contains(key.elems(), query.elems())
            }
        }
        RT_CONTAINS | RT_OLD_CONTAINS => inner_int_contains(key.elems(), query.elems()),
        // Unreachable as of intarray 1.4 (<@ removed from the opclass); kept
        // for older SQL definitions.
        RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => {
            if entry.page_is_leaf {
                inner_int_contains(query.elems(), key.elems())
            } else {
                // Empty arrays could be anywhere in the index.
                true
            }
        }
        _ => false,
    };
    Ok(Datum::from_bool(retval))
}

pub(crate) fn fc_g_int_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = f;
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let mut totlen = 0usize;
    let mut ents: Vec<IntArray> = Vec::with_capacity(entryvec.n as usize);
    for e in &entryvec.vector[..entryvec.n as usize] {
        let ent = IntArray::from_image(crate::key_image(e.key));
        ent.check_valid()?;
        totlen += ent.nelems();
        ents.push(ent);
    }
    let mut res = IntArray::new(totlen);
    let mut off = 0usize;
    for ent in &ents {
        let nel = ent.nelems();
        res.elems_mut()[off..off + nel].copy_from_slice(ent.elems());
        off += nel;
    }
    res.sort(true);
    res.unique();
    let size_out = fcinfo.arg(1).as_usize() as *mut i32;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *size_out = res.as_bytes().len() as i32 };
    image_result(fcinfo, res.as_bytes())
}

pub(crate) fn fc_g_int_compress(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let num_ranges = get_num_ranges(&f);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    if entry.leafkey {
        let mut r = IntArray::from_image(detoasted_image(mcx, entry.key)?);
        r.check_valid()?;
        r.prepare();
        if r.nelems() as i64 >= 2 * num_ranges as i64 {
            return Err(PgError::error(format!(
                "input array is too big ({} maximum allowed, {} current), use gist__intbig_ops opclass instead",
                2 * num_ranges - 1,
                r.nelems()
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .into());
        }
        let key = image_result(fcinfo, r.as_bytes())?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }

    let img = detoasted_image(mcx, entry.key)?;
    let mut r = IntArray::from_image(img);
    r.check_valid()?;
    if r.is_empty() {
        return Ok(fcinfo.arg(0));
    }
    let len = r.nelems() as i32;
    if len as i64 >= 2 * num_ranges as i64 {
        r.resize(2 * len as usize);
        let dr = r.elems_mut();
        // lenr = ranges we may remove by merging consecutive ints.
        let mut lenr = len - num_ranges;
        let mut i: i64 = len as i64 - 1;
        let mut j: i64 = i;
        while i > 0 && lenr > 0 {
            let r_end = dr[i as usize];
            let mut r_start = r_end;
            while i > 0 && lenr > 0 && dr[i as usize - 1] == r_start - 1 {
                r_start -= 1;
                i -= 1;
                lenr -= 1;
            }
            dr[2 * j as usize] = r_start;
            dr[2 * j as usize + 1] = r_end;
            i -= 1;
            j -= 1;
        }
        while i >= 0 {
            dr[2 * j as usize] = dr[i as usize];
            dr[2 * j as usize + 1] = dr[i as usize];
            i -= 1;
            j -= 1;
        }
        j += 1;
        let mut len = len;
        if j != 0 {
            dr.copy_within(2 * j as usize..2 * len as usize, 0);
        }
        len = 2 * (len - j as i32);
        let mut cand = 1usize;
        while len > num_ranges * 2 {
            let mut min = i64::MAX;
            let mut i = 2usize;
            while (i as i32) < len {
                let gap = dr[i] as i64 - dr[i - 1] as i64;
                if min > gap {
                    min = gap;
                    cand = i;
                }
                i += 2;
            }
            dr.copy_within(cand + 1..len as usize, cand - 1);
            len -= 2;
        }
        let lenr = internal_size(&dr[..len as usize]);
        if lenr < 0 || lenr > MAXNUMELTS {
            return Err(PgError::error(
                "data is too sparse, recreate index using gist__intbig_ops opclass instead",
            )
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .into());
        }
        r.resize(len as usize);
        let key = image_result(fcinfo, r.as_bytes())?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }
    Ok(fcinfo.arg(0))
}

pub(crate) fn fc_g_int_decompress(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let num_ranges = get_num_ranges(&f);
    let was_plain = crate::is_plain_4b(entry.key);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let img = detoasted_image(mcx, entry.key)?;
    let input = IntArray::from_image(img);
    input.check_valid()?;

    let rebuild_with = |fcinfo: &Fcinfo, bytes: &[u8]| -> PgResult<Datum> {
        let key = image_result(fcinfo, bytes)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        entry_result(fcinfo, &retval)
    };

    if input.is_empty() {
        return if was_plain { Ok(fcinfo.arg(0)) } else { rebuild_with(fcinfo, input.as_bytes()) };
    }
    let lenin = input.nelems() as i32;
    if (lenin as i64) < 2 * num_ranges as i64 {
        return if was_plain { Ok(fcinfo.arg(0)) } else { rebuild_with(fcinfo, input.as_bytes()) };
    }

    let din = input.elems();
    let lenr = internal_size(din);
    if lenr < 0 || lenr > MAXNUMELTS {
        return Err(PgError::error(
            "compressed array is too big, recreate index using gist__intbig_ops opclass instead",
        )
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }

    let mut r = IntArray::new(lenr as usize);
    {
        let dr = r.elems_mut();
        let mut w = 0usize;
        let mut i = 0usize;
        'outer: while i < lenin as usize {
            // i64 j: din[i + 1] can be INT_MAX.
            let mut jj = din[i] as i64;
            while jj <= din[i + 1] as i64 {
                if i == 0 || dr[w - 1] != jj as i32 {
                    if w == dr.len() {
                        // internal_size undercount is unreachable for keys we
                        // build; C overruns its buffer here.
                        break 'outer;
                    }
                    dr[w] = jj as i32;
                    w += 1;
                }
                jj += 1;
            }
            i += 2;
        }
    }
    rebuild_with(fcinfo, r.as_bytes())
}

pub(crate) fn fc_g_int_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = f;
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let orig = IntArray::from_image(crate::key_image(origentry.key));
    let newk = IntArray::from_image(crate::key_image(newentry.key));
    let ud = inner_int_union(&orig, &newk)?;
    let result = fcinfo.arg(2).as_usize() as *mut f32;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *result = ud.nelems() as f32 - orig.nelems() as f32 };
    Ok(fcinfo.arg(2))
}

pub(crate) fn fc_g_int_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = f;
    let a = IntArray::from_image(crate::key_image(fcinfo.arg(0)));
    let b = IntArray::from_image(crate::key_image(fcinfo.arg(1)));
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = same_core(&a, &b)? };
    Ok(fcinfo.arg(2))
}

fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

pub(crate) fn fc_g_int_picksplit(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let _ = f;
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };

    let key = |i: usize| IntArray::from_image(crate::key_image(entryvec.vector[i].key));

    let seed_maxoff = (entryvec.n - 2) as usize;
    let mut firsttime = true;
    let mut waste: f32 = 0.0;
    let (mut seed_1, mut seed_2) = (0usize, 0usize);
    for i in 1..seed_maxoff {
        let datum_alpha = key(i);
        for j in i + 1..=seed_maxoff {
            let datum_beta = key(j);
            let size_union = inner_int_union(&datum_alpha, &datum_beta)?.nelems() as f32;
            let size_inter = crate::tool::inner_int_inter(&datum_alpha, &datum_beta).nelems() as f32;
            let size_waste = size_union - size_inter;
            if size_waste > waste || firsttime {
                waste = size_waste;
                seed_1 = i;
                seed_2 = j;
                firsttime = false;
            }
        }
    }

    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    let mut datum_l = key(seed_1).copy_flat();
    let mut size_l = datum_l.nelems() as f32;
    let mut datum_r = key(seed_2).copy_flat();
    let mut size_r = datum_r.nelems() as f32;

    let maxoff = (entryvec.n - 1) as usize;
    struct SplitCost {
        pos: usize,
        cost: f32,
    }
    let mut costvector: Vec<SplitCost> = Vec::with_capacity(maxoff);
    for i in 1..=maxoff {
        let datum_alpha = key(i);
        let size_alpha = inner_int_union(&datum_l, &datum_alpha)?.nelems() as f32;
        let size_beta = inner_int_union(&datum_r, &datum_alpha)?.nelems() as f32;
        costvector.push(SplitCost {
            pos: i,
            cost: ((size_alpha - size_l) - (size_beta - size_r)).abs(),
        });
    }
    costvector.sort_by(|a, b| a.cost.partial_cmp(&b.cost).unwrap_or(core::cmp::Ordering::Equal));

    let mut spl_left: Vec<u16> = Vec::with_capacity(maxoff + 1);
    let mut spl_right: Vec<u16> = Vec::with_capacity(maxoff + 1);
    for sc in &costvector {
        let i = sc.pos;
        if i == seed_1 {
            spl_left.push(i as u16);
            continue;
        }
        if i == seed_2 {
            spl_right.push(i as u16);
            continue;
        }
        let datum_alpha = key(i);
        let union_dl = inner_int_union(&datum_l, &datum_alpha)?;
        let union_dr = inner_int_union(&datum_r, &datum_alpha)?;
        let size_alpha = union_dl.nelems() as f32;
        let size_beta = union_dr.nelems() as f32;

        if f64::from(size_alpha - size_l)
            < f64::from(size_beta - size_r)
                + wish_f(spl_left.len() as i32, spl_right.len() as i32, 0.01)
        {
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
    v.spl_ldatum = image_result(fcinfo, datum_l.as_bytes())?;
    v.spl_rdatum = image_result(fcinfo, datum_r.as_bytes())?;
    Ok(fcinfo.arg(1))
}

#[cold]
pub(crate) fn fc_g_int_options(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = f;
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0.
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(8);
    relopts.add_int("numranges", NUMRANGES_DEFAULT, 1, NUMRANGES_MAX, 4);
    Ok(Datum::from_usize(0))
}
