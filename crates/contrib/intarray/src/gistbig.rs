//! _intbig_gist.c: gist__intbig_ops — lossy signature-bitmap keys (GISTTYPE:
//! vl_len, flag, sign[siglen]; ALLISTRUE collapses the sign away).

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};
use types_tuple::varatt;

use crate::boolquery;
use crate::tool::{gensign, getbit, hashval, IntArray};
use crate::{
    detoasted_image, entry_arg, entry_result, image_result, opclass_option_i32,
    RT_CONTAINED_BY, RT_CONTAINS, RT_OLD_CONTAINED_BY, RT_OLD_CONTAINS, RT_OVERLAP, RT_SAME,
    BOOLEAN_SEARCH_STRATEGY,
};

pub(crate) const SIGLEN_DEFAULT: i32 = 63 * 4;
pub(crate) const SIGLEN_MAX: i32 = 2024;
const ALLISTRUE: i32 = 0x04;
const GTHDRSIZE: usize = 8;

fn get_siglen(f: &Option<&mut FmgrInfo>) -> usize {
    opclass_option_i32(f, SIGLEN_DEFAULT) as usize
}

fn gt_alloc(allistrue: bool, siglen: usize, sign: Option<&[u8]>) -> Vec<u8> {
    let size = GTHDRSIZE + if allistrue { 0 } else { siglen };
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&varatt::set_varsize_4b_word(size as u32).to_ne_bytes());
    img.extend_from_slice(&(if allistrue { ALLISTRUE } else { 0 }).to_ne_bytes());
    if !allistrue {
        match sign {
            Some(s) => img.extend_from_slice(&s[..siglen]),
            None => img.resize(size, 0),
        }
    }
    img
}

fn is_alltrue(img: &[u8]) -> bool {
    i32::from_ne_bytes(img[4..8].try_into().unwrap()) & ALLISTRUE != 0
}

fn sign_of(img: &[u8]) -> &[u8] {
    &img[GTHDRSIZE..]
}

fn sizebitvec(sign: &[u8], siglen: usize) -> i32 {
    sign[..siglen].iter().map(|b| b.count_ones() as i32).sum()
}

fn hemdistsign(a: &[u8], b: &[u8], siglen: usize) -> i32 {
    let mut dist = 0;
    for i in 0..siglen {
        dist += (a[i] ^ b[i]).count_ones() as i32;
    }
    dist
}

fn hemdist(a: &[u8], b: &[u8], siglen: usize) -> i32 {
    if is_alltrue(a) {
        if is_alltrue(b) {
            0
        } else {
            (siglen * 8) as i32 - sizebitvec(sign_of(b), siglen)
        }
    } else if is_alltrue(b) {
        (siglen * 8) as i32 - sizebitvec(sign_of(a), siglen)
    } else {
        hemdistsign(sign_of(a), sign_of(b), siglen)
    }
}

pub(crate) fn fc_intbig_in(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot accept a value of type intbig_gkey")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into())
}

pub(crate) fn fc_intbig_out(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot display a value of type intbig_gkey")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into())
}

pub(crate) fn fc_g_intbig_compress(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let siglen = get_siglen(&f);

    if entry.leafkey {
        // SAFETY: the armed result mcx outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let input = IntArray::from_image(detoasted_image(mcx, entry.key)?);
        let mut res = gt_alloc(false, siglen, None);
        input.check_valid()?;
        if !input.is_empty() {
            gensign(&mut res[GTHDRSIZE..], input.elems(), siglen);
        }
        let key = image_result(fcinfo, &res)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }

    let img = crate::key_image(entry.key);
    if !is_alltrue(img) {
        let sign = sign_of(img);
        for i in 0..siglen {
            if sign[i] != 0xff {
                return Ok(fcinfo.arg(0));
            }
        }
        let res = gt_alloc(true, siglen, Some(sign));
        let key = image_result(fcinfo, &res)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }
    Ok(fcinfo.arg(0))
}

pub(crate) fn fc_g_intbig_decompress(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

pub(crate) fn fc_g_intbig_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = crate::key_image(fcinfo.arg(0));
    let b = crate::key_image(fcinfo.arg(1));
    let siglen = get_siglen(&f);
    let res = if is_alltrue(a) && is_alltrue(b) {
        true
    } else if is_alltrue(a) || is_alltrue(b) {
        false
    } else {
        sign_of(a)[..siglen] == sign_of(b)[..siglen]
    };
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = res };
    Ok(fcinfo.arg(2))
}

pub(crate) fn fc_g_intbig_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let siglen = get_siglen(&f);
    let mut result = gt_alloc(false, siglen, None);
    for e in &entryvec.vector[..entryvec.n as usize] {
        let add = crate::key_image(e.key);
        if is_alltrue(add) {
            result = gt_alloc(true, siglen, None);
            break;
        }
        let sadd = sign_of(add);
        for i in 0..siglen {
            result[GTHDRSIZE + i] |= sadd[i];
        }
    }
    let size_out = fcinfo.arg(1).as_usize() as *mut i32;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *size_out = result.len() as i32 };
    image_result(fcinfo, &result)
}

pub(crate) fn fc_g_intbig_penalty(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let siglen = get_siglen(&f);
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe {
        *penalty =
            hemdist(crate::key_image(origentry.key), crate::key_image(newentry.key), siglen) as f32
    };
    Ok(fcinfo.arg(2))
}

struct SignKey {
    alltrue: bool,
    sign: Vec<u8>,
}

impl SignKey {
    fn from_image(img: &[u8], siglen: usize) -> SignKey {
        if is_alltrue(img) {
            SignKey { alltrue: true, sign: vec![0u8; siglen] }
        } else {
            SignKey { alltrue: false, sign: sign_of(img)[..siglen].to_vec() }
        }
    }

    fn image(&self, siglen: usize) -> Vec<u8> {
        gt_alloc(self.alltrue, siglen, Some(&self.sign))
    }

    fn hemdist_img(&self, other: &[u8], siglen: usize) -> i32 {
        if self.alltrue {
            if is_alltrue(other) {
                0
            } else {
                (siglen * 8) as i32 - sizebitvec(sign_of(other), siglen)
            }
        } else if is_alltrue(other) {
            (siglen * 8) as i32 - sizebitvec(&self.sign, siglen)
        } else {
            hemdistsign(&self.sign, sign_of(other), siglen)
        }
    }
}

fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

pub(crate) fn fc_g_intbig_picksplit(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let siglen = get_siglen(&f);

    let key = |i: usize| crate::key_image(entryvec.vector[i].key);

    let seed_maxoff = (entryvec.n - 2) as usize;
    let mut waste: i32 = -1;
    let (mut seed_1, mut seed_2) = (0usize, 0usize);
    for k in 1..seed_maxoff {
        for j in k + 1..=seed_maxoff {
            let size_waste = hemdist(key(k), key(j), siglen);
            if size_waste > waste {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
        }
    }

    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    let mut datum_l = SignKey::from_image(key(seed_1), siglen);
    let mut datum_r = SignKey::from_image(key(seed_2), siglen);

    let maxoff = (entryvec.n - 1) as usize;
    struct SplitCost {
        pos: usize,
        cost: i32,
    }
    let mut costvector: Vec<SplitCost> = Vec::with_capacity(maxoff);
    for j in 1..=maxoff {
        let size_alpha = datum_l.hemdist_img(key(j), siglen);
        let size_beta = datum_r.hemdist_img(key(j), siglen);
        costvector.push(SplitCost { pos: j, cost: (size_alpha - size_beta).abs() });
    }
    costvector.sort_by_key(|c| c.cost);

    let mut spl_left: Vec<u16> = Vec::with_capacity(maxoff + 1);
    let mut spl_right: Vec<u16> = Vec::with_capacity(maxoff + 1);
    for sc in &costvector {
        let j = sc.pos;
        if j == seed_1 {
            spl_left.push(j as u16);
            continue;
        }
        if j == seed_2 {
            spl_right.push(j as u16);
            continue;
        }
        let jimg = key(j);
        let size_alpha = datum_l.hemdist_img(jimg, siglen);
        let size_beta = datum_r.hemdist_img(jimg, siglen);

        if (size_alpha as f64)
            < size_beta as f64 + wish_f(spl_left.len() as i32, spl_right.len() as i32, 0.00001)
        {
            if datum_l.alltrue || is_alltrue(jimg) {
                if !datum_l.alltrue {
                    datum_l.sign.fill(0xff);
                }
            } else {
                let ptr = sign_of(jimg);
                for i in 0..siglen {
                    datum_l.sign[i] |= ptr[i];
                }
            }
            spl_left.push(j as u16);
        } else {
            if datum_r.alltrue || is_alltrue(jimg) {
                if !datum_r.alltrue {
                    datum_r.sign.fill(0xff);
                }
            } else {
                let ptr = sign_of(jimg);
                for i in 0..siglen {
                    datum_r.sign[i] |= ptr[i];
                }
            }
            spl_right.push(j as u16);
        }
    }

    v.spl_left = spl_left;
    v.spl_right = spl_right;
    v.spl_ldatum = image_result(fcinfo, &datum_l.image(siglen))?;
    v.spl_rdatum = image_result(fcinfo, &datum_r.image(siglen))?;
    Ok(fcinfo.arg(1))
}

pub(crate) fn fc_g_intbig_consistent(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    let siglen = get_siglen(&f);
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // All cases served by this function are inexact.
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = true };

    let kimg = crate::key_image(entry.key);
    if is_alltrue(kimg) {
        return Ok(Datum::from_bool(true));
    }
    let sign = sign_of(kimg);

    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let query_img = detoasted_image(mcx, fcinfo.arg(1))?;

    if strategy == BOOLEAN_SEARCH_STRATEGY {
        let items = boolquery::parse_items(query_img);
        let retval = boolquery::signconsistent(&items, sign, siglen, false)?;
        return Ok(Datum::from_bool(retval));
    }

    let query = IntArray::from_image(query_img);
    query.check_valid()?;

    let overlap = |sign: &[u8]| query.elems().iter().any(|&v| getbit(sign, hashval(v, siglen)));
    let contains = |sign: &[u8]| query.elems().iter().all(|&v| getbit(sign, hashval(v, siglen)));

    let retval = match strategy {
        RT_OVERLAP => overlap(sign),
        RT_SAME => {
            if entry.page_is_leaf {
                let mut dq = vec![0u8; siglen];
                gensign(&mut dq, query.elems(), siglen);
                sign[..siglen] == dq[..]
            } else {
                contains(sign)
            }
        }
        RT_CONTAINS | RT_OLD_CONTAINS => contains(sign),
        // Unreachable as of intarray 1.4 (<@ removed from the opclass); kept
        // for older SQL definitions.
        RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => {
            if entry.page_is_leaf {
                let mut dq = vec![0u8; siglen];
                gensign(&mut dq, query.elems(), siglen);
                let mut ok = true;
                for i in 0..siglen {
                    if sign[i] & !dq[i] != 0 {
                        ok = false;
                        break;
                    }
                }
                ok
            } else {
                // Empty arrays could be anywhere in the index.
                true
            }
        }
        _ => false,
    };
    Ok(Datum::from_bool(retval))
}

#[cold]
pub(crate) fn fc_g_intbig_options(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let _ = f;
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0.
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(8);
    relopts.add_int("siglen", SIGLEN_DEFAULT, 1, SIGLEN_MAX, 4);
    Ok(Datum::from_usize(0))
}
