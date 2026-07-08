//! gist_hstore_ops (hstore_gist.c) over the GISTENTRY fmgr protocol
//! (pg_trgm/tsgistidx precedent). A GISTTYPE key is
//! [varlena hdr | i32 flag | siglen sign bytes]; ALLISTRUE (0x04) stores no
//! sign. Unlike pg_trgm there is no leaf array form: compress hashes the
//! hstore's key/value CRCs straight into a signature.

use datum::Datum;
use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};
use types_tuple::varatt;

use crate::repr::HstoreView;

const HSTORE_CONTAINS_STRATEGY: u16 = 7;
const HSTORE_EXISTS_STRATEGY: u16 = 9;
const HSTORE_EXISTS_ANY_STRATEGY: u16 = 10;
const HSTORE_EXISTS_ALL_STRATEGY: u16 = 11;
const HSTORE_OLD_CONTAINS_STRATEGY: u16 = 13;

const BITBYTE: usize = 8;
pub const SIGLEN_DEFAULT: usize = 16;
const ALLISTRUE: i32 = 0x04;
const GTHDRSIZE: usize = 4 + 4;

const fn maxalign(len: usize) -> usize {
    (len + 7) & !7usize
}
const fn maxalign_down(len: usize) -> usize {
    len & !7usize
}
const GIST_MAX_INDEX_TUPLE_SIZE: usize = maxalign_down((8192 - 24 - 16) / 4 - 4);
// SIGLEN_MAX = GISTMaxIndexKeySize (hstore takes the raw key-size bound).
pub const SIGLEN_MAX: i32 = (GIST_MAX_INDEX_TUPLE_SIZE - maxalign(8)) as i32;

const GIST_HSTORE_OPTIONS_SIZE: usize = 8;
const GIST_HSTORE_OPTIONS_SIGLEN_OFF: usize = 4;

fn siglenbit(siglen: usize) -> usize {
    siglen * BITBYTE
}

fn hashval(val: u32, siglen: usize) -> usize {
    (val as usize) % siglenbit(siglen)
}

fn getbit(sign: &[u8], i: usize) -> bool {
    (sign[i / BITBYTE] >> (i % BITBYTE)) & 0x01 != 0
}

fn hash(sign: &mut [u8], val: u32, siglen: usize) {
    let i = hashval(val, siglen);
    sign[i / BITBYTE] |= 0x01 << (i % BITBYTE);
}

fn sizebitvec(sign: &[u8]) -> i32 {
    sign.iter().map(|b| b.count_ones() as i32).sum()
}

fn hemdistsign(a: &[u8], b: &[u8], siglen: usize) -> i32 {
    (0..siglen).map(|i| (a[i] ^ b[i]).count_ones() as i32).sum()
}

fn crc32_sz(buf: &[u8]) -> u32 {
    crc32c::traditional_crc32(buf)
}

#[derive(Clone)]
enum GhKey {
    Sign(Vec<u8>),
    AllTrue,
}

impl GhKey {
    fn is_alltrue(&self) -> bool {
        matches!(self, GhKey::AllTrue)
    }
}

fn sign_of(key: &GhKey) -> &[u8] {
    match key {
        GhKey::Sign(s) => s,
        GhKey::AllTrue => &[],
    }
}

// The stored image may carry MAXALIGN padding; bound by VARSIZE, not len.
fn decode_key(image: &[u8]) -> PgResult<GhKey> {
    if image.len() < GTHDRSIZE {
        return Err(PgError::error("corrupt ghstore GiST key (short header)").into());
    }
    let raw = u32::from_ne_bytes(image[0..4].try_into().unwrap());
    let varsize = ((raw >> 2) as usize).min(image.len()).max(GTHDRSIZE);
    let flag = i32::from_ne_bytes(image[4..8].try_into().unwrap());
    if flag & ALLISTRUE != 0 {
        Ok(GhKey::AllTrue)
    } else {
        Ok(GhKey::Sign(image[GTHDRSIZE..varsize].to_vec()))
    }
}

fn ghstore_alloc(allistrue: bool, siglen: usize, sign: Option<&[u8]>) -> Vec<u8> {
    let datalen = if allistrue { 0 } else { siglen };
    let size = GTHDRSIZE + datalen;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(size));
    img.extend_from_slice(&(if allistrue { ALLISTRUE } else { 0 }).to_ne_bytes());
    if !allistrue {
        match sign {
            Some(s) => img.extend_from_slice(s),
            None => img.resize(size, 0),
        }
    }
    img
}

fn hemdist(a: &GhKey, b: &GhKey, siglen: usize) -> i32 {
    if a.is_alltrue() {
        if b.is_alltrue() {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(sign_of(b))
        }
    } else if b.is_alltrue() {
        siglenbit(siglen) as i32 - sizebitvec(sign_of(a))
    } else {
        hemdistsign(sign_of(a), sign_of(b), siglen)
    }
}

// ===========================================================================
// fmgr protocol plumbing (pg_trgm precedent).
// ===========================================================================

#[inline]
fn get_siglen(f: &Option<&mut FmgrInfo>) -> usize {
    match f.as_ref().and_then(|f| f.opclass_options()) {
        Some(img) => i32::from_ne_bytes(
            img[GIST_HSTORE_OPTIONS_SIGLEN_OFF..GIST_HSTORE_OPTIONS_SIGLEN_OFF + 4]
                .try_into()
                .unwrap(),
        ) as usize,
        None => SIGLEN_DEFAULT,
    }
}

// SAFETY: gist fmgr protocol — arg i is a live GISTENTRY pointer.
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (e as *const GISTENTRY).cast::<u8>(),
            core::mem::size_of::<GISTENTRY>(),
        )
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

pub(crate) fn detoasted_image<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        } else if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            let src = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let total = 4 + src.len();
            let mut buf: mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
            mcx::vec_append_bytes(&mut buf, &varatt::set_varsize_4b_word(total as u32).to_ne_bytes())?;
            mcx::vec_append_bytes(&mut buf, src)?;
            let out = core::slice::from_raw_parts(buf.as_ptr(), buf.len());
            core::mem::forget(buf);
            Ok(out)
        } else {
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let flat = detoast::detoast_attr(mcx, raw)?;
            let out = core::slice::from_raw_parts(flat.as_ptr(), flat.len());
            core::mem::forget(flat);
            Ok(out)
        }
    }
}

fn key_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: ghstore keys are plain 4B-header images built by this module,
    // possibly with trailing MAXALIGN padding (decode bounds by VARSIZE).
    unsafe { core::slice::from_raw_parts(p, varatt::varsize_4b(p)) }
}

fn image_result(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

pub fn fc_ghstore_in(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot accept a value of type ghstore").into())
}

pub fn fc_ghstore_out(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot display a value of type ghstore").into())
}

pub fn fc_ghstore_options(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0.
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(GIST_HSTORE_OPTIONS_SIZE);
    relopts.add_int(
        "siglen",
        SIGLEN_DEFAULT as i32,
        1,
        SIGLEN_MAX,
        GIST_HSTORE_OPTIONS_SIGLEN_OFF,
    );
    Ok(Datum::from_usize(0))
}

pub fn fc_ghstore_compress(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f);
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if entry.leafkey {
        // SAFETY: the armed result mcx outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let img = detoasted_image(mcx, entry.key)?;
        let val = HstoreView::from_vardata(&img[4..]);
        let mut sign = vec![0u8; siglen];
        for i in 0..val.count() {
            hash(&mut sign, crc32_sz(val.key(i)), siglen);
            if !val.val_isnull(i) {
                hash(&mut sign, crc32_sz(val.val(i)), siglen);
            }
        }
        let res = ghstore_alloc(false, siglen, Some(&sign));
        let key = image_result(fcinfo, &res)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }
    if let GhKey::Sign(sign) = decode_key(key_image(entry.key))? {
        if !sign.is_empty() && sign.iter().all(|&b| b == 0xff) {
            let res = ghstore_alloc(true, siglen, None);
            let key = image_result(fcinfo, &res)?;
            let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
            return entry_result(fcinfo, &retval);
        }
    }
    Ok(fcinfo.arg(0))
}

// ghstore isn't toastable: identity.
pub fn fc_ghstore_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

pub fn fc_ghstore_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = decode_key(key_image(fcinfo.arg(0)))?;
    let b = decode_key(key_image(fcinfo.arg(1)))?;
    let siglen = get_siglen(&f);
    let same = match (&a, &b) {
        (GhKey::AllTrue, GhKey::AllTrue) => true,
        (GhKey::AllTrue, _) | (_, GhKey::AllTrue) => false,
        (GhKey::Sign(sa), GhKey::Sign(sb)) => sa[..siglen] == sb[..siglen],
    };
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = same };
    Ok(fcinfo.arg(2))
}

pub fn fc_ghstore_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let siglen = get_siglen(&f);
    let mut base = vec![0u8; siglen];
    let mut allistrue = false;
    for e in &entryvec.vector[..entryvec.n as usize] {
        match decode_key(key_image(e.key))? {
            GhKey::AllTrue => {
                allistrue = true;
                break;
            }
            GhKey::Sign(s) => {
                for i in 0..siglen {
                    base[i] |= s[i];
                }
            }
        }
    }
    let img = if allistrue {
        ghstore_alloc(true, siglen, None)
    } else {
        ghstore_alloc(false, siglen, Some(&base))
    };
    let size_out = fcinfo.arg(1).as_usize() as *mut i32;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *size_out = img.len() as i32 };
    image_result(fcinfo, &img)
}

pub fn fc_ghstore_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let siglen = get_siglen(&f);
    let origval = decode_key(key_image(origentry.key))?;
    let newval = decode_key(key_image(newentry.key))?;
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *penalty = hemdist(&origval, &newval, siglen) as f32 };
    Ok(fcinfo.arg(2))
}

fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

fn hemdist_working(datum_allistrue: bool, union: &[u8], other: &GhKey, siglen: usize) -> i32 {
    if datum_allistrue {
        if other.is_alltrue() {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(sign_of(other))
        }
    } else if other.is_alltrue() {
        siglenbit(siglen) as i32 - sizebitvec(union)
    } else {
        hemdistsign(union, sign_of(other), siglen)
    }
}

pub fn fc_ghstore_picksplit(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let siglen = get_siglen(&f);

    let n = entryvec.n as usize;
    // C: seed search over 1..n-2, distribution over 1..=n-1.
    let seed_maxoff = n - 2;
    let mut keys: Vec<Option<GhKey>> = (0..n).map(|_| None).collect();
    for k in 1..n {
        keys[k] = Some(decode_key(key_image(entryvec.vector[k].key))?);
    }

    let mut waste = -1i32;
    let (mut seed_1, mut seed_2) = (0usize, 0usize);
    for k in 1..seed_maxoff {
        for j in (k + 1)..=seed_maxoff {
            let sw = hemdist(keys[k].as_ref().unwrap(), keys[j].as_ref().unwrap(), siglen);
            if sw > waste {
                waste = sw;
                seed_1 = k;
                seed_2 = j;
            }
        }
    }
    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    let maxoff = n - 1;
    let datum_l_allistrue = keys[seed_1].as_ref().unwrap().is_alltrue();
    let datum_r_allistrue = keys[seed_2].as_ref().unwrap().is_alltrue();
    let mut union_l: Vec<u8> = match keys[seed_1].as_ref().unwrap() {
        GhKey::Sign(s) => s.clone(),
        GhKey::AllTrue => vec![0u8; siglen],
    };
    let mut union_r: Vec<u8> = match keys[seed_2].as_ref().unwrap() {
        GhKey::Sign(s) => s.clone(),
        GhKey::AllTrue => vec![0u8; siglen],
    };

    let mut costvector: Vec<(usize, i32)> = Vec::with_capacity(maxoff);
    for j in 1..=maxoff {
        let kj = keys[j].as_ref().unwrap();
        let size_alpha = hemdist_working(datum_l_allistrue, &union_l, kj, siglen);
        let size_beta = hemdist_working(datum_r_allistrue, &union_r, kj, siglen);
        costvector.push((j, (size_alpha - size_beta).abs()));
    }
    costvector.sort_by(|a, b| a.1.cmp(&b.1));

    let mut spl_left: Vec<u16> = Vec::new();
    let mut spl_right: Vec<u16> = Vec::new();
    for (j, _cost) in costvector {
        if j == seed_1 {
            spl_left.push(j as u16);
            continue;
        }
        if j == seed_2 {
            spl_right.push(j as u16);
            continue;
        }
        let kj = keys[j].as_ref().unwrap();
        let size_alpha = hemdist_working(datum_l_allistrue, &union_l, kj, siglen);
        let size_beta = hemdist_working(datum_r_allistrue, &union_r, kj, siglen);
        if (size_alpha as f64)
            < size_beta as f64 + wish_f(spl_left.len() as i32, spl_right.len() as i32, 0.0001)
        {
            if datum_l_allistrue || kj.is_alltrue() {
                if !datum_l_allistrue {
                    union_l.fill(0xff);
                }
            } else {
                let ptr = sign_of(kj);
                for i in 0..siglen {
                    union_l[i] |= ptr[i];
                }
            }
            spl_left.push(j as u16);
        } else {
            if datum_r_allistrue || kj.is_alltrue() {
                if !datum_r_allistrue {
                    union_r.fill(0xff);
                }
            } else {
                let ptr = sign_of(kj);
                for i in 0..siglen {
                    union_r[i] |= ptr[i];
                }
            }
            spl_right.push(j as u16);
        }
    }

    let ldatum = ghstore_alloc(datum_l_allistrue, siglen, Some(&union_l));
    let rdatum = ghstore_alloc(datum_r_allistrue, siglen, Some(&union_r));
    v.spl_left = spl_left;
    v.spl_right = spl_right;
    v.spl_ldatum = image_result(fcinfo, &ldatum)?;
    v.spl_rdatum = image_result(fcinfo, &rdatum)?;
    Ok(fcinfo.arg(1))
}

pub fn fc_ghstore_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    // All cases served by this opclass are inexact.
    let recheck_out = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck_out = true };

    let entry_key = decode_key(key_image(entry.key))?;
    if entry_key.is_alltrue() {
        return Ok(Datum::from_bool(true));
    }
    let sign = sign_of(&entry_key);
    let siglen = sign.len();

    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let query_image = detoasted_image(mcx, fcinfo.arg(1))?;

    let res = match strategy {
        HSTORE_CONTAINS_STRATEGY | HSTORE_OLD_CONTAINS_STRATEGY => {
            let query = HstoreView::from_vardata(&query_image[4..]);
            (0..query.count()).all(|i| {
                getbit(sign, hashval(crc32_sz(query.key(i)), siglen))
                    && (query.val_isnull(i)
                        || getbit(sign, hashval(crc32_sz(query.val(i)), siglen)))
            })
        }
        HSTORE_EXISTS_STRATEGY => {
            getbit(sign, hashval(crc32_sz(&query_image[4..]), siglen))
        }
        HSTORE_EXISTS_ALL_STRATEGY => {
            let scratch = mcx::MemoryContext::new("ghstore consistent text[]");
            let keys = crate::deconstruct_text_array(scratch.mcx(), query_image)?;
            keys.iter()
                .flatten()
                .all(|k| getbit(sign, hashval(crc32_sz(k), siglen)))
        }
        HSTORE_EXISTS_ANY_STRATEGY => {
            let scratch = mcx::MemoryContext::new("ghstore consistent text[]");
            let keys = crate::deconstruct_text_array(scratch.mcx(), query_image)?;
            keys.iter()
                .flatten()
                .any(|k| getbit(sign, hashval(crc32_sz(k), siglen)))
        }
        other => {
            return Err(PgError::error(format!("Unsupported strategy number: {other}")).into())
        }
    };
    Ok(Datum::from_bool(res))
}
