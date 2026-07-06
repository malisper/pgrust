//! tsgistidx.c: GiST support functions for tsvector_ops. The index key is a
//! SignTSVector varlena ridden byte-faithfully: 4B header, int32 flag, then
//! sorted lexeme-CRC int32s (ARRKEY, leaf) or a bit signature (SIGNKEY),
//! with an ALLISTRUE shortcut carrying no data.

use ::adt_tsvector_core::execute::{ts_execute, ExecPhraseData, Ternary, TS_EXEC_PHRASE_NO_POS};
use ::adt_tsvector_core::layout::TsVec;
use ::adt_tsvector_core::query::{Operand, TsQueryRef};
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use ::types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};
use ::types_scan::scankey::{RTContainedByStrategyNumber, RTContainsStrategyNumber};
use ::types_tuple::varatt;

#[cfg(test)]
mod tests;

pub const SIGLEN_DEFAULT: usize = 31 * 4;

pub const ARRKEY: i32 = 0x01;
pub const SIGNKEY: i32 = 0x02;
pub const ALLISTRUE: i32 = 0x04;

const GTHDRSIZE: usize = 8;
const TOAST_INDEX_TARGET: usize = ::types_storage::bufpage::MaxHeapTupleSize / 16;

// GISTMaxIndexKeySize bound; the regress-visible "between 1 and 2024" detail
// pins the value.
pub const SIGLEN_MAX: usize = 2024;
// GistTsVectorOptions: int32 vl_len_ then int siglen (offset 4, size 8).
const GISTTSVECTOROPTIONS_SIZE: usize = 8;
const GISTTSVECTOROPTIONS_SIGLEN_OFF: usize = 4;

// C GET_SIGLEN(): PG_HAS_OPCLASS_OPTIONS ? options->siglen : SIGLEN_DEFAULT.
#[inline]
fn get_siglen(f: &Option<&mut FmgrInfo>) -> usize {
    match f.as_ref().and_then(|f| f.opclass_options()) {
        Some(img) => i32::from_ne_bytes(
            img[GISTTSVECTOROPTIONS_SIGLEN_OFF..GISTTSVECTOROPTIONS_SIGLEN_OFF + 4]
                .try_into()
                .unwrap(),
        ) as usize,
        None => SIGLEN_DEFAULT,
    }
}

const fn calcgtsize(flag: i32, len: usize) -> usize {
    GTHDRSIZE
        + if flag & ARRKEY != 0 {
            len * 4
        } else if flag & ALLISTRUE != 0 {
            0
        } else {
            len
        }
}

/// Borrowed SignTSVector image (full 4B-header varlena, as stored).
#[derive(Clone, Copy)]
pub struct GtsRef<'a> {
    pub image: &'a [u8],
}

impl<'a> GtsRef<'a> {
    /// # Safety
    /// `d` points at a live plain (4B-header, untoasted) SignTSVector image.
    pub unsafe fn at(d: Datum) -> GtsRef<'a> {
        let p = d.as_usize() as *const u8;
        debug_assert!(unsafe { varatt::varatt_is_4b_u(p) });
        GtsRef {
            image: unsafe { core::slice::from_raw_parts(p, varatt::varsize_4b(p)) },
        }
    }

    pub fn flag(self) -> i32 {
        i32::from_ne_bytes(self.image[4..8].try_into().unwrap())
    }
    pub fn is_arrkey(self) -> bool {
        self.flag() & ARRKEY != 0
    }
    pub fn is_signkey(self) -> bool {
        self.flag() & SIGNKEY != 0
    }
    pub fn is_alltrue(self) -> bool {
        self.flag() & ALLISTRUE != 0
    }
    pub fn siglen(self) -> usize {
        self.image.len() - GTHDRSIZE
    }
    pub fn sign(self) -> &'a [u8] {
        &self.image[GTHDRSIZE..]
    }
    pub fn arrnelem(self) -> usize {
        (self.image.len() - GTHDRSIZE) / 4
    }
    pub fn arr_at(self, i: usize) -> i32 {
        let off = GTHDRSIZE + i * 4;
        i32::from_ne_bytes(self.image[off..off + 4].try_into().unwrap())
    }
}

struct GtsBuilder<'m> {
    img: PgVec<'m, u8>,
}

impl<'m> GtsBuilder<'m> {
    fn alloc_empty(mcx: Mcx<'m>, flag: i32, size: usize) -> PgResult<Self> {
        let mut img: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, size)?;
        mcx::vec_append_bytes(&mut img, &varatt::set_varsize_4b_word(size as u32).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut img, &flag.to_ne_bytes())?;
        Ok(GtsBuilder { img })
    }

    fn alloc(mcx: Mcx<'m>, flag: i32, len: usize, sign: Option<&[u8]>) -> PgResult<Self> {
        let size = calcgtsize(flag, len);
        let mut img: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, size)?;
        mcx::vec_append_bytes(&mut img, &varatt::set_varsize_4b_word(size as u32).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut img, &flag.to_ne_bytes())?;
        if flag & (SIGNKEY | ALLISTRUE) == SIGNKEY {
            match sign {
                Some(s) => mcx::vec_append_bytes(&mut img, &s[..len])?,
                None => img.resize(size, 0),
            }
        } else {
            img.resize(size, 0);
        }
        Ok(GtsBuilder { img })
    }

    fn set_flag(&mut self, flag: i32) {
        self.img[4..8].copy_from_slice(&flag.to_ne_bytes());
    }

    fn flag(&self) -> i32 {
        i32::from_ne_bytes(self.img[4..8].try_into().unwrap())
    }

    fn sign_mut(&mut self) -> &mut [u8] {
        &mut self.img[GTHDRSIZE..]
    }

    fn truncate_to(&mut self, size: usize) {
        self.img.truncate(size);
        self.img[0..4].copy_from_slice(&varatt::set_varsize_4b_word(size as u32).to_ne_bytes());
    }

    fn finish(self) -> Datum {
        varlena_result(::datum::Varlena::from_image(self.img))
    }
}

const SIGLENBIT: fn(usize) -> i32 = |siglen| (siglen * 8) as i32;

#[inline]
fn hashval(val: i32, siglen: usize) -> usize {
    (val as u32 as usize) % (siglen * 8)
}

#[inline]
fn hash(sign: &mut [u8], val: i32, siglen: usize) {
    let i = hashval(val, siglen);
    sign[i / 8] |= 1 << (i % 8);
}

#[inline]
fn getbit(sign: &[u8], i: usize) -> bool {
    (sign[i / 8] >> (i % 8)) & 1 != 0
}

fn makesign_arr(sign: &mut [u8], arr: &[i32], siglen: usize) {
    sign[..siglen].fill(0);
    for &v in arr {
        hash(sign, v, siglen);
    }
}

fn makesign(sign: &mut [u8], a: GtsRef<'_>, siglen: usize) {
    sign[..siglen].fill(0);
    for k in 0..a.arrnelem() {
        hash(sign, a.arr_at(k), siglen);
    }
}

fn sizebitvec(sign: &[u8], siglen: usize) -> i32 {
    sign[..siglen].iter().map(|b| b.count_ones() as i32).sum()
}

fn hemdistsign(a: &[u8], b: &[u8], siglen: usize) -> i32 {
    let mut dist = 0i32;
    for i in 0..siglen {
        dist += (a[i] ^ b[i]).count_ones() as i32;
    }
    dist
}

fn hemdist(a: GtsRef<'_>, b: GtsRef<'_>) -> i32 {
    if a.is_alltrue() {
        if b.is_alltrue() {
            0
        } else {
            SIGLENBIT(b.siglen()) - sizebitvec(b.sign(), b.siglen())
        }
    } else if b.is_alltrue() {
        SIGLENBIT(a.siglen()) - sizebitvec(a.sign(), a.siglen())
    } else {
        debug_assert!(a.siglen() == b.siglen());
        hemdistsign(a.sign(), b.sign(), a.siglen())
    }
}

// SAFETY contract shared by the gist fmgr protocol helpers below: arg
// pointers live for the duration of the call.
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    // SAFETY: plain-old-data GISTENTRY copied out byte-wise.
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (e as *const GISTENTRY).cast::<u8>(),
            core::mem::size_of::<GISTENTRY>(),
        )
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

fn detoasted_image<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        } else if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            // Expand short headers to a 4B image (payload alignment).
            let src = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let total = 4 + src.len();
            let mut buf: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
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

fn fc_gtsvectorin(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot accept a value of type gtsvector")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into())
}

fn fc_gtsvectorout(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let img = detoasted_image(mcx, fcinfo.arg(0))?;
    let key = GtsRef { image: img };
    let s = if key.is_arrkey() {
        format!("{} unique words", key.arrnelem())
    } else if key.is_alltrue() {
        "all true bits".to_string()
    } else {
        let siglen = key.siglen();
        let cnttrue = sizebitvec(key.sign(), siglen);
        format!("{} true bits, {} false bits", cnttrue, SIGLENBIT(siglen) - cnttrue)
    };
    let mut out: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
    mcx::vec_append_bytes(&mut out, s.as_bytes())?;
    out.push(0);
    Ok(cstring_result(out))
}

fn fc_gtsvector_compress(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let siglen = get_siglen(&f);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    if entry.leafkey {
        let img = detoasted_image(mcx, entry.key)?;
        let val = TsVec { payload: &img[4..] };
        let n = val.size();
        // qsort + qunique over the legacy-CRC hashes.
        let mut arr: PgVec<'_, i32> = mcx::vec_with_capacity_in(mcx, n)?;
        for i in 0..n {
            let we = val.entry(i);
            arr.push(::crc32c::legacy_crc32_lexeme(val.lexeme(we)) as i32);
        }
        arr.sort_unstable();
        let mut len = 0usize;
        for i in 0..n {
            if i == 0 || arr[i] != arr[len - 1] {
                arr[len] = arr[i];
                len += 1;
            }
        }

        let res = if calcgtsize(ARRKEY, len) > TOAST_INDEX_TARGET {
            let mut ressign = GtsBuilder::alloc(mcx, SIGNKEY, siglen, None)?;
            makesign_arr(&mut ressign.img[GTHDRSIZE..], &arr[..len], siglen);
            ressign
        } else {
            let size = calcgtsize(ARRKEY, len);
            let mut b = GtsBuilder::alloc_empty(mcx, ARRKEY, size)?;
            for v in &arr[..len] {
                mcx::vec_append_bytes(&mut b.img, &v.to_ne_bytes())?;
            }
            b
        };

        let key = res.finish();
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        entry_result(fcinfo, &retval)
    } else {
        // SAFETY: stored keys are plain SignTSVector images.
        let key = unsafe { GtsRef::at(entry.key) };
        if key.is_signkey() && !key.is_alltrue() {
            let sign = key.sign();
            for i in 0..siglen {
                if sign[i] != 0xff {
                    return Ok(fcinfo.arg(0));
                }
            }
            let res = GtsBuilder::alloc(mcx, SIGNKEY | ALLISTRUE, siglen, Some(sign))?;
            let retval = GISTENTRY::init(res.finish(), entry.offset, false, entry.page_is_leaf);
            entry_result(fcinfo, &retval)
        } else {
            Ok(fcinfo.arg(0))
        }
    }
}

fn fc_gtsvector_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let p = entry.key.as_usize() as *const u8;
    // SAFETY: non-null varlena key readable through its header.
    if unsafe { varatt::varatt_is_4b_u(p) } {
        return Ok(fcinfo.arg(0));
    }
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let img = detoasted_image(mcx, entry.key)?;
    let retval = GISTENTRY::init(
        Datum::from_usize(img.as_ptr() as usize),
        entry.offset,
        false,
        entry.page_is_leaf,
    );
    entry_result(fcinfo, &retval)
}

fn checkcondition_arr(key: GtsRef<'_>, val: &Operand) -> Ternary {
    if val.prefix {
        return Ternary::Maybe;
    }
    let mut lo = 0usize;
    let mut hi = key.arrnelem();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let v = key.arr_at(mid);
        if v == val.valcrc {
            return Ternary::Maybe;
        } else if v < val.valcrc {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ternary::No
}

fn checkcondition_bit(key: GtsRef<'_>, val: &Operand) -> Ternary {
    if val.prefix {
        return Ternary::Maybe;
    }
    if getbit(key.sign(), hashval(val.valcrc, key.siglen())) {
        Ternary::Maybe
    } else {
        Ternary::No
    }
}

pub fn gtsvector_consistent_core(
    mcx: Mcx<'_>,
    key: GtsRef<'_>,
    query: TsQueryRef<'_>,
) -> PgResult<bool> {
    if query.size() == 0 {
        return Ok(false);
    }
    if key.is_signkey() {
        if key.is_alltrue() {
            return Ok(true);
        }
        let mut chk = |_i: usize, val: &Operand, _d: Option<&mut ExecPhraseData<'_>>| {
            Ok(checkcondition_bit(key, val))
        };
        ts_execute(mcx, query, TS_EXEC_PHRASE_NO_POS, &mut chk)
    } else {
        let mut chk = |_i: usize, val: &Operand, _d: Option<&mut ExecPhraseData<'_>>| {
            Ok(checkcondition_arr(key, val))
        };
        ts_execute(mcx, query, TS_EXEC_PHRASE_NO_POS, &mut chk)
    }
}

fn fc_gtsvector_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let qimg = detoasted_image(mcx, fcinfo.arg(1))?;
    let query = TsQueryRef { payload: &qimg[4..] };
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame; all lanes inexact.
    unsafe { *recheck = true };
    // SAFETY: decompressed key is a plain SignTSVector image.
    let key = unsafe { GtsRef::at(entry.key) };
    Ok(Datum::from_bool(gtsvector_consistent_core(mcx, key, query)?))
}

fn unionkey(sbase: &mut [u8], add: GtsRef<'_>, siglen: usize) -> bool {
    if add.is_signkey() {
        if add.is_alltrue() {
            return true;
        }
        debug_assert!(add.siglen() == siglen);
        let sadd = add.sign();
        for i in 0..siglen {
            sbase[i] |= sadd[i];
        }
    } else {
        for i in 0..add.arrnelem() {
            hash(sbase, add.arr_at(i), siglen);
        }
    }
    false
}

fn fc_gtsvector_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let siglen = get_siglen(&f);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut result = GtsBuilder::alloc(mcx, SIGNKEY, siglen, None)?;

    for e in &entryvec.vector[..entryvec.n as usize] {
        // SAFETY: keys are plain SignTSVector images.
        let add = unsafe { GtsRef::at(e.key) };
        if unionkey(&mut result.sign_mut()[..siglen], add, siglen) {
            let flag = result.flag() | ALLISTRUE;
            result.set_flag(flag);
            result.truncate_to(calcgtsize(flag, siglen));
            break;
        }
    }

    let size_out = fcinfo.arg(1).as_usize() as *mut i32;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *size_out = result.img.len() as i32 };
    Ok(result.finish())
}

fn fc_gtsvector_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol; args 0/1 are key datums.
    let a = unsafe { GtsRef::at(fcinfo.arg(0)) };
    let b = unsafe { GtsRef::at(fcinfo.arg(1)) };
    let siglen = get_siglen(&f);
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    let r = if a.is_signkey() {
        if a.is_alltrue() || b.is_alltrue() {
            a.is_alltrue() && b.is_alltrue()
        } else {
            debug_assert!(a.siglen() == siglen && b.siglen() == siglen);
            a.sign()[..siglen] == b.sign()[..siglen]
        }
    } else {
        a.image[GTHDRSIZE..] == b.image[GTHDRSIZE..]
    };
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = r };
    Ok(fcinfo.arg(2))
}

fn fc_gtsvector_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    let siglen = get_siglen(&f);
    // SAFETY: keys are plain SignTSVector images.
    let origval = unsafe { GtsRef::at(origentry.key) };
    let newval = unsafe { GtsRef::at(newentry.key) };

    let p: f32 = if newval.is_arrkey() {
        // SAFETY: the armed result mcx outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let mut sign: PgVec<'_, u8> = mcx::vec_from_elem_in(mcx, 0u8, siglen);
        makesign(&mut sign, newval, siglen);
        if origval.is_alltrue() {
            let siglenbit = SIGLENBIT(siglen);
            (siglenbit - sizebitvec(&sign, siglen)) as f32 / (siglenbit + 1) as f32
        } else {
            hemdistsign(&sign, origval.sign(), siglen) as f32
        }
    } else {
        hemdist(origval, newval) as f32
    };
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *penalty = p };
    Ok(fcinfo.arg(2))
}

// CACHESIGN as C lays it out: one flat sign buffer + per-slot allistrue.
struct SignCache<'m> {
    alltrue: PgVec<'m, bool>,
    signs: PgVec<'m, u8>,
    siglen: usize,
}

impl<'m> SignCache<'m> {
    fn new(mcx: Mcx<'m>, nslots: usize, siglen: usize) -> Self {
        SignCache {
            alltrue: mcx::vec_from_elem_in(mcx, false, nslots),
            signs: mcx::vec_from_elem_in(mcx, 0u8, nslots * siglen),
            siglen,
        }
    }

    fn sign(&self, i: usize) -> &[u8] {
        &self.signs[i * self.siglen..(i + 1) * self.siglen]
    }

    fn fill(&mut self, i: usize, key: GtsRef<'_>) {
        let siglen = self.siglen;
        let slot = &mut self.signs[i * siglen..(i + 1) * siglen];
        self.alltrue[i] = false;
        if key.is_arrkey() {
            makesign(slot, key, siglen);
        } else if key.is_alltrue() {
            self.alltrue[i] = true;
        } else {
            slot.copy_from_slice(&key.sign()[..siglen]);
        }
    }

    fn hemdist(&self, a: usize, b: usize) -> i32 {
        if self.alltrue[a] {
            if self.alltrue[b] {
                0
            } else {
                SIGLENBIT(self.siglen) - sizebitvec(self.sign(b), self.siglen)
            }
        } else if self.alltrue[b] {
            SIGLENBIT(self.siglen) - sizebitvec(self.sign(a), self.siglen)
        } else {
            hemdistsign(self.sign(a), self.sign(b), self.siglen)
        }
    }
}

#[inline]
fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    -((((a - b) * (a - b) * (a - b)) as f64) * c)
}

fn fc_gtsvector_picksplit(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let siglen = get_siglen(&f);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: keys are plain SignTSVector images.
    let key_at = |pos: usize| unsafe { GtsRef::at(entryvec.vector[pos].key) };

    let mut maxoff = (entryvec.n - 2) as usize;
    let mut cache = SignCache::new(mcx, maxoff + 2, siglen);

    cache.fill(1, key_at(1));

    let mut seed_1 = 0usize;
    let mut seed_2 = 0usize;
    let mut waste = -1i32;
    for k in 1..maxoff {
        for j in k + 1..=maxoff {
            if k == 1 {
                cache.fill(j, key_at(j));
            }
            let size_waste = cache.hemdist(j, k);
            if size_waste > waste {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
        }
    }

    v.spl_left = Vec::with_capacity(maxoff + 1);
    v.spl_right = Vec::with_capacity(maxoff + 1);

    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    // Running union sides; ALLISTRUE arises only via an alltrue seed.
    let l_alltrue = cache.alltrue[seed_1];
    let r_alltrue = cache.alltrue[seed_2];
    let mut sign_l: PgVec<'_, u8> = mcx::vec_from_elem_in(mcx, 0u8, siglen);
    let mut sign_r: PgVec<'_, u8> = mcx::vec_from_elem_in(mcx, 0u8, siglen);
    if !l_alltrue {
        sign_l.copy_from_slice(cache.sign(seed_1));
    }
    if !r_alltrue {
        sign_r.copy_from_slice(cache.sign(seed_2));
    }

    maxoff += 1;
    cache.fill(maxoff, key_at(maxoff));

    #[derive(Clone, Copy)]
    struct SplitCost {
        pos: u16,
        cost: i32,
    }
    let mut costvector: PgVec<'_, SplitCost> = mcx::vec_with_capacity_in(mcx, maxoff)?;
    for j in 1..=maxoff {
        let size_alpha = cache.hemdist(seed_1, j);
        let size_beta = cache.hemdist(seed_2, j);
        costvector.push(SplitCost { pos: j as u16, cost: (size_alpha - size_beta).abs() });
    }
    costvector.sort_unstable_by_key(|s| s.cost);

    for k in 0..maxoff {
        let j = costvector[k].pos as usize;
        if j == seed_1 {
            v.spl_left.push(j as u16);
            continue;
        } else if j == seed_2 {
            v.spl_right.push(j as u16);
            continue;
        }

        let side_dist = |alltrue: bool, sign: &[u8]| -> i32 {
            if alltrue || cache.alltrue[j] {
                if alltrue && cache.alltrue[j] {
                    0
                } else {
                    let from: &[u8] = if cache.alltrue[j] { sign } else { cache.sign(j) };
                    SIGLENBIT(siglen) - sizebitvec(from, siglen)
                }
            } else {
                hemdistsign(cache.sign(j), sign, siglen)
            }
        };
        let size_alpha = side_dist(l_alltrue, &sign_l);
        let size_beta = side_dist(r_alltrue, &sign_r);

        let merge = |alltrue: bool, sign: &mut PgVec<'_, u8>| {
            if alltrue || cache.alltrue[j] {
                if !alltrue {
                    sign[..siglen].fill(0xff);
                }
            } else {
                let src = cache.sign(j);
                for i in 0..siglen {
                    sign[i] |= src[i];
                }
            }
        };

        if (size_alpha as f64)
            < size_beta as f64 + wish_f(v.spl_left.len() as i32, v.spl_right.len() as i32, 0.1)
        {
            merge(l_alltrue, &mut sign_l);
            v.spl_left.push(j as u16);
        } else {
            merge(r_alltrue, &mut sign_r);
            v.spl_right.push(j as u16);
        }
    }

    let build = |alltrue: bool, sign: &[u8]| -> PgResult<Datum> {
        let flag = SIGNKEY | if alltrue { ALLISTRUE } else { 0 };
        let b = GtsBuilder::alloc(mcx, flag, siglen, Some(sign))?;
        Ok(b.finish())
    };
    v.spl_ldatum = build(l_alltrue, &sign_l)?;
    v.spl_rdatum = build(r_alltrue, &sign_r)?;

    Ok(fcinfo.arg(1))
}

#[cold]
fn fc_gtsvector_options(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0 (C
    // PG_GETARG_POINTER protocol).
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(GISTTSVECTOROPTIONS_SIZE);
    relopts.add_int(
        "siglen",
        SIGLEN_DEFAULT as i32,
        1,
        SIGLEN_MAX as i32,
        GISTTSVECTOROPTIONS_SIGLEN_OFF,
    );
    Ok(Datum::from_usize(0))
}

// tsquery_op.c makeTSQuerySign: TSQS_SIGLEN = 64.
pub fn make_tsquery_sign(q: TsQueryRef<'_>) -> u64 {
    let mut sign: u64 = 0;
    for i in 0..q.size() {
        if let ::adt_tsvector_core::query::Item::Val(op) = q.item(i) {
            sign |= 1u64 << ((op.valcrc as u32) % 64);
        }
    }
    sign
}

// tsquery_gist.c gtsquery_compress; the stored key is the bare TSQuerySign
// int8 datum, no decompress proc.
fn fc_gtsquery_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if !entry.leafkey {
        return Ok(fcinfo.arg(0));
    }
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let img = detoasted_image(mcx, entry.key)?;
    let sign = make_tsquery_sign(TsQueryRef { payload: &img[4..] });
    let retval = GISTENTRY::init(
        Datum::from_u64(sign),
        entry.offset,
        false,
        entry.page_is_leaf,
    );
    entry_result(fcinfo, &retval)
}

// tsquery_gist.c gtsquery_consistent; keys are bare TSQuerySign int8 datums.
fn fc_gtsquery_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let qimg = detoasted_image(mcx, fcinfo.arg(1))?;
    let strategy = fcinfo.arg_i16(2) as u16;
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame; all lanes inexact.
    unsafe { *recheck = true };
    let key = entry.key.as_u64();
    let sq = make_tsquery_sign(TsQueryRef { payload: &qimg[4..] });
    let retval = if strategy == RTContainsStrategyNumber {
        if entry.page_is_leaf {
            (key & sq) == sq
        } else {
            (key & sq) != 0
        }
    } else if strategy == RTContainedByStrategyNumber {
        if entry.page_is_leaf {
            (key & sq) == key
        } else {
            (key & sq) != 0
        }
    } else {
        false
    };
    Ok(Datum::from_bool(retval))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const TSGISTIDX_BUILTINS: &[FmgrBuiltin] = &[
    b(3434, "gtsvector_options", 1, fc_gtsvector_options),
    b(3646, "gtsvectorin", 1, fc_gtsvectorin),
    b(3647, "gtsvectorout", 1, fc_gtsvectorout),
    b(3648, "gtsvector_compress", 1, fc_gtsvector_compress),
    b(3649, "gtsvector_decompress", 1, fc_gtsvector_decompress),
    b(3650, "gtsvector_picksplit", 2, fc_gtsvector_picksplit),
    b(3651, "gtsvector_union", 2, fc_gtsvector_union),
    b(3652, "gtsvector_same", 3, fc_gtsvector_same),
    b(3653, "gtsvector_penalty", 3, fc_gtsvector_penalty),
    b(3654, "gtsvector_consistent", 5, fc_gtsvector_consistent),
    b(3695, "gtsquery_compress", 1, fc_gtsquery_compress),
    b(3701, "gtsquery_consistent", 5, fc_gtsquery_consistent),
    b(3790, "gtsvector_consistent_oldsig", 5, fc_gtsvector_consistent),
    b(3793, "gtsquery_consistent_oldsig", 5, fc_gtsquery_consistent),
];
