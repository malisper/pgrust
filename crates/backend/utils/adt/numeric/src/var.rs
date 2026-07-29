use core::cell::RefCell;

use types_error::PgResult;

use crate::{
    numeric_can_be_short, numeric_overflow_error, Num, NumericDigit, DEC_DIGITS, HALF_NBASE,
    NBASE, NUMERIC_DSCALE_MASK, NUMERIC_HDRSZ, NUMERIC_HDRSZ_SHORT, NUMERIC_NAN, NUMERIC_NEG,
    NUMERIC_NINF, NUMERIC_PINF, NUMERIC_POS, NUMERIC_SHORT, NUMERIC_SHORT_DSCALE_SHIFT,
    NUMERIC_SHORT_SIGN_MASK, NUMERIC_SHORT_WEIGHT_MASK, NUMERIC_SHORT_WEIGHT_SIGN_MASK,
    NUMERIC_SIGN_MASK, NUMERIC_SPECIAL, VARHDRSZ,
};

pub const ROUND_POWERS: [i32; 4] = [0, 1000, 100, 10];

// C's digitbuf_alloc/free are palloc/pfree per operation; the TLS freelist
// keeps retained capacity instead (rule 7). std Vec is deliberate: this is
// backend-thread scratch outside any mcx's accounting, same as C's
// per-operation palloc churn that never survives the call.
std::thread_local! {
    static DIGIT_POOL: RefCell<Vec<Vec<NumericDigit>>> = const { RefCell::new(Vec::new()) };
    static WORD_POOL: RefCell<Vec<Vec<u16>>> = const { RefCell::new(Vec::new()) };
}

const DIGIT_POOL_SLOTS: usize = 16;

// pub for proofs/numeric-probe (Kani stubs the TLS pool: thread_local
// destructor registration reaches `_tlv_atexit`, a Kani-unsupported symbol).
pub fn word_buf_take() -> Vec<u16> {
    WORD_POOL
        .with(|p| p.borrow_mut().pop())
        .unwrap_or_default()
}

// pub for proofs/numeric-probe (see word_buf_take).
pub fn word_buf_put(v: Vec<u16>) {
    if v.capacity() == 0 {
        return;
    }
    WORD_POOL.with(|p| {
        let mut p = p.borrow_mut();
        if p.len() < DIGIT_POOL_SLOTS {
            p.push(v);
        }
    });
}

// Covers every value the packed format can produce for typical OLTP widths
// (numeric_in 40+10 decimal digits, mul 40x40, div) without touching the heap
// — a structural win over C, whose NumericVar always pallocs its buf.
pub(crate) const INLINE_DIGITS: usize = 36;

pub(crate) struct DigitBuf {
    len: u32,
    inline: core::mem::MaybeUninit<[NumericDigit; INLINE_DIGITS]>,
    heap: Vec<NumericDigit>,
}

impl DigitBuf {
    pub fn empty() -> DigitBuf {
        DigitBuf {
            len: 0,
            inline: core::mem::MaybeUninit::uninit(),
            heap: Vec::new(),
        }
    }

    // C's digitbuf_alloc: contents uninitialized; callers write every live
    // digit before reading (round's carry walk only reaches written spares).
    // In place: no by-value moves of the inline array, and a heap buffer once
    // acquired is retained across reallocations (rule 7).
    pub fn realloc_uninit(&mut self, n: usize) {
        if n <= INLINE_DIGITS {
            self.len = n as u32;
            return;
        }
        if self.heap.capacity() < n {
            let mut v = DIGIT_POOL
                .with(|p| p.borrow_mut().pop())
                .unwrap_or_default();
            v.clear();
            v.reserve(n);
            self.heap = v;
        } else {
            self.heap.clear();
        }
        // SAFETY: capacity ensured; i16 has no invalid bit patterns and the
        // exposed prefix is written by the caller before any read.
        unsafe { self.heap.set_len(n) };
        self.len = n as u32;
    }

    #[inline]
    pub fn as_slice(&self) -> &[NumericDigit] {
        let n = self.len as usize;
        if n <= INLINE_DIGITS {
            // SAFETY: the exposed prefix is written before reads (uninit contract).
            unsafe { core::slice::from_raw_parts(self.inline.as_ptr().cast(), n) }
        } else {
            &self.heap
        }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [NumericDigit] {
        let n = self.len as usize;
        if n <= INLINE_DIGITS {
            // SAFETY: as as_slice; i16 has no invalid bit patterns.
            unsafe { core::slice::from_raw_parts_mut(self.inline.as_mut_ptr().cast(), n) }
        } else {
            &mut self.heap
        }
    }
}

// Drop is the pool-return guard (memory guard exception to the no-drop rule).
impl Drop for DigitBuf {
    fn drop(&mut self) {
        if self.heap.capacity() == 0 {
            return;
        }
        let v = core::mem::take(&mut self.heap);
        DIGIT_POOL.with(|p| {
            let mut p = p.borrow_mut();
            if p.len() < DIGIT_POOL_SLOTS {
                p.push(v);
            }
        });
    }
}

/// Borrowed operand form (C's `init_var_from_num` / const vars): header
/// fields plus a digit slice that may alias a packed image.
#[derive(Clone, Copy, Debug)]
pub struct VarView<'a> {
    pub ndigits: i32,
    pub weight: i32,
    pub sign: u16,
    pub dscale: i32,
    pub digits: &'a [NumericDigit],
}

pub const CONST_ZERO: VarView<'static> = VarView {
    ndigits: 0,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 0,
    digits: &[],
};

pub const CONST_ONE: VarView<'static> = VarView {
    ndigits: 1,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 0,
    digits: &[1],
};

pub(crate) const CONST_MINUS_ONE: VarView<'static> = VarView {
    ndigits: 1,
    weight: 0,
    sign: NUMERIC_NEG,
    dscale: 0,
    digits: &[1],
};

pub(crate) const CONST_TWO: VarView<'static> = VarView {
    ndigits: 1,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 0,
    digits: &[2],
};

// DEC_DIGITS == 4 digit encodings (C's #if ladder).
pub(crate) const CONST_ZERO_POINT_NINE: VarView<'static> = VarView {
    ndigits: 1,
    weight: -1,
    sign: NUMERIC_POS,
    dscale: 1,
    digits: &[9000],
};

pub(crate) const CONST_ONE_POINT_ONE: VarView<'static> = VarView {
    ndigits: 2,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 1,
    digits: &[1, 1000],
};

/// Owned working form (C's NumericVar with a palloc'd buf). Digits live at
/// `buf[offset .. offset+ndigits]`; `offset >= 1` whenever a buffer is
/// allocated, so one spare zero digit sits below for rounding carry-out.
pub struct NumericVar {
    pub ndigits: i32,
    pub weight: i32,
    pub sign: u16,
    pub dscale: i32,
    buf: DigitBuf,
    offset: u32,
}

impl NumericVar {
    pub fn new() -> NumericVar {
        NumericVar {
            ndigits: 0,
            weight: 0,
            sign: NUMERIC_POS,
            dscale: 0,
            buf: DigitBuf::empty(),
            offset: 0,
        }
    }

    pub fn alloc(&mut self, ndigits: i32) {
        self.buf.realloc_uninit(ndigits as usize + 1);
        self.buf.as_mut_slice()[0] = 0;
        self.offset = 1;
        self.ndigits = ndigits;
    }

    pub fn set_zero(&mut self) {
        self.offset = 0;
        self.ndigits = 0;
        self.weight = 0;
        self.sign = NUMERIC_POS;
    }

    pub fn from_view(v: VarView<'_>) -> NumericVar {
        let mut var = NumericVar::new();
        var.set_from_view(v);
        var
    }

    pub fn set_from_view(&mut self, v: VarView<'_>) {
        self.buf.realloc_uninit(v.ndigits as usize + 1);
        let s = self.buf.as_mut_slice();
        s[0] = 0;
        s[1..].copy_from_slice(v.digits);
        self.offset = 1;
        self.ndigits = v.ndigits;
        self.weight = v.weight;
        self.sign = v.sign;
        self.dscale = v.dscale;
    }

    #[inline]
    pub fn digits(&self) -> &[NumericDigit] {
        &self.buf.as_slice()[self.offset as usize..self.offset as usize + self.ndigits as usize]
    }

    #[inline]
    pub fn digits_mut(&mut self) -> &mut [NumericDigit] {
        &mut self.buf.as_mut_slice()
            [self.offset as usize..self.offset as usize + self.ndigits as usize]
    }

    #[inline]
    pub fn view(&self) -> VarView<'_> {
        VarView {
            ndigits: self.ndigits,
            weight: self.weight,
            sign: self.sign,
            dscale: self.dscale,
            digits: self.digits(),
        }
    }

    pub fn round(&mut self, rscale: i32) {
        self.dscale = rscale;

        let mut di = (self.weight + 1) * DEC_DIGITS + rscale;
        if di < 0 {
            self.ndigits = 0;
            self.weight = 0;
            self.sign = NUMERIC_POS;
            return;
        }

        let mut ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;
        di %= DEC_DIGITS;

        if ndigits < self.ndigits || (ndigits == self.ndigits && di > 0) {
            debug_assert!(ndigits <= self.ndigits && self.offset >= 1);
            self.ndigits = ndigits;
            let off = self.offset as isize;
            // SAFETY throughout: accesses sit at off + i for -1 <= i <=
            // ndigits <= the old ndigits; offset >= 1 leaves one spare digit
            // below (alloc invariant) — C's round_var pointer walk verbatim.
            let buf = self.buf.as_mut_slice().as_mut_ptr();
            let mut carry: i32;

            unsafe {
                let at = |i: i32| buf.offset(off + i as isize);
                if di == 0 {
                    carry = if *at(ndigits) as i32 >= HALF_NBASE { 1 } else { 0 };
                } else {
                    let pow10 = ROUND_POWERS[di as usize];
                    ndigits -= 1;
                    let extra = *at(ndigits) as i32 % pow10;
                    *at(ndigits) -= extra as NumericDigit;
                    carry = 0;
                    if extra >= pow10 / 2 {
                        let mut p = pow10 + *at(ndigits) as i32;
                        if p >= NBASE {
                            p -= NBASE;
                            carry = 1;
                        }
                        *at(ndigits) = p as NumericDigit;
                    }
                }

                while carry != 0 {
                    ndigits -= 1;
                    let c = carry + *at(ndigits) as i32;
                    if c >= NBASE {
                        *at(ndigits) = (c - NBASE) as NumericDigit;
                        carry = 1;
                    } else {
                        *at(ndigits) = c as NumericDigit;
                        carry = 0;
                    }
                }
            }

            if ndigits < 0 {
                debug_assert!(ndigits == -1);
                debug_assert!(self.offset > 0);
                self.offset -= 1;
                self.ndigits += 1;
                self.weight += 1;
            }
        }
    }

    pub fn trunc(&mut self, rscale: i32) {
        self.dscale = rscale;

        let mut di = (self.weight + 1) * DEC_DIGITS + rscale;
        if di <= 0 {
            self.ndigits = 0;
            self.weight = 0;
            self.sign = NUMERIC_POS;
            return;
        }

        let mut ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;
        if ndigits <= self.ndigits {
            self.ndigits = ndigits;
            di %= DEC_DIGITS;
            if di > 0 {
                let off = self.offset as i32;
                let buf = self.buf.as_mut_slice();
                let pow10 = ROUND_POWERS[di as usize];
                ndigits -= 1;
                let extra = buf[(off + ndigits) as usize] as i32 % pow10;
                buf[(off + ndigits) as usize] -= extra as NumericDigit;
            }
        }
    }

    pub fn strip(&mut self) {
        let mut n = self.ndigits as usize;
        // SAFETY: reads stay in [offset, offset + ndigits), within the
        // allocation by the digits() invariant.
        let d = unsafe { self.buf.as_slice().as_ptr().add(self.offset as usize) };
        let mut start = 0usize;
        let mut weight_drop = 0;
        unsafe {
            while n > 0 && *d.add(start) == 0 {
                start += 1;
                weight_drop += 1;
                n -= 1;
            }
            while n > 0 && *d.add(start + n - 1) == 0 {
                n -= 1;
            }
        }
        self.weight -= weight_drop;
        if n == 0 {
            self.sign = NUMERIC_POS;
            self.weight = 0;
        }
        self.offset += start as u32;
        self.ndigits = n as i32;
    }
}

impl Default for NumericVar {
    fn default() -> Self {
        NumericVar::new()
    }
}

/// Owned packed numeric: the full varlena image (4-byte varlena header +
/// numeric payload). u16-backed so the digit array is always 2-byte aligned;
/// backing storage cycles through the TLS pool (C pallocs per result).
#[derive(Debug)]
pub struct NumericImage {
    words: Vec<u16>,
}

impl Clone for NumericImage {
    fn clone(&self) -> NumericImage {
        let mut words = word_buf_take();
        words.clear();
        words.extend_from_slice(&self.words);
        NumericImage { words }
    }
}

impl PartialEq for NumericImage {
    fn eq(&self, other: &NumericImage) -> bool {
        self.words == other.words
    }
}

impl Eq for NumericImage {}

// Pool-return guard (memory guard exception to the no-drop rule).
impl Drop for NumericImage {
    fn drop(&mut self) {
        word_buf_put(core::mem::take(&mut self.words));
    }
}

impl NumericImage {
    pub fn empty() -> NumericImage {
        NumericImage {
            words: word_buf_take(),
        }
    }

    // Callers write every payload word; only the varlena header is set here.
    // Retained capacity is reused (rule 7) — the engine's fc layer holds one
    // image per resolved call site, C pallocs per call.
    fn reset_payload_len(&mut self, payload: usize) {
        debug_assert!(payload % 2 == 0);
        let total = VARHDRSZ + payload;
        let n = total / 2;
        self.words.clear();
        self.words.reserve(n);
        // SAFETY: capacity reserved; u16 has no invalid bit patterns and all
        // words are written before any read (header here, payload by caller).
        unsafe { self.words.set_len(n) };
        let header = ((total as u32) << 2).to_ne_bytes();
        self.words[0] = u16::from_ne_bytes([header[0], header[1]]);
        self.words[1] = u16::from_ne_bytes([header[2], header[3]]);
    }

    fn with_payload_len(payload: usize) -> NumericImage {
        let mut img = NumericImage::empty();
        img.reset_payload_len(payload);
        img
    }

    pub fn set_special(&mut self, header: u16) {
        debug_assert!(header & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL);
        self.reset_payload_len(NUMERIC_HDRSZ_SHORT - VARHDRSZ);
        self.words[2] = header;
    }

    pub fn set_from_num(&mut self, num: Num<'_>) {
        let payload = num.as_bytes();
        self.reset_payload_len(payload.len());
        // SAFETY: words after the varlena header spans exactly payload.len() bytes.
        unsafe {
            core::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                self.words.as_mut_ptr().cast::<u8>().add(VARHDRSZ),
                payload.len(),
            );
        }
    }

    pub fn special(header: u16) -> NumericImage {
        debug_assert!(header & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL);
        let mut img = NumericImage::with_payload_len(NUMERIC_HDRSZ_SHORT - VARHDRSZ);
        img.words[2] = header;
        img
    }

    pub fn nan() -> NumericImage {
        NumericImage::special(NUMERIC_NAN)
    }

    pub fn pinf() -> NumericImage {
        NumericImage::special(NUMERIC_PINF)
    }

    pub fn ninf() -> NumericImage {
        NumericImage::special(NUMERIC_NINF)
    }

    pub fn from_num(num: Num<'_>) -> NumericImage {
        let mut img = NumericImage::empty();
        img.set_from_num(num);
        img
    }

    #[inline]
    pub fn set_header_word(&mut self, header: u16) {
        self.words[2] = header;
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: words is plain u16 storage viewed as bytes.
        unsafe {
            core::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.words.len() * 2)
        }
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.as_bytes()[VARHDRSZ..]
    }

    #[inline]
    pub fn num(&self) -> Num<'_> {
        Num::from_payload(self.payload())
    }
}

#[inline]
fn copy_digits(dst: &mut [u16], src: &[NumericDigit]) {
    debug_assert_eq!(dst.len(), src.len());
    // SAFETY: i16 and u16 share layout; lengths asserted equal.
    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr().cast::<u16>(), dst.as_mut_ptr(), src.len());
    }
}

/// C's make_result_opt_error; writes into `out` (retained capacity), false on
/// weight/dscale overflow of the packed format.
pub fn make_result_into(var: VarView<'_>, out: &mut NumericImage) -> bool {
    let sign = var.sign;

    if sign & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL {
        assert!(
            sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF,
            "invalid numeric sign value 0x{sign:x}"
        );
        out.set_special(sign);
        return true;
    }

    let mut digits = var.digits;
    let mut weight = var.weight;
    let mut sign = sign;
    let mut n = var.ndigits as usize;

    while n > 0 && digits[0] == 0 {
        digits = &digits[1..];
        weight -= 1;
        n -= 1;
    }
    while n > 0 && digits[n - 1] == 0 {
        n -= 1;
    }
    if n == 0 {
        weight = 0;
        sign = NUMERIC_POS;
    }

    if numeric_can_be_short(var.dscale, weight) {
        out.reset_payload_len(NUMERIC_HDRSZ_SHORT - VARHDRSZ + n * 2);
        out.words[2] = (if sign == NUMERIC_NEG {
            NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK
        } else {
            NUMERIC_SHORT
        }) | ((var.dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT)
            | (if weight < 0 {
                NUMERIC_SHORT_WEIGHT_SIGN_MASK
            } else {
                0
            })
            | (weight as u16 & NUMERIC_SHORT_WEIGHT_MASK);
        copy_digits(&mut out.words[3..], &digits[..n]);
    } else {
        if weight != weight as i16 as i32 || var.dscale != (var.dscale & NUMERIC_DSCALE_MASK as i32)
        {
            return false;
        }
        out.reset_payload_len(NUMERIC_HDRSZ - VARHDRSZ + n * 2);
        out.words[2] = sign | (var.dscale as u16 & NUMERIC_DSCALE_MASK);
        out.words[3] = weight as i16 as u16;
        copy_digits(&mut out.words[4..], &digits[..n]);
    }

    debug_assert_eq!(out.num().ndigits() as usize, n);
    true
}

pub fn make_result_opt_error(var: VarView<'_>) -> Option<NumericImage> {
    let mut img = NumericImage::empty();
    if make_result_into(var, &mut img) {
        Some(img)
    } else {
        None
    }
}

pub fn make_result(var: VarView<'_>) -> PgResult<NumericImage> {
    make_result_opt_error(var).ok_or_else(|| numeric_overflow_error().into())
}

pub fn int64_to_var(val: i64) -> NumericVar {
    let mut var = NumericVar::new();
    set_var_from_int64(val, &mut var);
    var
}

pub fn set_var_from_int64(val: i64, var: &mut NumericVar) {
    var.alloc(20 / DEC_DIGITS);
    var.sign = if val < 0 { NUMERIC_NEG } else { NUMERIC_POS };
    var.dscale = 0;
    if val == 0 {
        var.ndigits = 0;
        var.weight = 0;
        return;
    }
    let mut uval = val.unsigned_abs();
    let total = var.ndigits;
    let mut i = total;
    let digits_start = var.offset as usize;
    let buf = var.buf.as_mut_slice();
    while uval != 0 {
        i -= 1;
        let newuval = uval / NBASE as u64;
        buf[digits_start + i as usize] = (uval - newuval * NBASE as u64) as NumericDigit;
        uval = newuval;
    }
    var.offset += i as u32;
    var.ndigits = total - i;
    var.weight = total - i - 1;
}

pub fn int128_to_var(val: i128, var: &mut NumericVar) {
    var.alloc(40 / DEC_DIGITS);
    var.sign = if val < 0 { NUMERIC_NEG } else { NUMERIC_POS };
    var.dscale = 0;
    if val == 0 {
        var.ndigits = 0;
        var.weight = 0;
        return;
    }
    let mut uval = val.unsigned_abs();
    let total = var.ndigits;
    let mut i = total;
    let digits_start = var.offset as usize;
    let buf = var.buf.as_mut_slice();
    while uval != 0 {
        i -= 1;
        let newuval = uval / NBASE as u128;
        buf[digits_start + i as usize] = (uval - newuval * NBASE as u128) as NumericDigit;
        uval = newuval;
    }
    var.offset += i as u32;
    var.ndigits = total - i;
    var.weight = total - i - 1;
}

fn rounded_integer_var(var: VarView<'_>) -> NumericVar {
    let mut rounded = NumericVar::from_view(var);
    rounded.round(0);
    rounded.strip();
    rounded
}

pub fn var_to_int64(var: VarView<'_>) -> Option<i64> {
    let rounded = rounded_integer_var(var);
    let ndigits = rounded.ndigits;
    if ndigits == 0 {
        return Some(0);
    }

    let weight = rounded.weight;
    debug_assert!(weight >= 0 && ndigits <= weight + 1);

    // Accumulate negatively so i64::MIN survives (C's trick).
    let digits = rounded.digits();
    let neg = rounded.sign == NUMERIC_NEG;
    let mut val = -(digits[0] as i64);
    for i in 1..=weight {
        val = val.checked_mul(NBASE as i64)?;
        if i < ndigits {
            val = val.checked_sub(digits[i as usize] as i64)?;
        }
    }

    if !neg {
        if val == i64::MIN {
            return None;
        }
        val = -val;
    }
    Some(val)
}

pub fn var_to_uint64(var: VarView<'_>) -> Option<u64> {
    let rounded = rounded_integer_var(var);
    let ndigits = rounded.ndigits;
    if ndigits == 0 {
        return Some(0);
    }
    if rounded.sign == NUMERIC_NEG {
        return None;
    }

    let weight = rounded.weight;
    debug_assert!(weight >= 0 && ndigits <= weight + 1);

    let digits = rounded.digits();
    let mut val = digits[0] as u64;
    for i in 1..=weight {
        val = val.checked_mul(NBASE as u64)?;
        if i < ndigits {
            val = val.checked_add(digits[i as usize] as u64)?;
        }
    }
    Some(val)
}

pub fn var_to_int128(var: VarView<'_>) -> Option<i128> {
    let rounded = rounded_integer_var(var);
    let ndigits = rounded.ndigits;
    if ndigits == 0 {
        return Some(0);
    }

    let weight = rounded.weight;
    debug_assert!(weight >= 0 && ndigits <= weight + 1);

    let digits = rounded.digits();
    let neg = rounded.sign == NUMERIC_NEG;
    let mut val = digits[0] as i128;
    for i in 1..=weight {
        let oldval = val;
        val = val.wrapping_mul(NBASE as i128);
        if i < ndigits {
            val = val.wrapping_add(digits[i as usize] as i128);
        }
        if val / NBASE as i128 != oldval {
            // i128::MIN is representable only via the negative path.
            if !neg || val.wrapping_neg() != val || val == 0 || oldval < 0 {
                return None;
            }
        }
    }
    Some(if neg { val.wrapping_neg() } else { val })
}

pub fn var_to_int32(var: VarView<'_>) -> Option<i32> {
    let val = var_to_int64(var)?;
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return None;
    }
    Some(val as i32)
}

const _: () = assert!(core::mem::size_of::<NumericVar>() <= 128);
