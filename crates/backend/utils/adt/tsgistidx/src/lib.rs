//! Port of `src/backend/utils/adt/tsgistidx.c` (PostgreSQL 18.3) — the GiST
//! support functions for the `tsvector_ops` operator class
//! (`compress`/`decompress`/`consistent`/`union`/`same`/`penalty`/`picksplit`/
//! `options`, plus the `gtsvectorin`/`gtsvectorout` I/O and the
//! `gtsvector_consistent_oldsig` shim).
//!
//! The GiST index key is a [`SignTsVector`]: an array key (`ARRKEY`, sorted
//! lexeme-CRC hashes, leaf level) or a signature key (`SIGNKEY`, a fixed bit
//! signature, inner pages), with an `ALLISTRUE` shortcut.
//!
//! External calls go through owner seam crates: the `TS_execute` engine
//! (`tsvector_op.c`), the legacy CRC32 of a lexeme (`common/pg_crc.h`), and
//! `init_local_reloptions`/`add_local_int_reloption` (`reloptions.c`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use reloptions_seams as relopts_seams;
use tsvector_core_seams as tsop;
use hash_small_seams as crc;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_reloptions::local_relopts;
use ::tsearch::tsearch::{ExecPhraseData, QueryItem, QueryOperand, TSTernaryValue, TS_EXEC_PHRASE_NO_POS};
use ::tsearch::tsgistidx::{
    LexemeBytes, PickSplitResult, SignTsVector, SignTsVectorData, ALLISTRUE, ARRKEY, SIGNKEY,
};

use TSTernaryValue::{TS_MAYBE, TS_NO};

mod fmgr_builtins;

/// Install this crate's seams. Registers the SQL-callable `gtsvector` I/O
/// builtins (`gtsvectorin`/`gtsvectorout`) so `fmgr_info` / by-OID dispatch
/// resolve them (C: their `fmgr_builtins[]` rows).
pub fn init_seams() {
    fmgr_builtins::register_tsgistidx_builtins();
}

// ===========================================================================
// Constants and helpers (tsgistidx.c:28-84)
// ===========================================================================

/// `BITS_PER_BYTE` (c.h).
const BITS_PER_BYTE: i32 = 8;

/// `SIGLEN_DEFAULT` (tsgistidx.c:35) — `31 * 4`.
pub const SIGLEN_DEFAULT: i32 = 31 * 4;

/// `MAXIMUM_ALIGNOF` — 8 on supported targets.
const MAXIMUM_ALIGNOF: usize = 8;

#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

#[inline]
const fn maxalign_down(len: usize) -> usize {
    len & !(MAXIMUM_ALIGNOF - 1)
}

/// `SizeOfPageHeaderData` (`storage/bufpage.h`) — `offsetof(PageHeaderData,
/// pd_linp)` = 24 on the supported targets.
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;

/// `sizeof(GISTPageOpaqueData)` (`access/gist.h`) — `8 + 4 + 2 + 2 = 16`.
const SIZEOF_GIST_PAGE_OPAQUE_DATA: usize = 16;

/// `sizeof(ItemIdData)` (`storage/itemid.h`) — a 4-byte bitfield word.
const SIZEOF_ITEM_ID_DATA: usize = 4;

/// `sizeof(IndexTupleData)` (`access/itup.h`) — `8` bytes.
const SIZEOF_INDEX_TUPLE_DATA: usize = 8;

/// `BLCKSZ` (`pg_config.h`).
const BLCKSZ: usize = types_core::BLCKSZ;

/// `GISTMaxIndexTupleSize` (access/gist.h:101).
const GISTMaxIndexTupleSize: usize = maxalign_down(
    (BLCKSZ - SIZE_OF_PAGE_HEADER_DATA - SIZEOF_GIST_PAGE_OPAQUE_DATA) / 4 - SIZEOF_ITEM_ID_DATA,
);

/// `GISTMaxIndexKeySize` (access/gist.h:105) — the `SIGLEN_MAX` upper bound.
pub const SIGLEN_MAX: i32 = (GISTMaxIndexTupleSize - maxalign(SIZEOF_INDEX_TUPLE_DATA)) as i32;

/// `MaxHeapTupleSize` (`access/htup_details.h`) —
/// `BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData))`.
const MAX_HEAP_TUPLE_SIZE: usize =
    BLCKSZ - maxalign(SIZE_OF_PAGE_HEADER_DATA + SIZEOF_ITEM_ID_DATA);

/// `TOAST_INDEX_TARGET` (access/heaptoast.h) — `MaxHeapTupleSize / 16`.
const TOAST_INDEX_TARGET: usize = MAX_HEAP_TUPLE_SIZE / 16;

/// `SIGLENBIT(siglen)` (tsgistidx.c:41).
#[inline]
fn SIGLENBIT(siglen: i32) -> i32 {
    siglen * BITS_PER_BYTE
}

/// `HASHVAL(val, siglen)` (tsgistidx.c:54).
#[inline]
fn HASHVAL(val: i32, siglen: i32) -> i32 {
    ((val as u32) % (SIGLENBIT(siglen) as u32)) as i32
}

/// `SETBIT(x, i)` (tsgistidx.c:51).
#[inline]
fn SETBIT(sign: &mut [u8], i: i32) {
    let byte = (i / BITS_PER_BYTE) as usize;
    sign[byte] |= 0x01 << (i % BITS_PER_BYTE);
}

/// `GETBIT(x, i)` (tsgistidx.c:52).
#[inline]
fn GETBIT(sign: &[u8], i: i32) -> i32 {
    let byte = (i / BITS_PER_BYTE) as usize;
    ((sign[byte] >> (i % BITS_PER_BYTE)) & 0x01) as i32
}

/// `HASH(sign, val, siglen)` (tsgistidx.c:55).
#[inline]
fn HASH(sign: &mut [u8], val: i32, siglen: i32) {
    SETBIT(sign, HASHVAL(val, siglen));
}

/// `VARHDRSZ`.
const VARHDRSZ: usize = 4;

/// `GTHDRSIZE` (tsgistidx.c:78) — `VARHDRSZ + sizeof(int32)`.
const GTHDRSIZE: usize = VARHDRSZ + core::mem::size_of::<i32>();

/// `CALCGTSIZE(flag, len)` (tsgistidx.c:79).
#[inline]
fn CALCGTSIZE(flag: i32, len: usize) -> usize {
    GTHDRSIZE
        + if flag & ARRKEY != 0 {
            len * core::mem::size_of::<i32>()
        } else if flag & ALLISTRUE != 0 {
            0
        } else {
            len
        }
}

// ===========================================================================
// gtsvector_alloc (tsgistidx.c:146-159)
// ===========================================================================

/// `gtsvector_alloc(flag, len, sign)` (tsgistidx.c:146) — build a fresh
/// `SignTSVector` of `flag` / `len`, optionally copying a signature in.
fn gtsvector_alloc(flag: i32, len: i32, sign: Option<&[u8]>) -> SignTsVector {
    let len = len as usize;
    let data = if flag & ARRKEY != 0 {
        SignTsVectorData::Arr(alloc_vec_i32(len))
    } else if flag & ALLISTRUE != 0 {
        SignTsVectorData::AllTrue
    } else {
        // plain SIGNKEY
        let mut s = alloc_vec_u8(len);
        // C: if ((flag & (SIGNKEY | ALLISTRUE)) == SIGNKEY && sign)
        //        memcpy(GETSIGN(res), sign, len);
        if (flag & (SIGNKEY | ALLISTRUE)) == SIGNKEY {
            if let Some(src) = sign {
                s[..len].copy_from_slice(&src[..len]);
            }
        }
        SignTsVectorData::Sign(s)
    };
    SignTsVector { flag, data }
}

/// Zero-initialized owned `Vec<i32>` of `len` (the array-key payload escapes to
/// the caller as the owned key value).
#[inline]
fn alloc_vec_i32(len: usize) -> Vec<i32> {
    vec![0i32; len]
}

/// Zero-initialized owned `Vec<u8>` of `len`.
#[inline]
fn alloc_vec_u8(len: usize) -> Vec<u8> {
    vec![0u8; len]
}


// ===========================================================================
// makesign / compareint (tsgistidx.c:125-144)
// ===========================================================================

/// `pg_cmp_s32(a, b)` (common/int.h).
#[inline]
fn pg_cmp_s32(a: i32, b: i32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

/// `compareint` (tsgistidx.c:125).
#[inline]
fn compareint(a: i32, b: i32) -> i32 {
    pg_cmp_s32(a, b)
}

/// `makesign(sign, a, siglen)` (tsgistidx.c:134).
fn makesign(sign: &mut [u8], a: &SignTsVector, siglen: i32) {
    let arr = a.arr();
    // MemSet(sign, 0, siglen)
    for b in sign.iter_mut().take(siglen as usize) {
        *b = 0;
    }
    for &v in arr {
        HASH(sign, v, siglen);
    }
}

// ===========================================================================
// gtsvectorin / gtsvectorout (tsgistidx.c:88-123)
// ===========================================================================

/// `gtsvectorin` (tsgistidx.c:88) — there is no need to support input of
/// gtsvectors, so this always errors.
pub fn gtsvectorin() -> PgResult<()> {
    Err(PgError::error(format!("cannot accept a value of type {}", "gtsvector"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `gtsvectorout` (tsgistidx.c:99) — render a `SignTSVector` GiST key as text
/// (the `psprintf`/`pstrdup` C-string body).
pub fn gtsvectorout(key: &SignTsVector) -> String {
    if key.is_arrkey() {
        format!("{} unique words", key.arr().len())
    } else if key.is_alltrue() {
        "all true bits".to_string()
    } else {
        let siglen = key.sign().len() as i32;
        let cnttrue = sizebitvec(key.sign(), siglen);
        format!(
            "{} true bits, {} false bits",
            cnttrue,
            SIGLENBIT(siglen) - cnttrue
        )
    }
}

// ===========================================================================
// gtsvector_compress (tsgistidx.c:162-240)
// ===========================================================================

/// The leaf branch of `gtsvector_compress` (tsgistidx.c:169-219): build a
/// `SignTSVector` array key (or a signature key if too long) from the input
/// tsvector's lexemes. `lexemes` are the per-lexeme byte slices in `WordEntry`
/// order; `siglen` is `GET_SIGLEN()`.
pub fn gtsvector_compress_leaf(lexemes: &[LexemeBytes<'_>], siglen: i32) -> SignTsVector {
    let val_size = lexemes.len();
    let mut res = gtsvector_alloc(ARRKEY, val_size as i32, None);

    {
        let SignTsVectorData::Arr(arr) = &mut res.data else {
            unreachable!("gtsvector_alloc(ARRKEY, ..) builds an Arr");
        };
        for (i, &lex) in lexemes.iter().enumerate() {
            // INIT/COMP/FIN_LEGACY_CRC32(c, words + ptr->pos, ptr->len)
            let c = crc::legacy_crc32_lexeme::call(lex);
            arr[i] = c as i32;
        }
        // qsort(GETARR(res), val->size, sizeof(int), compareint); keys are plain
        // int32, so equal elements are byte-identical and tie order is moot.
        arr.sort_unstable_by(|&a, &b| compareint(a, b).cmp(&0));
    }

    // len = qunique(GETARR(res), val->size, sizeof(int), compareint);
    let len = {
        let SignTsVectorData::Arr(arr) = &mut res.data else {
            unreachable!("res is still the ARRKEY built above");
        };
        let n = qunique_i32(arr);
        if n != val_size as i32 {
            // hash-function collision; len always less than val->size
            arr.truncate(n as usize);
        }
        n
    };

    // make signature, if array is too long
    if CALCGTSIZE(ARRKEY, len as usize) > TOAST_INDEX_TARGET {
        let mut ressign = gtsvector_alloc(SIGNKEY, siglen, None);
        let res_for_sign = res;
        {
            let SignTsVectorData::Sign(sign) = &mut ressign.data else {
                unreachable!("gtsvector_alloc(SIGNKEY, ..) builds a Sign");
            };
            makesign(sign, &res_for_sign, siglen);
        }
        res = ressign;
    }

    res
}

/// The inner branch of `gtsvector_compress` (tsgistidx.c:220-238): rewrite an
/// inner `SIGNKEY` whose every byte is `0xff` as `ALLISTRUE`; `None` keeps the
/// entry unchanged. The caller guarantees `ISSIGNKEY(key) && !ISALLTRUE(key)`.
pub fn gtsvector_compress_inner_alltrue(key: &SignTsVector, siglen: i32) -> Option<SignTsVector> {
    let sign = key.sign();
    for i in 0..siglen as usize {
        if (sign[i] & 0xff) != 0xff {
            return None;
        }
    }
    Some(gtsvector_alloc(SIGNKEY | ALLISTRUE, siglen, Some(sign)))
}

// gtsvector_decompress (tsgistidx.c:242-264) is purely detoast + GISTENTRY
// repack (fmgr/Datum layer) and so has no in-crate computational body.

// ===========================================================================
// checkcondition_arr / checkcondition_bit (tsgistidx.c:266-322)
// ===========================================================================

/// `CHKVAL` (tsgistidx.c:266) — the leaf-array check state.
pub struct ChkVal<'a> {
    /// The `[arrb, arre)` array of `int32` hashes.
    pub arr: &'a [i32],
}

/// `checkcondition_arr` (tsgistidx.c:275) — `TS_execute` callback for matching a
/// tsquery operand against GiST leaf-page (array-key) data.
pub fn checkcondition_arr(checkval: &ChkVal<'_>, val: &QueryOperand) -> TSTernaryValue {
    let arr = checkval.arr;

    // we are not able to find a prefix by hash value
    if val.prefix {
        return TS_MAYBE;
    }

    let mut stop_low: usize = 0;
    let mut stop_high: usize = arr.len();

    while stop_low < stop_high {
        let stop_middle = stop_low + (stop_high - stop_low) / 2;
        if arr[stop_middle] == val.valcrc {
            return TS_MAYBE;
        } else if arr[stop_middle] < val.valcrc {
            stop_low = stop_middle + 1;
        } else {
            stop_high = stop_middle;
        }
    }

    TS_NO
}

/// `checkcondition_bit` (tsgistidx.c:307) — `TS_execute` callback for matching a
/// tsquery operand against GiST non-leaf (signature-key) data.
pub fn checkcondition_bit(key: &SignTsVector, val: &QueryOperand) -> TSTernaryValue {
    // we are not able to find a prefix in signature tree
    if val.prefix {
        return TS_MAYBE;
    }

    let sign = key.sign();
    let siglen = sign.len() as i32;
    if GETBIT(sign, HASHVAL(val.valcrc, siglen)) != 0 {
        TS_MAYBE
    } else {
        TS_NO
    }
}

// ===========================================================================
// gtsvector_consistent (tsgistidx.c:324-362)
// ===========================================================================

/// `gtsvector_consistent` (tsgistidx.c:324) — the GiST `consistent` support
/// function. `key` is the detoasted GiST key; `query_items` the decoded
/// `QueryItem` array, `query_size` its `query->size`. Returns `(matched,
/// recheck)`; `recheck` is always `true`. The `TS_execute` engine is seamed.
pub fn gtsvector_consistent(
    mcx: Mcx<'_>,
    key: &SignTsVector,
    query_items: &[QueryItem],
    query_size: i32,
) -> PgResult<(bool, bool)> {
    // All cases served by this function are inexact
    let recheck = true;

    if query_size == 0 {
        return Ok((false, recheck));
    }

    if key.is_signkey() {
        if key.is_alltrue() {
            return Ok((true, recheck));
        }

        // TS_execute(GETQUERY(query), key, TS_EXEC_PHRASE_NO_POS, checkcondition_bit)
        let mut cb = |_idx: usize, val: &QueryOperand, _data: Option<&mut ExecPhraseData>| {
            checkcondition_bit(key, val)
        };
        let matched = tsop::ts_execute::call(mcx, query_items, TS_EXEC_PHRASE_NO_POS, &mut cb)?;
        Ok((matched, recheck))
    } else {
        // only leaf pages: chkval.arrb = GETARR(key); chkval.arre += ARRNELEM(key)
        let chkval = ChkVal { arr: key.arr() };
        let mut cb = |_idx: usize, val: &QueryOperand, _data: Option<&mut ExecPhraseData>| {
            checkcondition_arr(&chkval, val)
        };
        let matched = tsop::ts_execute::call(mcx, query_items, TS_EXEC_PHRASE_NO_POS, &mut cb)?;
        Ok((matched, recheck))
    }
}

// ===========================================================================
// unionkey / gtsvector_union (tsgistidx.c:364-417)
// ===========================================================================

/// `unionkey(sbase, add, siglen)` (tsgistidx.c:364) — OR key `add` into the
/// running signature `sbase`. Returns `1` when `add` is `ALLISTRUE`, else `0`.
fn unionkey(sbase: &mut [u8], add: &SignTsVector, siglen: i32) -> i32 {
    if add.is_signkey() {
        if add.is_alltrue() {
            return 1;
        }

        let sadd = add.sign();
        debug_assert_eq!(sadd.len() as i32, siglen);

        for i in 0..siglen as usize {
            sbase[i] |= sadd[i];
        }
    } else {
        for &v in add.arr() {
            HASH(sbase, v, siglen);
        }
    }
    0
}

/// `gtsvector_union` (tsgistidx.c:392) — GiST `union`. `entries` are the
/// detoasted keys of the `GistEntryVector` in order; `siglen` is `GET_SIGLEN()`.
pub fn gtsvector_union(entries: &[&SignTsVector], siglen: i32) -> SignTsVector {
    let mut result = gtsvector_alloc(SIGNKEY, siglen, None);

    for &entry in entries {
        let alltrue = {
            let SignTsVectorData::Sign(base) = &mut result.data else {
                unreachable!("result starts as a plain SIGNKEY");
            };
            unionkey(base, entry, siglen)
        };
        if alltrue != 0 {
            result.flag |= ALLISTRUE;
            result.data = SignTsVectorData::AllTrue;
            break;
        }
    }

    result
}

// ===========================================================================
// gtsvector_same (tsgistidx.c:419-478)
// ===========================================================================

/// `gtsvector_same` (tsgistidx.c:419) — GiST `same`.
pub fn gtsvector_same(a: &SignTsVector, b: &SignTsVector, siglen: i32) -> bool {
    if a.is_signkey() {
        // then b also ISSIGNKEY
        if a.is_alltrue() && b.is_alltrue() {
            true
        } else if a.is_alltrue() {
            false
        } else if b.is_alltrue() {
            false
        } else {
            let sa = a.sign();
            let sb = b.sign();
            debug_assert!(sa.len() as i32 == siglen && sb.len() as i32 == siglen);

            let mut result = true;
            for i in 0..siglen as usize {
                if sa[i] != sb[i] {
                    result = false;
                    break;
                }
            }
            result
        }
    } else {
        // a and b ISARRKEY
        let pa = a.arr();
        let pb = b.arr();
        if pa.len() != pb.len() {
            false
        } else {
            let mut result = true;
            for i in 0..pa.len() {
                if pa[i] != pb[i] {
                    result = false;
                    break;
                }
            }
            result
        }
    }
}

// ===========================================================================
// sizebitvec / hemdistsign / hemdist (tsgistidx.c:480-521)
// ===========================================================================

/// `sizebitvec` (tsgistidx.c:480) — `pg_popcount(sign, siglen)`.
fn sizebitvec(sign: &[u8], siglen: i32) -> i32 {
    let mut cnt: i32 = 0;
    for &b in sign.iter().take(siglen as usize) {
        cnt += b.count_ones() as i32;
    }
    cnt
}

/// `hemdistsign` (tsgistidx.c:486) — Hamming distance between two byte signatures.
fn hemdistsign(a: &[u8], b: &[u8], siglen: i32) -> i32 {
    let mut dist: i32 = 0;
    for i in 0..siglen as usize {
        let diff = a[i] ^ b[i];
        // Using the popcount functions here isn't likely to win.
        dist += diff.count_ones() as i32;
    }
    dist
}

/// `hemdist` (tsgistidx.c:502) — Hamming distance between two signature keys,
/// honoring the `ALLISTRUE` shortcuts.
fn hemdist(a: &SignTsVector, b: &SignTsVector) -> i32 {
    let siglena = a.sign().len() as i32;
    let siglenb = b.sign().len() as i32;

    if a.is_alltrue() {
        if b.is_alltrue() {
            return 0;
        } else {
            return SIGLENBIT(siglenb) - sizebitvec(b.sign(), siglenb);
        }
    } else if b.is_alltrue() {
        return SIGLENBIT(siglena) - sizebitvec(a.sign(), siglena);
    }

    debug_assert_eq!(siglena, siglenb);

    hemdistsign(a.sign(), b.sign(), siglena)
}

// ===========================================================================
// gtsvector_penalty (tsgistidx.c:523-558)
// ===========================================================================

/// `gtsvector_penalty` (tsgistidx.c:523) — GiST `penalty`. `origval` (always
/// `ISSIGNKEY`) and `newval` are the two keys; `siglen` is `GET_SIGLEN()`. The
/// transient `makesign` signature buffer is allocated in `mcx`.
pub fn gtsvector_penalty(
    mcx: Mcx<'_>,
    origval: &SignTsVector,
    newval: &SignTsVector,
    siglen: i32,
) -> PgResult<f32> {
    // C: `*penalty = 0.0;` — always overwritten by one of the branches below.
    let penalty: f32;

    if newval.is_arrkey() {
        let mut sign = zeroed_signature(mcx, siglen)?;
        makesign(&mut sign, newval, siglen);

        if origval.is_alltrue() {
            let siglenbit = SIGLENBIT(siglen);
            penalty = (siglenbit - sizebitvec(&sign, siglen)) as f32 / (siglenbit + 1) as f32;
        } else {
            penalty = hemdistsign(&sign, origval.sign(), siglen) as f32;
        }
    } else {
        penalty = hemdist(origval, newval) as f32;
    }

    Ok(penalty)
}

/// Build a `siglen`-byte zeroed signature buffer in `mcx` (C `palloc(siglen)`).
fn zeroed_signature(mcx: Mcx<'_>, siglen: i32) -> PgResult<PgVec<'_, u8>> {
    let n = siglen as usize;
    let mut sign = vec_with_capacity_in(mcx, n)?;
    sign.resize(n, 0u8);
    Ok(sign)
}

// ===========================================================================
// picksplit support (tsgistidx.c:560-609)
// ===========================================================================

/// `CACHESIGN` (tsgistidx.c:560) — a cached signature for one entry during
/// picksplit; each slot owns its `siglen`-byte signature.
struct CacheSign<'mcx> {
    allistrue: bool,
    sign: PgVec<'mcx, u8>,
}

/// `fillcache` (tsgistidx.c:566) — materialize entry `key` into cache slot.
fn fillcache(item: &mut CacheSign<'_>, key: &SignTsVector, siglen: i32) {
    item.allistrue = false;
    if key.is_arrkey() {
        makesign(&mut item.sign, key, siglen);
    } else if key.is_alltrue() {
        item.allistrue = true;
    } else {
        item.sign[..siglen as usize].copy_from_slice(&key.sign()[..siglen as usize]);
    }
}

/// `WISH_F(a, b, c)` (tsgistidx.c:578).
#[inline]
fn WISH_F(a: i32, b: i32, c: f64) -> f64 {
    -(((a - b) * (a - b) * (a - b)) as f64) * c
}

/// `SPLITCOST` (tsgistidx.c:579).
#[derive(Clone, Copy)]
struct SplitCost {
    pos: u16,
    cost: i32,
}

/// `comparecost` (tsgistidx.c:585).
#[inline]
fn comparecost(a: &SplitCost, b: &SplitCost) -> i32 {
    pg_cmp_s32(a.cost, b.cost)
}

/// `hemdistcache` (tsgistidx.c:595).
fn hemdistcache(a: &CacheSign<'_>, b: &CacheSign<'_>, siglen: i32) -> i32 {
    if a.allistrue {
        if b.allistrue {
            return 0;
        } else {
            return SIGLENBIT(siglen) - sizebitvec(&b.sign, siglen);
        }
    } else if b.allistrue {
        return SIGLENBIT(siglen) - sizebitvec(&a.sign, siglen);
    }

    hemdistsign(&a.sign, &b.sign, siglen)
}

// ===========================================================================
// gtsvector_picksplit (tsgistidx.c:611-784)
// ===========================================================================

/// `OffsetNumberNext(off)` (storage/off.h).
#[inline]
fn OffsetNumberNext(off: u16) -> u16 {
    off + 1
}

/// `FirstOffsetNumber` (storage/off.h).
const FirstOffsetNumber: u16 = 1;

/// `gtsvector_picksplit` (tsgistidx.c:611) — GiST `picksplit`. `entries` are the
/// detoasted keys indexed by offset number (`entries[FirstOffsetNumber]` is the
/// first real entry, matching `GETENTRY`); `entryvec_n` is `entryvec->n`;
/// `siglen` is `GET_SIGLEN()`. The transient `cache` / `costvector` buffers are
/// allocated in `mcx`.
pub fn gtsvector_picksplit(
    mcx: Mcx<'_>,
    entries: &[&SignTsVector],
    entryvec_n: i32,
    siglen: i32,
) -> PgResult<PickSplitResult> {
    let mut maxoff: u16 = (entryvec_n - 2) as u16;

    // cache = palloc(sizeof(CACHESIGN) * (maxoff + 2));
    let nslots = (maxoff as usize) + 2;
    let mut cache: PgVec<'_, CacheSign<'_>> = vec_with_capacity_in(mcx, nslots)?;
    for _ in 0..nslots {
        cache.push(CacheSign {
            allistrue: false,
            sign: zeroed_signature(mcx, siglen)?,
        });
    }

    fillcache(
        &mut cache[FirstOffsetNumber as usize],
        entries[FirstOffsetNumber as usize],
        siglen,
    );

    let mut seed_1: u16 = 0;
    let mut seed_2: u16 = 0;
    let mut waste: i32 = -1;

    let mut k = FirstOffsetNumber;
    while k < maxoff {
        let mut j = OffsetNumberNext(k);
        while j <= maxoff {
            if k == FirstOffsetNumber {
                fillcache(&mut cache[j as usize], entries[j as usize], siglen);
            }

            let size_waste = hemdistcache(&cache[j as usize], &cache[k as usize], siglen);
            if size_waste > waste {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
            j = OffsetNumberNext(j);
        }
        k = OffsetNumberNext(k);
    }

    let mut left: Vec<u16> = Vec::new();
    let mut right: Vec<u16> = Vec::new();

    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    // form initial ..
    let mut datum_l = gtsvector_alloc(
        SIGNKEY | if cache[seed_1 as usize].allistrue { ALLISTRUE } else { 0 },
        siglen,
        Some(&cache[seed_1 as usize].sign[..]),
    );
    let mut datum_r = gtsvector_alloc(
        SIGNKEY | if cache[seed_2 as usize].allistrue { ALLISTRUE } else { 0 },
        siglen,
        Some(&cache[seed_2 as usize].sign[..]),
    );

    maxoff = OffsetNumberNext(maxoff);
    fillcache(&mut cache[maxoff as usize], entries[maxoff as usize], siglen);

    // sort before ..
    let mut costvector: PgVec<'_, SplitCost> = vec_with_capacity_in(mcx, maxoff as usize)?;
    for _ in 0..maxoff as usize {
        costvector.push(SplitCost { pos: 0, cost: 0 });
    }
    {
        let mut j = FirstOffsetNumber;
        while j <= maxoff {
            costvector[(j - 1) as usize].pos = j;
            let size_alpha = hemdistcache(&cache[seed_1 as usize], &cache[j as usize], siglen);
            let size_beta = hemdistcache(&cache[seed_2 as usize], &cache[j as usize], siglen);
            costvector[(j - 1) as usize].cost = (size_alpha - size_beta).abs();
            j = OffsetNumberNext(j);
        }
    }
    // qsort(costvector, maxoff, sizeof(SPLITCOST), comparecost); equal-cost
    // entries are ties whose visitation order changes group assignment (via
    // WISH_F balancing) — mirror libc qsort's unstable order.
    costvector.sort_unstable_by(|a, b| comparecost(a, b).cmp(&0));

    for k in 0..maxoff {
        let j = costvector[k as usize].pos;
        if j == seed_1 {
            left.push(j);
            continue;
        } else if j == seed_2 {
            right.push(j);
            continue;
        }

        let size_alpha: i32;
        if datum_l.is_alltrue() || cache[j as usize].allistrue {
            if datum_l.is_alltrue() && cache[j as usize].allistrue {
                size_alpha = 0;
            } else {
                let from: &[u8] = if cache[j as usize].allistrue {
                    datum_l.sign()
                } else {
                    &cache[j as usize].sign[..]
                };
                size_alpha = SIGLENBIT(siglen) - sizebitvec(from, siglen);
            }
        } else {
            size_alpha = hemdistsign(&cache[j as usize].sign, datum_l.sign(), siglen);
        }

        let size_beta: i32;
        if datum_r.is_alltrue() || cache[j as usize].allistrue {
            if datum_r.is_alltrue() && cache[j as usize].allistrue {
                size_beta = 0;
            } else {
                let from: &[u8] = if cache[j as usize].allistrue {
                    datum_r.sign()
                } else {
                    &cache[j as usize].sign[..]
                };
                size_beta = SIGLENBIT(siglen) - sizebitvec(from, siglen);
            }
        } else {
            size_beta = hemdistsign(&cache[j as usize].sign, datum_r.sign(), siglen);
        }

        if (size_alpha as f64)
            < size_beta as f64 + WISH_F(left.len() as i32, right.len() as i32, 0.1)
        {
            if datum_l.is_alltrue() || cache[j as usize].allistrue {
                if !datum_l.is_alltrue() {
                    // memset(GETSIGN(datum_l), 0xff, siglen);
                    let sign = datum_l.sign_mut();
                    for b in sign.iter_mut().take(siglen as usize) {
                        *b = 0xff;
                    }
                }
            } else {
                // LOOPBYTE(siglen) union_l[i] |= ptr[i];
                let ptr = &cache[j as usize].sign;
                let union_l = datum_l.sign_mut();
                for i in 0..siglen as usize {
                    union_l[i] |= ptr[i];
                }
            }
            left.push(j);
        } else {
            if datum_r.is_alltrue() || cache[j as usize].allistrue {
                if !datum_r.is_alltrue() {
                    let sign = datum_r.sign_mut();
                    for b in sign.iter_mut().take(siglen as usize) {
                        *b = 0xff;
                    }
                }
            } else {
                let ptr = &cache[j as usize].sign;
                let union_r = datum_r.sign_mut();
                for i in 0..siglen as usize {
                    union_r[i] |= ptr[i];
                }
            }
            right.push(j);
        }
    }

    // C writes `*right = *left = FirstOffsetNumber;` into the one-past-end
    // sentinel slot; it is never read, so it is elided here.

    Ok(PickSplitResult {
        spl_left: left,
        spl_right: right,
        spl_ldatum: Some(datum_l),
        spl_rdatum: Some(datum_r),
    })
}

// ===========================================================================
// gtsvector_consistent_oldsig (tsgistidx.c:793-797)
// ===========================================================================

/// `gtsvector_consistent_oldsig` (tsgistidx.c:793) — pre-9.6-signature
/// compatibility shim; forwards to [`gtsvector_consistent`].
pub fn gtsvector_consistent_oldsig(
    mcx: Mcx<'_>,
    key: &SignTsVector,
    query_items: &[QueryItem],
    query_size: i32,
) -> PgResult<(bool, bool)> {
    gtsvector_consistent(mcx, key, query_items, query_size)
}

// ===========================================================================
// gtsvector_options (tsgistidx.c:799-810)
// ===========================================================================

/// `offsetof(GistTsVectorOptions, siglen)` — `siglen` follows the 4-byte
/// `vl_len_` varlena header, so it is at offset 4.
const OFFSETOF_GISTTSVECTOROPTIONS_SIGLEN: i32 = 4;

/// `sizeof(GistTsVectorOptions)` (tsgistidx.c:29) — `int32 vl_len_` + `int
/// siglen`.
const SIZEOF_GISTTSVECTOROPTIONS: usize =
    core::mem::size_of::<i32>() + core::mem::size_of::<i32>();

/// `gtsvector_options` (tsgistidx.c:799) — register the `siglen` opclass option.
pub fn gtsvector_options(relopts: &mut local_relopts) {
    relopts_seams::init_local_reloptions::call(relopts, SIZEOF_GISTTSVECTOROPTIONS);
    relopts_seams::add_local_int_reloption::call(
        relopts,
        "siglen",
        Some("signature length"),
        SIGLEN_DEFAULT,
        1,
        SIGLEN_MAX,
        OFFSETOF_GISTTSVECTOROPTIONS_SIGLEN,
    );
}

// ===========================================================================
// qunique helper.
// ===========================================================================

/// `qunique(array, len, sizeof(int), compareint)` (lib/qunique.h) specialized to
/// a sorted `int32` slice: collapse adjacent duplicates in place, returning the
/// number of distinct elements.
fn qunique_i32(arr: &mut [i32]) -> i32 {
    if arr.is_empty() {
        return 0;
    }
    let mut last: usize = 0;
    for i in 1..arr.len() {
        if compareint(arr[i], arr[last]) != 0 {
            last += 1;
            if last != i {
                arr[last] = arr[i];
            }
        }
    }
    (last + 1) as i32
}
