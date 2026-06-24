//! `contrib/ltree/ltree_gist.c` (`gist_ltree_ops`) and `_ltree_gist.c`
//! (`gist__ltree_ops`, the `ltree[]` array opclass) — the GiST opclass support
//! functions, ported over pgrust's generic catalog-driven GiST opclass dispatch
//! (mirroring `pg_trgm`'s `trgm_gist`).
//!
//! The GiST core resolves each support proc into an `FmgrInfo`; `gist-proc`'s
//! `extdispatch` invokes the body here through a real fmgr frame, passing the
//! `GISTENTRY`/`GistEntryVector`/`GIST_SPLITVEC` + the `*recheck`/`*penalty`/
//! `*size` out-parameters through the [`::gist::extproc`] internal protocol
//! struct (slot 0), and the `consistent` query (an `ltree`/`lquery`/`ltxtquery`/
//! `ltree[]` varlena, per strategy) on the by-ref lane (slot 1).
//!
//! ## The `ltree_gist` key
//!
//! A GiST key is a varlena: `[VARHDR(4) | flag:uint32 | data]`, with
//! `LTG_HDRSIZE = MAXALIGN(VARHDRSZ + sizeof(uint32)) = 8`. The `flag` bits
//! (`ltree.h`):
//! * `LTG_ONENODE` — a leaf key holding a single `ltree` node (`data` is the
//!   node, no signature);
//! * otherwise an inner key: `data` is `[sign(siglen)][left ltree][right ltree?]`
//!   (`LTG_NORIGHT` means right == left; `LTG_ALLTRUE` means the signature is the
//!   all-ones bitmap and is NOT stored, so `data` is just `[left][right?]`).
//!
//! The varlena length header is encoded `len << 2` (the uncompressed-inline 4B-U
//! varlena tag), exactly as `repr::set_varsize`; the payload is bounded by
//! `VARSIZE` so trailing MAXALIGN padding the GiST core adds is ignored.
//!
//! ## `siglen`
//!
//! C reads the opclass `siglen` option via `LTREE_GET_SIGLEN()` /
//! `LTREE_GET_ASIGLEN()`. pgrust's generic GiST dispatch does not thread opclass
//! options to the support procs (the documented `tsvector_ops` divergence), so
//! the build side uses [`LTREE_SIGLEN_DEFAULT`] / [`LTREE_ASIGLEN_DEFAULT`] and
//! the read side uses the same default — the index is built and read at one
//! self-consistent signature length. Results stay correct regardless of the
//! configured `siglen`: the array opclass's `_ltree_consistent` always sets
//! `recheck = true` (the heap recheck makes the scan exact), and the scalar
//! opclass's leaf `ltree_consistent` checks the actual node exactly while inner
//! signature checks are a Bloom filter (false positives only, no false
//! negatives → exact final counts). Only an index built with an explicit
//! non-default `siglen` differs from C in its physical signature length, not in
//! its results. The `siglen` reloption is still REGISTERED with its range +
//! "multiple of 4" validator so `WITH (siglen = N)` parses and rejects bad
//! values exactly as C.

use ::types_error::error::{ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use ::types_error::{PgError, PgResult};

use crate::array::LtreeArray;
use crate::crc::ltree_crc32_sz;
use crate::op;
use crate::repr::{intalign, read_u32, set_varsize, varsize, Ltree, Lquery, VARHDRSZ};

// ===========================================================================
// ltree.h GiST constants.
// ===========================================================================

const BITBYTE: usize = 8;

/// `LTG_HDRSIZE = MAXALIGN(VARHDRSZ + sizeof(uint32))` = MAXALIGN(8) = 8.
const LTG_HDRSIZE: usize = 8;

const LTG_ONENODE: u32 = 0x01;
const LTG_ALLTRUE: u32 = 0x02;
const LTG_NORIGHT: u32 = 0x04;

/// `LTREE_SIGLEN_DEFAULT = 2 * sizeof(int32)` (8 bytes) for `gist_ltree_ops`.
pub const LTREE_SIGLEN_DEFAULT: i32 = 2 * 4;
/// `LTREE_ASIGLEN_DEFAULT = 7 * sizeof(int32)` (28 bytes) for `gist__ltree_ops`.
pub const LTREE_ASIGLEN_DEFAULT: i32 = 7 * 4;

/// `LTREE_SIGLEN_MAX = LTREE_ASIGLEN_MAX = GISTMaxIndexKeySize`.
///
/// `GISTMaxIndexKeySize = MAXALIGN_DOWN((BLCKSZ - SizeOfPageHeaderData -
/// sizeof(GISTPageOpaqueData)) / 4 - sizeof(ItemIdData)) - MAXALIGN(sizeof(
/// IndexTupleData))`, the same derivation `trgm_gist`/`tsgistidx` use. With
/// BLCKSZ=8192 this evaluates to 2024.
const fn maxalign_down(len: usize) -> usize {
    len & !7usize
}
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7usize
}
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
const SIZEOF_GIST_PAGE_OPAQUE_DATA: usize = 16;
const SIZEOF_ITEM_ID_DATA: usize = 4;
const SIZEOF_INDEX_TUPLE_DATA: usize = 8;
const GIST_MAX_INDEX_KEY_SIZE: usize = maxalign_down(
    (8192 - SIZE_OF_PAGE_HEADER_DATA - SIZEOF_GIST_PAGE_OPAQUE_DATA) / 4 - SIZEOF_ITEM_ID_DATA,
) - maxalign(SIZEOF_INDEX_TUPLE_DATA);
/// `LTREE_SIGLEN_MAX` (= 2024 for BLCKSZ=8192).
pub const LTREE_SIGLEN_MAX: i32 = GIST_MAX_INDEX_KEY_SIZE as i32;

/// `INTALIGN(1)` — the `siglen` reloption minimum for `gist_ltree_ops`.
pub const SIGLEN_MIN_INTALIGNED: i32 = 4;

/// `offsetof(LtreeGistOptions, siglen)` — the `int siglen` after the 4-byte
/// varlena header.
const OFFSETOF_LTREE_GIST_OPTIONS_SIGLEN: i32 = 4;
/// `sizeof(LtreeGistOptions)` (`int32 vl_len_` + `int siglen`).
const SIZEOF_LTREE_GIST_OPTIONS: usize = 8;

// `FLG_CANLOOKSIGN(x)` (LOWER_NODE build): no NOT / '*' / '%' bit set.
const LQL_NOT: u8 = 0x10;
const LVAR_ANYEND: u8 = 0x01;
const LVAR_SUBLEXEME: u8 = 0x04;
fn flg_canlooksign(flag: u8) -> bool {
    flag & (LQL_NOT | LVAR_ANYEND | LVAR_SUBLEXEME) == 0
}

// ===========================================================================
// Signature bitmap primitives (ltree.h macros).
//
// NB: ltree's SIGLENBIT(siglen) = siglen * BITBYTE (no `-1`, unlike pg_trgm).
// ===========================================================================

fn siglenbit(siglen: usize) -> usize {
    siglen * BITBYTE
}
/// `HASHVAL(val, siglen) = ((unsigned)val) % SIGLENBIT(siglen)`. The CRC is a
/// `u32`; cast through u32 to match C's `(unsigned int) val`.
fn hashval(val: i32, siglen: usize) -> usize {
    (val as u32 as usize) % siglenbit(siglen)
}
fn getbit(sign: &[u8], i: usize) -> bool {
    (sign[i / BITBYTE] >> (i % BITBYTE)) & 0x01 != 0
}
fn setbit(sign: &mut [u8], i: usize) {
    sign[i / BITBYTE] |= 0x01 << (i % BITBYTE);
}

/// `pg_popcount(sign, siglen)`.
fn sizebitvec(sign: &[u8]) -> i32 {
    sign.iter().map(|b| b.count_ones() as i32).sum()
}
/// `hemdistsign(a, b, siglen)`.
fn hemdistsign(a: &[u8], b: &[u8], siglen: usize) -> i32 {
    let mut dist = 0;
    for i in 0..siglen {
        dist += (a[i] ^ b[i]).count_ones() as i32;
    }
    dist
}

// ===========================================================================
// The decoded ltree_gist key.
// ===========================================================================

/// A decoded `ltree_gist` key. `siglen` is the opclass signature length the
/// decode was performed with (the layout of LNODE/RNODE depends on it).
#[derive(Clone, Debug)]
struct LtgKey {
    flag: u32,
    /// For ONENODE: the single node. For an inner key: the LEFT node.
    lnode: Vec<u8>,
    /// For an inner key: the RIGHT node (== lnode when NORIGHT). Empty for
    /// ONENODE (use `lnode`).
    rnode: Vec<u8>,
    /// The signature bytes (length `siglen`), or empty when ALLTRUE / ONENODE.
    sign: Vec<u8>,
}

impl LtgKey {
    fn is_onenode(&self) -> bool {
        self.flag & LTG_ONENODE != 0
    }
    fn is_alltrue(&self) -> bool {
        self.flag & LTG_ALLTRUE != 0
    }
    /// `LTG_GETLNODE` — the bounding left node (== the node for ONENODE).
    fn get_lnode(&self) -> &[u8] {
        &self.lnode
    }
    /// `LTG_GETRNODE` — the bounding right node (== the node for ONENODE).
    fn get_rnode(&self) -> &[u8] {
        if self.is_onenode() {
            &self.lnode
        } else {
            &self.rnode
        }
    }
}

/// Decode a header-ful `ltree_gist` varlena image at the given `siglen`.
fn decode_key(image: &[u8], siglen: usize) -> PgResult<LtgKey> {
    if image.len() < LTG_HDRSIZE {
        return Err(PgError::error("corrupt ltree_gist GiST key (short header)"));
    }
    let total = {
        let raw = read_u32(image, 0);
        // VARSIZE = word >> 2 (low 2 bits are the inline tag).
        ((raw >> 2) as usize).min(image.len()).max(LTG_HDRSIZE)
    };
    let flag = read_u32(image, 4);
    let data = &image[LTG_HDRSIZE..total];

    if flag & LTG_ONENODE != 0 {
        // (len)(flag)(ltree)
        let node = read_node(data, 0)?;
        return Ok(LtgKey {
            flag,
            lnode: node,
            rnode: Vec::new(),
            sign: Vec::new(),
        });
    }

    // Inner: (len)(flag)(sign?)(left?)(right?)
    //
    // The scalar opclass stores bounding left/right ltree nodes after the
    // signature; the array opclass stores NONE (signature-only keys). Detect
    // the latter when no bytes remain after the signature.
    let (sign, off) = if flag & LTG_ALLTRUE != 0 {
        (Vec::new(), 0usize)
    } else {
        if data.len() < siglen {
            return Err(PgError::error("corrupt ltree_gist GiST key (short signature)"));
        }
        (data[..siglen].to_vec(), siglen)
    };

    if off >= data.len() {
        // Signature-only key (array opclass, or an ALLTRUE key with no nodes).
        return Ok(LtgKey {
            flag,
            lnode: Vec::new(),
            rnode: Vec::new(),
            sign,
        });
    }

    let left = read_node(data, off)?;
    let left_size = varsize(&data[off..]);
    let rnode = if flag & LTG_NORIGHT != 0 {
        left.clone()
    } else if off + left_size >= data.len() {
        left.clone()
    } else {
        read_node(data, off + left_size)?
    };
    Ok(LtgKey {
        flag,
        lnode: left,
        rnode,
        sign,
    })
}

/// Read an `ltree` node at byte offset `off` within `data` (bounded by its own
/// `VARSIZE`).
fn read_node(data: &[u8], off: usize) -> PgResult<Vec<u8>> {
    if off + VARHDRSZ > data.len() {
        return Err(PgError::error("corrupt ltree_gist GiST key (truncated node)"));
    }
    let sz = varsize(&data[off..]);
    if off + sz > data.len() {
        return Err(PgError::error("corrupt ltree_gist GiST key (node overruns key)"));
    }
    Ok(data[off..off + sz].to_vec())
}

/// `ltree_gist_alloc(isalltrue, sign, siglen, left, right)` — build a key image.
/// `sign == None` means a zeroed signature; pass `siglen == 0` to build a
/// ONENODE key (then `left` is the single node and `isalltrue`/`sign` ignored).
fn ltree_gist_alloc(
    isalltrue: bool,
    sign: Option<&[u8]>,
    siglen: usize,
    left: Option<&[u8]>,
    right: Option<&[u8]>,
) -> Vec<u8> {
    let left_sz = left.map_or(0, varsize);
    let right_sz = right.map_or(0, varsize);
    // size = LTG_HDRSIZE + (isalltrue ? 0 : siglen)
    //      + (left ? VARSIZE(left) + (right ? VARSIZE(right) : 0) : 0)
    let size = LTG_HDRSIZE
        + if isalltrue { 0 } else { siglen }
        + if left.is_some() {
            left_sz + if right.is_some() { right_sz } else { 0 }
        } else {
            0
        };
    let mut img = vec![0u8; size];
    set_varsize(&mut img, size);

    if siglen != 0 {
        let mut flag: u32 = 0;
        let mut off = LTG_HDRSIZE;
        if isalltrue {
            flag |= LTG_ALLTRUE;
        } else {
            // memcpy(LTG_SIGN(result), sign, siglen) or memset 0.
            if let Some(s) = sign {
                img[off..off + siglen].copy_from_slice(&s[..siglen]);
            }
            off += siglen;
        }
        if let Some(l) = left {
            img[off..off + left_sz].copy_from_slice(&l[..left_sz]);
            let off_r = off + left_sz;
            // NORIGHT iff no right, or left==right, or ISEQ(left,right).
            let noright = match right {
                None => true,
                Some(r) => iseq(l, r),
            };
            if noright {
                flag |= LTG_NORIGHT;
            } else {
                let r = right.unwrap();
                img[off_r..off_r + right_sz].copy_from_slice(&r[..right_sz]);
            }
        }
        // write flag (uint32 at offset 4).
        img[4..8].copy_from_slice(&flag.to_ne_bytes());
    } else {
        // ONENODE.
        let flag = LTG_ONENODE;
        img[4..8].copy_from_slice(&flag.to_ne_bytes());
        let l = left.expect("ltree_gist_alloc(siglen=0) requires a node");
        img[LTG_HDRSIZE..LTG_HDRSIZE + left_sz].copy_from_slice(&l[..left_sz]);
    }
    img
}

/// `ISEQ(a, b)` — `a->numlevel == b->numlevel && ltree_compare(a, b) == 0`.
fn iseq(a: &[u8], b: &[u8]) -> bool {
    Ltree::new(a).numlevel() == Ltree::new(b).numlevel() && op::ltree_compare(a, b) == 0
}

/// `hashing(sign, t, siglen)` (scalar opclass) — set a bit for each level CRC.
fn hashing(sign: &mut [u8], t: &[u8], siglen: usize) {
    for lvl in Ltree::new(t).levels() {
        let h = ltree_crc32_sz(lvl.name) as i32;
        setbit(sign, hashval(h, siglen));
    }
}

// ===========================================================================
// gist_ltree_ops (ltree_gist.c)
// ===========================================================================

/// `ltree_compress(entry)` — a leaf `ltree` becomes a ONENODE key; an inner key
/// passes through. Returns `Some(new_key)` or `None` (pass-through).
pub fn ltree_compress(leafkey: bool, key_image: &[u8], key_is_null: bool) -> PgResult<Option<Vec<u8>>> {
    if leafkey {
        if key_is_null {
            return Err(PgError::error("ltree_compress: NULL leaf key"));
        }
        // val = DatumGetLtreeP(entry->key); key = ltree_gist_alloc(false,NULL,0,val,0)
        let val = canonical_varlena(key_image);
        let key = ltree_gist_alloc(false, None, 0, Some(&val), None);
        return Ok(Some(key));
    }
    // inner: pass through.
    Ok(None)
}

/// `ltree_decompress` — only PG_DETOASTs; the owned by-ref lane already delivers
/// a plain image, so identity (pass-through).
pub fn ltree_decompress() -> Option<Vec<u8>> {
    None
}

/// `ltree_same(a, b, &result)` (scalar opclass).
pub fn ltree_same(a_image: &[u8], b_image: &[u8], siglen: usize) -> PgResult<bool> {
    let a = decode_key(a_image, siglen)?;
    let b = decode_key(b_image, siglen)?;

    if a.is_onenode() != b.is_onenode() {
        return Ok(false);
    }
    if a.is_onenode() {
        return Ok(iseq(&a.lnode, &b.lnode));
    }
    if a.is_alltrue() != b.is_alltrue() {
        return Ok(false);
    }
    if !iseq(a.get_lnode(), b.get_lnode()) {
        return Ok(false);
    }
    if !iseq(a.get_rnode(), b.get_rnode()) {
        return Ok(false);
    }
    if !a.is_alltrue() && a.sign != b.sign {
        return Ok(false);
    }
    Ok(true)
}

/// `ltree_union(entryvec, &size)` (scalar opclass).
pub fn ltree_union(entries: &[(Vec<u8>, bool)], siglen: usize) -> PgResult<Vec<u8>> {
    let mut base = vec![0u8; siglen];
    let mut left: Option<Vec<u8>> = None;
    let mut right: Option<Vec<u8>> = None;
    let mut isalltrue = false;

    for (img, is_null) in entries {
        if *is_null {
            continue;
        }
        let cur = decode_key(img, siglen)?;
        if cur.is_onenode() {
            let curtree = &cur.lnode;
            hashing(&mut base, curtree, siglen);
            if left.as_ref().map_or(true, |l| op::ltree_compare(l, curtree) > 0) {
                left = Some(curtree.clone());
            }
            if right.as_ref().map_or(true, |r| op::ltree_compare(r, curtree) < 0) {
                right = Some(curtree.clone());
            }
        } else {
            if isalltrue || cur.is_alltrue() {
                isalltrue = true;
            } else {
                for i in 0..siglen {
                    base[i] |= cur.sign[i];
                }
            }
            let lt = cur.get_lnode();
            if left.as_ref().map_or(true, |l| op::ltree_compare(l, lt) > 0) {
                left = Some(lt.to_vec());
            }
            let rt = cur.get_rnode();
            if right.as_ref().map_or(true, |r| op::ltree_compare(r, rt) < 0) {
                right = Some(rt.to_vec());
            }
        }
    }

    if !isalltrue {
        isalltrue = base.iter().all(|&b| b == 0xff);
    }

    Ok(ltree_gist_alloc(
        isalltrue,
        Some(&base),
        siglen,
        left.as_deref(),
        right.as_deref(),
    ))
}

/// `ltree_penalty(origentry, newentry, &penalty)` (scalar opclass).
pub fn ltree_penalty(orig_image: &[u8], new_image: &[u8], siglen: usize) -> PgResult<f32> {
    let origval = decode_key(orig_image, siglen)?;
    let newval = decode_key(new_image, siglen)?;
    let cmpl = op::ltree_compare(origval.get_lnode(), newval.get_lnode());
    let cmpr = op::ltree_compare(newval.get_rnode(), origval.get_rnode());
    let penalty = cmpl.max(0) + cmpr.max(0);
    Ok(penalty as f32)
}

/// `ltree_picksplit(entryvec, &v)` (scalar opclass).
pub fn ltree_picksplit(
    entries: &[(Vec<u8>, bool)],
    siglen: usize,
) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)> {
    // entries indexed 1-based; index 0 is the placeholder slot.
    let n = entries.len();
    if n < 3 {
        return Err(PgError::error("ltree_picksplit: fewer than two entries to split"));
    }
    let maxoff = n - 1; // OffsetNumber maxoff = entryvec->n - 1.

    // decode all entries (1..=maxoff).
    let mut keys: Vec<Option<LtgKey>> = (0..=maxoff).map(|_| None).collect();
    for j in 1..=maxoff {
        let (img, is_null) = &entries[j];
        if *is_null {
            return Err(PgError::error("ltree_picksplit: NULL entry key"));
        }
        keys[j] = Some(decode_key(img, siglen)?);
    }

    // RIX array: array[j].index = j, array[j].r = LTG_GETLNODE(entry j).
    // Sort array[FirstOffsetNumber..=maxoff] by ltree_compare on the L node.
    let mut array: Vec<(usize, Vec<u8>)> = (1..=maxoff)
        .map(|j| (j, keys[j].as_ref().unwrap().get_lnode().to_vec()))
        .collect();
    array.sort_by(|a, b| {
        let c = op::ltree_compare(&a.1, &b.1);
        c.cmp(&0)
    });

    let mut spl_left: Vec<u16> = Vec::new();
    let mut spl_right: Vec<u16> = Vec::new();

    let mut ls = vec![0u8; siglen];
    let mut rs = vec![0u8; siglen];
    let mut lisat = false;
    let mut risat = false;
    let mut lu_r: Option<Vec<u8>> = None;
    let mut ru_r: Option<Vec<u8>> = None;

    // C: half = (maxoff - FirstOffsetNumber + 1) / 2; loop j over 1..=maxoff in
    // the SORTED order; j is the 1-based loop counter, the entry is array[j-1].
    let half = (maxoff as i32 - 1 + 1) / 2; // (maxoff - FirstOffsetNumber + 1)/2
    for (jcounter, (index, _r)) in array.iter().enumerate() {
        let j = (jcounter + 1) as i32; // FirstOffsetNumber == 1.
        let lu = keys[*index].as_ref().unwrap();
        if j <= half {
            spl_left.push(*index as u16);
            let rt = lu.get_rnode();
            if lu_r.as_ref().map_or(true, |x| op::ltree_compare(rt, x) > 0) {
                lu_r = Some(rt.to_vec());
            }
            if lu.is_onenode() {
                hashing(&mut ls, &lu.lnode, siglen);
            } else if lisat || lu.is_alltrue() {
                lisat = true;
            } else {
                for i in 0..siglen {
                    ls[i] |= lu.sign[i];
                }
            }
        } else {
            spl_right.push(*index as u16);
            let rt = lu.get_rnode();
            if ru_r.as_ref().map_or(true, |x| op::ltree_compare(rt, x) > 0) {
                ru_r = Some(rt.to_vec());
            }
            if lu.is_onenode() {
                hashing(&mut rs, &lu.lnode, siglen);
            } else if risat || lu.is_alltrue() {
                risat = true;
            } else {
                for i in 0..siglen {
                    rs[i] |= lu.sign[i];
                }
            }
        }
    }

    if !lisat {
        lisat = ls.iter().all(|&b| b == 0xff);
    }
    if !risat {
        risat = rs.iter().all(|&b| b == 0xff);
    }

    // lu_l = LTG_GETLNODE(entry array[FirstOffsetNumber].index)
    let lu_l = array[0].1.clone();
    let lu = ltree_gist_alloc(lisat, Some(&ls), siglen, Some(&lu_l), lu_r.as_deref());

    // ru_l = LTG_GETLNODE(entry array[1 + half].index)
    let ru_l_index = array[(1 + half) as usize - 1].0;
    let ru_l = keys[ru_l_index].as_ref().unwrap().get_lnode().to_vec();
    let ru = ltree_gist_alloc(risat, Some(&rs), siglen, Some(&ru_l), ru_r.as_deref());

    Ok((spl_left, spl_right, lu, ru))
}

// --- consistent helpers (ltree_gist.c) ------------------------------------

/// `gist_isparent(key, query, siglen)` — does `query` (some prefix of it) fall
/// within `[lnode, rnode]`?
fn gist_isparent(key: &LtgKey, query: &[u8]) -> bool {
    let numlevel = Ltree::new(query).numlevel() as i32;
    let lnode = key.get_lnode();
    let rnode = key.get_rnode();
    for i in (0..=numlevel).rev() {
        // query->numlevel = i — truncate the query to i levels.
        let q = truncate_ltree(query, i as usize);
        if op::ltree_compare(&q, lnode) >= 0 && op::ltree_compare(&q, rnode) <= 0 {
            return true;
        }
    }
    false
}

/// `gist_ischild(key, query, siglen)`.
fn gist_ischild(key: &LtgKey, query: &[u8]) -> bool {
    let qn = Ltree::new(query).numlevel();
    let mut left = key.get_lnode().to_vec();
    let mut right = key.get_rnode().to_vec();
    if Ltree::new(&left).numlevel() > qn {
        left = truncate_ltree(&left, qn);
    }
    let mut res = op::ltree_compare(query, &left) >= 0;
    if Ltree::new(&right).numlevel() > qn {
        right = truncate_ltree(&right, qn);
    }
    if res && op::ltree_compare(query, &right) > 0 {
        res = false;
    }
    res
}

/// Truncate an `ltree` image to its first `n` levels (rebuild a fresh image).
fn truncate_ltree(t: &[u8], n: usize) -> Vec<u8> {
    let tt = Ltree::new(t);
    let labels: Vec<&[u8]> = tt.levels().take(n).map(|l| l.name).collect();
    crate::repr::build_ltree(&labels)
}

/// `gist_qe(key, query, siglen)` (scalar opclass) — lquery signature test.
fn gist_qe(key: &LtgKey, query: &[u8], siglen: usize) -> bool {
    if key.is_alltrue() {
        return true;
    }
    let sign = &key.sign;
    for curq in Lquery::new(query).levels() {
        // if (curq->numvar && LQL_CANLOOKSIGN(curq))
        if curq.numvar() != 0 && flg_canlooksign((curq.flag() & 0xff) as u8) {
            let mut isexist = false;
            for v in curq.variants() {
                if getbit(sign, hashval(v.val, siglen)) {
                    isexist = true;
                    break;
                }
            }
            if !isexist {
                return false;
            }
        }
    }
    true
}

/// `gist_tqcmp(t, q)` — compare an ltree against the leading "good" levels of an
/// lquery.
fn gist_tqcmp(t: &[u8], q: &Lquery) -> i32 {
    let tt = Ltree::new(t);
    let an = tt.numlevel() as i32;
    let bn = q.firstgood() as i32;
    let mut tl = tt.levels();
    let mut ql = q.levels();
    let mut an_left = an;
    let mut bn_left = bn;
    while an_left > 0 && bn_left > 0 {
        let al = tl.next().unwrap();
        let qlvl = ql.next().unwrap();
        // bl = LQL_FIRST(ql) — the first variant of this level.
        let bl = qlvl.variants().next();
        let bname = bl.map(|v| v.name).unwrap_or(&[]);
        let minlen = al.name.len().min(bname.len());
        let res = al.name[..minlen].cmp(&bname[..minlen]);
        match res {
            core::cmp::Ordering::Equal => {
                if al.name.len() != bname.len() {
                    return al.name.len() as i32 - bname.len() as i32;
                }
            }
            core::cmp::Ordering::Less => return -1,
            core::cmp::Ordering::Greater => return 1,
        }
        an_left -= 1;
        bn_left -= 1;
    }
    an.min(bn) - bn
}

/// `gist_between(key, query, siglen)`.
fn gist_between(key: &LtgKey, query: &[u8]) -> bool {
    let q = Lquery::new(query);
    if q.firstgood() == 0 {
        return true;
    }
    if gist_tqcmp(key.get_lnode(), &q) > 0 {
        return false;
    }
    if gist_tqcmp(key.get_rnode(), &q) < 0 {
        return false;
    }
    true
}

/// `gist_qtxt(key, query, siglen)` (scalar opclass) — ltxtquery signature test.
fn gist_qtxt(key: &LtgKey, query: &[u8], siglen: usize) -> bool {
    if key.is_alltrue() {
        return true;
    }
    let sign = &key.sign;
    op::ltxtq_exec_sign(
        query,
        &flg_canlooksign,
        &|val| getbit(sign, hashval(val, siglen)),
    )
}

/// `arrq_cons(key, _query, siglen)` — `?` operator: any lquery in the array
/// matches the inner key (scalar opclass).
fn arrq_cons(key: &LtgKey, query_array: &[u8], siglen: usize) -> PgResult<bool> {
    let arr = LtreeArray::parse(query_array);
    arr.check_1d_no_nulls()?;
    for q in arr.elements() {
        if gist_qe(key, q, siglen) && gist_between(key, q) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `ltree_consistent(entry, query, strategy, subtype, recheck)`. The query rides
/// the by-ref lane (its payload is the header-ful varlena image). Returns
/// `(matched, recheck)`; all scalar-opclass cases are EXACT (`recheck = false`).
pub fn ltree_consistent(
    is_leaf: bool,
    key_image: &[u8],
    key_is_null: bool,
    query_image: &[u8],
    strategy: u16,
    siglen: usize,
) -> PgResult<(bool, bool)> {
    if key_is_null {
        return Err(PgError::error("ltree_consistent: NULL key"));
    }
    let key = decode_key(key_image, siglen)?;
    // The by-ref query image may carry a short/long varlena header from the
    // boundary; canonicalize to a plain 4-byte-header image the repr walkers
    // (which assume `VARSIZE = word >> 2`) read.
    let query = canonical_varlena(query_image);

    let res = match strategy {
        // BTLessStrategyNumber = 1
        1 => {
            if is_leaf {
                op::ltree_compare(&query, key.get_lnode()) > 0
            } else {
                op::ltree_compare(&query, key.get_lnode()) >= 0
            }
        }
        // BTLessEqualStrategyNumber = 2
        2 => op::ltree_compare(&query, key.get_lnode()) >= 0,
        // BTEqualStrategyNumber = 3
        3 => {
            if is_leaf {
                op::ltree_compare(&query, key.get_lnode()) == 0
            } else {
                op::ltree_compare(&query, key.get_lnode()) >= 0
                    && op::ltree_compare(&query, key.get_rnode()) <= 0
            }
        }
        // BTGreaterEqualStrategyNumber = 4
        4 => op::ltree_compare(&query, key.get_rnode()) <= 0,
        // BTGreaterStrategyNumber = 5
        5 => {
            if is_leaf {
                op::ltree_compare(&query, key.get_rnode()) < 0
            } else {
                op::ltree_compare(&query, key.get_rnode()) <= 0
            }
        }
        // 10: ltree @> query (isparent)
        10 => {
            if is_leaf {
                op::inner_isparent(&query, key.get_lnode())
            } else {
                gist_isparent(&key, &query)
            }
        }
        // 11: ltree <@ query (ischild)
        11 => {
            if is_leaf {
                op::inner_isparent(key.get_lnode(), &query)
            } else {
                gist_ischild(&key, &query)
            }
        }
        // 12, 13: ltree ~ lquery (ltq_regex / gist_qe & gist_between)
        12 | 13 => {
            if is_leaf {
                op::ltq_regex(key.get_lnode(), &query)?
            } else {
                gist_qe(&key, &query, siglen) && gist_between(&key, &query)
            }
        }
        // 14, 15: ltree @ ltxtquery
        14 | 15 => {
            if is_leaf {
                op::ltxtq_exec(key.get_lnode(), &query)
            } else {
                gist_qtxt(&key, &query, siglen)
            }
        }
        // 16, 17: ltree ? lquery[]
        16 | 17 => {
            if is_leaf {
                // lt_q_regex(node, array): any lquery in the array matches node.
                let arr = LtreeArray::parse(&query);
                arr.check_1d_no_nulls()?;
                let mut r = false;
                for q in arr.elements() {
                    if op::ltq_regex(key.get_lnode(), q)? {
                        r = true;
                        break;
                    }
                }
                r
            } else {
                arrq_cons(&key, &query, siglen)?
            }
        }
        other => {
            return Err(PgError::error(format!("unrecognized StrategyNumber: {other}")));
        }
    };
    // All cases served by this function are exact.
    Ok((res, false))
}

/// `ltree_gist_relopts_validator` — `siglen` must be a multiple of `ALIGNOF_INT`
/// (4). Reads `options->siglen` from the parsed `LtreeGistOptions` bytea. The
/// reloptions builder produces the full fixed-size struct INCLUDING the 4-byte
/// varlena header (`allocateReloptStruct` + `fillRelOptions`'s `SET_VARSIZE`),
/// exactly the `void *parsed_options` C's validator receives, so `siglen` sits
/// at byte offset `offsetof(LtreeGistOptions, siglen) = 4`.
pub fn ltree_gist_relopts_validator(parsed: &mut [u8]) -> Result<(), String> {
    let siglen = read_options_siglen(parsed);
    if siglen != intalign(siglen as usize) as i32 {
        return Err("siglen value must be a multiple of 4".to_string());
    }
    Ok(())
}

/// Read the `siglen` field out of a parsed `LtreeGistOptions` bytea at
/// `offsetof(LtreeGistOptions, siglen)` (after the 4-byte varlena header).
fn read_options_siglen(parsed: &[u8]) -> i32 {
    let off = OFFSETOF_LTREE_GIST_OPTIONS_SIGLEN as usize;
    if parsed.len() >= off + 4 {
        i32::from_ne_bytes([parsed[off], parsed[off + 1], parsed[off + 2], parsed[off + 3]])
    } else {
        LTREE_SIGLEN_DEFAULT
    }
}

// ===========================================================================
// gist__ltree_ops (_ltree_gist.c) — the ltree[] array opclass.
// ===========================================================================

/// `hashing(sign, t, siglen)` (array opclass — AHASH, same math here as the
/// scalar one since ASIGLENBIT == SIGLENBIT).
fn ahashing(sign: &mut [u8], t: &[u8], siglen: usize) {
    hashing(sign, t, siglen)
}

/// `_ltree_compress(entry)` (array opclass). A leaf `ltree[]` becomes a
/// signature key; an all-0xff inner signature rewrites to ALLTRUE; else
/// pass-through.
pub fn array_compress(
    leafkey: bool,
    key_image: &[u8],
    key_is_null: bool,
    siglen: usize,
) -> PgResult<Option<Vec<u8>>> {
    if leafkey {
        if key_is_null {
            return Err(PgError::error("_ltree_compress: NULL leaf key"));
        }
        let arr = LtreeArray::parse(key_image);
        if arr.ndim() > 1 {
            return Err(PgError::error("array must be one-dimensional")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
        if arr.contains_nulls() {
            return Err(PgError::error("array must not contain nulls")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        // key = ltree_gist_alloc(false, NULL, siglen, NULL, NULL); hash each item.
        let mut sign = vec![0u8; siglen];
        for item in arr.elements() {
            ahashing(&mut sign, item, siglen);
        }
        return Ok(Some(ltree_gist_alloc(false, Some(&sign), siglen, None, None)));
    }
    // inner, !ALLTRUE: rewrite to ALLTRUE iff every signature byte is 0xff.
    let key = decode_key(key_image, siglen)?;
    if key.is_alltrue() {
        return Ok(None);
    }
    if key.sign.iter().all(|&b| b == 0xff) {
        return Ok(Some(ltree_gist_alloc(true, Some(&key.sign), siglen, None, None)));
    }
    Ok(None)
}

/// `_ltree_same(a, b, &result)` (array opclass).
pub fn array_same(a_image: &[u8], b_image: &[u8], siglen: usize) -> PgResult<bool> {
    let a = decode_key(a_image, siglen)?;
    let b = decode_key(b_image, siglen)?;
    let res = if a.is_alltrue() && b.is_alltrue() {
        true
    } else if a.is_alltrue() || b.is_alltrue() {
        false
    } else {
        a.sign == b.sign
    };
    Ok(res)
}

/// `_ltree_union(entryvec, &size)` (array opclass).
pub fn array_union(entries: &[(Vec<u8>, bool)], siglen: usize) -> PgResult<Vec<u8>> {
    let mut base = vec![0u8; siglen];
    let mut isalltrue = false;
    for (img, is_null) in entries {
        if *is_null {
            continue;
        }
        let add = decode_key(img, siglen)?;
        if add.is_alltrue() {
            isalltrue = true;
            break;
        }
        for i in 0..siglen {
            base[i] |= add.sign[i];
        }
    }
    if isalltrue {
        Ok(ltree_gist_alloc(true, Some(&base), siglen, None, None))
    } else {
        Ok(ltree_gist_alloc(false, Some(&base), siglen, None, None))
    }
}

/// `hemdist(a, b, siglen)` (array opclass).
fn ahemdist(a: &LtgKey, b: &LtgKey, siglen: usize) -> i32 {
    if a.is_alltrue() {
        if b.is_alltrue() {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(&b.sign)
        }
    } else if b.is_alltrue() {
        siglenbit(siglen) as i32 - sizebitvec(&a.sign)
    } else {
        hemdistsign(&a.sign, &b.sign, siglen)
    }
}

/// `_ltree_penalty(origentry, newentry, &penalty)` (array opclass).
pub fn array_penalty(orig_image: &[u8], new_image: &[u8], siglen: usize) -> PgResult<f32> {
    let origval = decode_key(orig_image, siglen)?;
    let newval = decode_key(new_image, siglen)?;
    Ok(ahemdist(&origval, &newval, siglen) as f32)
}

/// `WISH_F(a, b, c)`.
fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

/// `_ltree_picksplit(entryvec, &v)` (array opclass).
pub fn array_picksplit(
    entries: &[(Vec<u8>, bool)],
    siglen: usize,
) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)> {
    // C: maxoff = entryvec->n - 2; entries indexed 1-based; index 0 placeholder.
    let n = entries.len();
    if n < 3 {
        return Err(PgError::error("_ltree_picksplit: fewer than two entries to split"));
    }
    let maxoff_seed = n.saturating_sub(2); // entryvec->n - 2

    // decode 1..=(n-1).
    let mut keys: Vec<Option<LtgKey>> = (0..n).map(|_| None).collect();
    for j in 1..n {
        let (img, is_null) = &entries[j];
        if *is_null {
            return Err(PgError::error("_ltree_picksplit: NULL entry key"));
        }
        keys[j] = Some(decode_key(img, siglen)?);
    }

    // find the two furthest-apart seeds over [FirstOffsetNumber, maxoff_seed].
    let mut waste = -1i32;
    let mut seed_1 = 0usize;
    let mut seed_2 = 0usize;
    for k in 1..maxoff_seed {
        for j in (k + 1)..=maxoff_seed {
            let sw = ahemdist(keys[k].as_ref().unwrap(), keys[j].as_ref().unwrap(), siglen);
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

    // initial datum_l/datum_r from the seeds (signature-only keys).
    let datum_l_allistrue = keys[seed_1].as_ref().unwrap().is_alltrue();
    let mut union_l = if datum_l_allistrue {
        vec![0u8; siglen]
    } else {
        keys[seed_1].as_ref().unwrap().sign.clone()
    };
    let datum_r_allistrue = keys[seed_2].as_ref().unwrap().is_alltrue();
    let mut union_r = if datum_r_allistrue {
        vec![0u8; siglen]
    } else {
        keys[seed_2].as_ref().unwrap().sign.clone()
    };

    // maxoff = OffsetNumberNext(maxoff_seed) = entryvec->n - 1.
    let maxoff = maxoff_seed + 1;

    // cost vector over [1, maxoff], sorted ascending by abs(alpha-beta).
    let seed_l = make_seed_key(datum_l_allistrue, &union_l);
    let seed_r = make_seed_key(datum_r_allistrue, &union_r);
    let mut costvector: Vec<(usize, i32)> = Vec::with_capacity(maxoff);
    for j in 1..=maxoff {
        let kj = keys[j].as_ref().unwrap();
        let alpha = ahemdist(&seed_l, kj, siglen);
        let beta = ahemdist(&seed_r, kj, siglen);
        costvector.push((j, (alpha - beta).abs()));
    }
    costvector.sort_by(|a, b| a.1.cmp(&b.1));

    let mut spl_left: Vec<u16> = Vec::new();
    let mut spl_right: Vec<u16> = Vec::new();
    let mut l_allistrue = datum_l_allistrue;
    let mut r_allistrue = datum_r_allistrue;

    for (j, _cost) in costvector.iter().copied() {
        if j == seed_1 {
            spl_left.push(j as u16);
            continue;
        } else if j == seed_2 {
            spl_right.push(j as u16);
            continue;
        }
        let kj = keys[j].as_ref().unwrap();
        let cur_l = make_seed_key(l_allistrue, &union_l);
        let cur_r = make_seed_key(r_allistrue, &union_r);
        let alpha = ahemdist(&cur_l, kj, siglen);
        let beta = ahemdist(&cur_r, kj, siglen);

        if (alpha as f64)
            < beta as f64 + wish_f(spl_left.len() as i32, spl_right.len() as i32, 0.00001)
        {
            if l_allistrue || kj.is_alltrue() {
                if !l_allistrue {
                    for b in union_l.iter_mut() {
                        *b = 0xff;
                    }
                    l_allistrue = true;
                }
            } else {
                for i in 0..siglen {
                    union_l[i] |= kj.sign[i];
                }
            }
            spl_left.push(j as u16);
        } else {
            if r_allistrue || kj.is_alltrue() {
                if !r_allistrue {
                    for b in union_r.iter_mut() {
                        *b = 0xff;
                    }
                    r_allistrue = true;
                }
            } else {
                for i in 0..siglen {
                    union_r[i] |= kj.sign[i];
                }
            }
            spl_right.push(j as u16);
        }
    }

    let ldatum = ltree_gist_alloc(datum_l_allistrue, Some(&union_l), siglen, None, None);
    let rdatum = ltree_gist_alloc(datum_r_allistrue, Some(&union_r), siglen, None, None);
    Ok((spl_left, spl_right, ldatum, rdatum))
}

/// A synthetic signature-only key for `ahemdist` math during picksplit.
fn make_seed_key(allistrue: bool, sign: &[u8]) -> LtgKey {
    LtgKey {
        flag: if allistrue { LTG_ALLTRUE } else { 0 },
        lnode: Vec::new(),
        rnode: Vec::new(),
        sign: sign.to_vec(),
    }
}

// --- array consistent helpers (_ltree_gist.c) -----------------------------

/// `gist_te(key, query, siglen)` — `ltree[] @> ltree` / `<@`: every level CRC of
/// `query` is present in `key`'s signature.
fn gist_te(key: &LtgKey, query: &[u8], siglen: usize) -> bool {
    if key.is_alltrue() {
        return true;
    }
    let sign = &key.sign;
    for lvl in Ltree::new(query).levels() {
        let hv = ltree_crc32_sz(lvl.name) as i32;
        if !getbit(sign, hashval(hv, siglen)) {
            return false;
        }
    }
    true
}

/// `_arrq_cons(key, _query, siglen)` — `?` operator (array opclass).
fn array_arrq_cons(key: &LtgKey, query_array: &[u8], siglen: usize) -> PgResult<bool> {
    let arr = LtreeArray::parse(query_array);
    arr.check_1d_no_nulls()?;
    for q in arr.elements() {
        if gist_qe(key, q, siglen) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `_ltree_consistent(entry, query, strategy, subtype, recheck)` (array
/// opclass). All cases are INEXACT (`recheck = true`).
pub fn array_consistent(
    key_image: &[u8],
    key_is_null: bool,
    query_image: &[u8],
    strategy: u16,
    siglen: usize,
) -> PgResult<(bool, bool)> {
    if key_is_null {
        return Err(PgError::error("_ltree_consistent: NULL key"));
    }
    let key = decode_key(key_image, siglen)?;
    let query = canonical_varlena(query_image);

    let res = match strategy {
        10 | 11 => gist_te(&key, &query, siglen),
        12 | 13 => gist_qe(&key, &query, siglen),
        14 | 15 => gist_qtxt(&key, &query, siglen),
        16 | 17 => array_arrq_cons(&key, &query, siglen)?,
        other => {
            return Err(PgError::error(format!("unrecognized StrategyNumber: {other}")));
        }
    };
    Ok((res, true))
}

// ===========================================================================
// options procs.
// ===========================================================================

/// `ltree_gist_options(relopts)` — register `siglen` (default 8, range [4, 2024],
/// multiple-of-4 validator).
pub fn ltree_gist_options(relopts: &mut ::types_reloptions::local_relopts) {
    ::reloptions_seams::init_local_reloptions::call(relopts, SIZEOF_LTREE_GIST_OPTIONS);
    ::reloptions_seams::add_local_int_reloption::call(
        relopts,
        "siglen",
        Some("signature length in bytes"),
        LTREE_SIGLEN_DEFAULT,
        SIGLEN_MIN_INTALIGNED,
        LTREE_SIGLEN_MAX,
        OFFSETOF_LTREE_GIST_OPTIONS_SIGLEN,
    );
    ::reloptions_seams::register_reloptions_validator::call(
        relopts,
        ltree_gist_relopts_validator,
    );
}

/// `_ltree_gist_options(relopts)` — register `siglen` (default 28, range
/// [1, 2024], no multiple-of-4 validator — C uses `1` as the min and registers
/// NO validator for the array opclass).
pub fn array_gist_options(relopts: &mut ::types_reloptions::local_relopts) {
    ::reloptions_seams::init_local_reloptions::call(relopts, SIZEOF_LTREE_GIST_OPTIONS);
    ::reloptions_seams::add_local_int_reloption::call(
        relopts,
        "siglen",
        Some("signature length"),
        LTREE_ASIGLEN_DEFAULT,
        1,
        LTREE_SIGLEN_MAX,
        OFFSETOF_LTREE_GIST_OPTIONS_SIGLEN,
    );
}

// ===========================================================================
// shared helpers.
// ===========================================================================

/// Canonicalize a by-ref varlena image to a plain 4-byte-header image the repr
/// walkers (which read `VARSIZE = word >> 2`) expect. The boundary normalizes
/// args to the 4-byte inline form, so this is the identity for those; it only
/// rewrites a 1-byte "short" header if one survives (defensive).
fn canonical_varlena(image: &[u8]) -> Vec<u8> {
    match image.first() {
        // 1-byte short header (VARATT_IS_1B): byte 0 has low bit set, byte 0 != 0x01.
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => {
            let len = (h >> 1) as usize; // VARSIZE_1B
            let payload = &image[1..len.max(1)];
            let total = payload.len() + VARHDRSZ;
            let mut out = vec![0u8; total];
            set_varsize(&mut out, total);
            out[VARHDRSZ..].copy_from_slice(payload);
            out
        }
        _ => image.to_vec(),
    }
}
