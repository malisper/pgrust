//! GiST `tsvector_ops` key vocabulary (`tsgistidx.c:63`): the `SignTSVector`
//! index key and the owned `picksplit` result.
//!
//! C packs all three key forms into the varlena `data[]`, distinguished by the
//! `flag` bits, and reads them back with `GETARR`/`GETSIGN`/`ARRNELEM`/
//! `GETSIGLEN`. The owned rewrite keeps `flag` and replaces `data[]` with a
//! [`SignTsVectorData`] enum: `Vec<i32>` hashes for `ARRKEY`, `Vec<u8>` byte
//! signature for a plain `SIGNKEY`, no payload for `ALLISTRUE`.

use alloc::vec::Vec;

/// `ARRKEY` (tsgistidx.c:70) — sorted array of lexeme hashes (leaf key).
pub const ARRKEY: i32 = 0x01;
/// `SIGNKEY` (tsgistidx.c:71) — bit signature (inner key).
pub const SIGNKEY: i32 = 0x02;
/// `ALLISTRUE` (tsgistidx.c:72) — every signature bit set (`SIGNKEY` shortcut).
pub const ALLISTRUE: i32 = 0x04;

/// The payload of a [`SignTsVector`], selected by its `flag`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignTsVectorData {
    /// `ARRKEY`: the sorted array of lexeme-CRC `int32` hashes.
    Arr(Vec<i32>),
    /// plain `SIGNKEY`: the `siglen`-byte bit signature.
    Sign(Vec<u8>),
    /// `SIGNKEY | ALLISTRUE`: no payload.
    AllTrue,
}

/// `SignTSVector` (tsgistidx.c:63) — the GiST index key for `tsvector_ops`.
///
/// ```c
/// typedef struct {
///     int32 vl_len_;
///     int32 flag;
///     char  data[FLEXIBLE_ARRAY_MEMBER];
/// } SignTSVector;
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignTsVector {
    /// `flag` — a bitmask of [`ARRKEY`] / [`SIGNKEY`] / [`ALLISTRUE`].
    pub flag: i32,
    /// The typed payload.
    pub data: SignTsVectorData,
}

impl SignTsVector {
    /// `ISARRKEY(x)` (tsgistidx.c:74).
    #[inline]
    pub fn is_arrkey(&self) -> bool {
        self.flag & ARRKEY != 0
    }
    /// `ISSIGNKEY(x)` (tsgistidx.c:75).
    #[inline]
    pub fn is_signkey(&self) -> bool {
        self.flag & SIGNKEY != 0
    }
    /// `ISALLTRUE(x)` (tsgistidx.c:76).
    #[inline]
    pub fn is_alltrue(&self) -> bool {
        self.flag & ALLISTRUE != 0
    }
    /// `GETARR(x)`/`ARRNELEM(x)` — the `int32` hashes; `ARRKEY` only.
    #[inline]
    pub fn arr(&self) -> &[i32] {
        match &self.data {
            SignTsVectorData::Arr(a) => a,
            _ => &[],
        }
    }
    /// `GETSIGN(x)` — the byte bit-signature; plain `SIGNKEY` only.
    #[inline]
    pub fn sign(&self) -> &[u8] {
        match &self.data {
            SignTsVectorData::Sign(s) => s,
            _ => &[],
        }
    }
    /// Mutable `GETSIGN(x)`.
    #[inline]
    pub fn sign_mut(&mut self) -> &mut [u8] {
        match &mut self.data {
            SignTsVectorData::Sign(s) => s,
            _ => &mut [],
        }
    }
}

/// The owned result of `gtsvector_picksplit`, replacing the C `GIST_SPLITVEC`'s
/// `spl_left`/`spl_right`/`spl_nleft`/`spl_nright`/`spl_ldatum`/`spl_rdatum`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PickSplitResult {
    /// `v->spl_left` (`spl_nleft == spl_left.len()`).
    pub spl_left: Vec<u16>,
    /// `v->spl_right` (`spl_nright == spl_right.len()`).
    pub spl_right: Vec<u16>,
    /// `v->spl_ldatum` — the left union key.
    pub spl_ldatum: Option<SignTsVector>,
    /// `v->spl_rdatum` — the right union key.
    pub spl_rdatum: Option<SignTsVector>,
}

/// One detoasted `tsvector` lexeme handed to `gtsvector_compress`'s leaf branch:
/// the lexeme's raw bytes (the C `words + ptr->pos`, length `ptr->len`).
pub type LexemeBytes<'a> = &'a [u8];
