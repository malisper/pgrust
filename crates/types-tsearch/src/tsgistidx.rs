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

    /// Decode a `SignTSVector` varlena body — the bytes AFTER the 4-byte varlena
    /// header (`VARDATA_ANY`), i.e. `flag` (`int32`) followed by `data[]`.
    /// `body_len` is `VARSIZE_ANY_EXHDR(x)` (`VARSIZE - VARHDRSZ`), so the
    /// `data[]` payload length is `body_len - sizeof(int32)`. C reads this with
    /// `ARRNELEM(x)` = `(VARSIZE(x) - GTHDRSIZE) / sizeof(int32)` for an `ARRKEY`
    /// and `GETSIGLEN(x)` = `VARSIZE(x) - GTHDRSIZE` for a `SIGNKEY`.
    ///
    /// Returns `None` if the body is too short to hold the `flag` word or the
    /// `ARRKEY` payload is not a whole number of `int32`s (a corrupt key).
    pub fn from_image(body: &[u8]) -> Option<SignTsVector> {
        const I32: usize = core::mem::size_of::<i32>();
        if body.len() < I32 {
            return None;
        }
        let flag = i32::from_ne_bytes([body[0], body[1], body[2], body[3]]);
        let payload = &body[I32..];
        let data = if flag & ARRKEY != 0 {
            if payload.len() % I32 != 0 {
                return None;
            }
            let mut arr = Vec::with_capacity(payload.len() / I32);
            for chunk in payload.chunks_exact(I32) {
                arr.push(i32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            SignTsVectorData::Arr(arr)
        } else if flag & ALLISTRUE != 0 {
            SignTsVectorData::AllTrue
        } else {
            SignTsVectorData::Sign(payload.to_vec())
        };
        Some(SignTsVector { flag, data })
    }

    /// Encode this key as a complete `SignTSVector` varlena image: the 4-byte
    /// `vl_len_` header (`SET_VARSIZE`) followed by `flag` (`int32`) and the
    /// `data[]` payload. Mirrors the C `palloc(CALCGTSIZE(flag, len))` /
    /// `SET_VARSIZE` layout exactly; native-endian `int32`s match the on-disk /
    /// in-memory form the C side reads back with `GETARR`/`GETSIGN`.
    pub fn to_image(&self) -> Vec<u8> {
        const I32: usize = core::mem::size_of::<i32>();
        let payload_len = match &self.data {
            SignTsVectorData::Arr(a) => a.len() * I32,
            SignTsVectorData::Sign(s) => s.len(),
            SignTsVectorData::AllTrue => 0,
        };
        let total = 4 + I32 + payload_len;
        let mut out = Vec::with_capacity(total);
        // SET_VARSIZE(res, total): a real 4-byte ("4B-U") varlena length header.
        // The on-disk / fmgr varlena convention stores the total byte length in
        // the high 30 bits (`len << 2`), with the low 2 bits the 4B-uncompressed
        // tag (`00`); `VARSIZE_4B` reads it back as `word >> 2`. The GiST index
        // storage (`index_form_tuple`) reads this header to size the key, so it
        // must be the real shifted form, not the bare total.
        out.extend_from_slice(&((total as i32) << 2).to_ne_bytes());
        out.extend_from_slice(&self.flag.to_ne_bytes());
        match &self.data {
            SignTsVectorData::Arr(a) => {
                for &v in a {
                    out.extend_from_slice(&v.to_ne_bytes());
                }
            }
            SignTsVectorData::Sign(s) => out.extend_from_slice(s),
            SignTsVectorData::AllTrue => {}
        }
        out
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn roundtrip(v: &SignTsVector) {
        let img = v.to_image();
        // The 4-byte header records the full image length as a real 4B-U varlena
        // header (`SET_VARSIZE` = `len << 2`); `VARSIZE_4B` reads it `word >> 2`.
        let total = (i32::from_ne_bytes([img[0], img[1], img[2], img[3]]) >> 2) as usize;
        assert_eq!(total, img.len());
        // from_image consumes the body (header stripped), as the fmgr by-ref lane
        // hands it after VARDATA.
        let back = SignTsVector::from_image(&img[4..]).expect("decode");
        assert_eq!(*v, back);
    }

    #[test]
    fn arrkey_roundtrips() {
        roundtrip(&SignTsVector {
            flag: ARRKEY,
            data: SignTsVectorData::Arr(vec![-5, 0, 7, 1024]),
        });
    }

    #[test]
    fn signkey_roundtrips() {
        roundtrip(&SignTsVector {
            flag: SIGNKEY,
            data: SignTsVectorData::Sign(vec![0x00, 0xff, 0xa5, 0x10]),
        });
    }

    #[test]
    fn alltrue_roundtrips() {
        roundtrip(&SignTsVector {
            flag: SIGNKEY | ALLISTRUE,
            data: SignTsVectorData::AllTrue,
        });
    }

    #[test]
    fn arrkey_layout_matches_calcgtsize() {
        // CALCGTSIZE(ARRKEY, n) = VARHDRSZ + sizeof(int32) /*flag*/ + n*sizeof(int32).
        let v = SignTsVector {
            flag: ARRKEY,
            data: SignTsVectorData::Arr(vec![1, 2, 3]),
        };
        assert_eq!(v.to_image().len(), 4 + 4 + 3 * 4);
    }

    #[test]
    fn truncated_body_is_none() {
        assert!(SignTsVector::from_image(&[0u8; 2]).is_none());
        // ARRKEY payload not a whole number of int32s.
        let mut img = SignTsVector {
            flag: ARRKEY,
            data: SignTsVectorData::Arr(vec![9]),
        }
        .to_image();
        img.pop();
        assert!(SignTsVector::from_image(&img[4..]).is_none());
    }
}
