//! _int_tool.c: 1-D int4 array image helpers and sorted-set primitives.

use types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use types_tuple::varatt;

const INT4OID: i32 = 23;

/// Full varlena array image as native-endian i32 words (every field of a
/// non-toasted ArrayType is 4-byte-aligned, incl. MAXALIGNed data offsets).
pub struct IntArray {
    w: Vec<i32>,
}

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

fn overhead_nonulls_words(ndim: usize) -> usize {
    maxalign(16 + 8 * ndim) / 4
}

impl IntArray {
    pub fn new(num: usize) -> IntArray {
        if num == 0 {
            return IntArray::empty();
        }
        let hdr = overhead_nonulls_words(1);
        let nbytes = (hdr + num) * 4;
        let mut w = vec![0i32; hdr + num];
        w[0] = varatt::set_varsize_4b_word(nbytes as u32) as i32;
        w[1] = 1;
        w[2] = 0;
        w[3] = INT4OID;
        w[4] = num as i32;
        w[5] = 1;
        IntArray { w }
    }

    /// construct_empty_array(INT4OID): the zero-dimensional array.
    pub fn empty() -> IntArray {
        let mut w = vec![0i32; 4];
        w[0] = varatt::set_varsize_4b_word(16) as i32;
        w[3] = INT4OID;
        IntArray { w }
    }

    /// PG_GETARG_ARRAYTYPE_P(_COPY): owned copy of a detoasted 4B-header
    /// image, dims/null-bitmap preserved.
    pub fn from_image(image: &[u8]) -> IntArray {
        debug_assert!(image.len() % 4 == 0 && image.len() >= 16);
        let mut w = vec![0i32; image.len() / 4];
        // SAFETY: plain byte copy into the i32 backing store.
        unsafe {
            core::ptr::copy_nonoverlapping(
                image.as_ptr(),
                w.as_mut_ptr().cast::<u8>(),
                image.len(),
            );
        }
        IntArray { w }
    }

    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: the words are the varlena image; byte view of the same
        // allocation.
        unsafe { core::slice::from_raw_parts(self.w.as_ptr().cast::<u8>(), self.w.len() * 4) }
    }

    pub fn ndim(&self) -> usize {
        self.w[1] as usize
    }

    fn dataoffset(&self) -> i32 {
        self.w[2]
    }

    pub fn dims(&self) -> &[i32] {
        &self.w[4..4 + self.ndim()]
    }

    fn data_off_words(&self) -> usize {
        if self.dataoffset() != 0 {
            self.dataoffset() as usize / 4
        } else {
            overhead_nonulls_words(self.ndim())
        }
    }

    /// ARRNELEMS: ArrayGetNItems over the dims.
    pub fn nelems(&self) -> usize {
        let mut n: i64 = 1;
        if self.ndim() == 0 {
            return 0;
        }
        for &d in self.dims() {
            n *= d as i64;
        }
        n.max(0) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.nelems() == 0
    }

    // Both accessors clamp to the physical image: a null-bearing array
    // stores fewer than nelems() values, and every caller rejects those via
    // check_valid() first (C reads out of bounds there).
    pub fn elems(&self) -> &[i32] {
        let off = self.data_off_words().min(self.w.len());
        let end = (off + self.nelems()).min(self.w.len());
        &self.w[off..end]
    }

    pub fn elems_mut(&mut self) -> &mut [i32] {
        let off = self.data_off_words().min(self.w.len());
        let end = (off + self.nelems()).min(self.w.len());
        &mut self.w[off..end]
    }

    fn has_null(&self) -> bool {
        if self.dataoffset() == 0 {
            return false;
        }
        // Null bitmap starts right after the dims/lbound words.
        let bitmap = &self.as_bytes()[16 + 8 * self.ndim()..];
        let nitems = self.nelems();
        for i in 0..nitems {
            if bitmap[i / 8] & (1 << (i % 8)) == 0 {
                return true;
            }
        }
        false
    }

    /// CHECKARRVALID.
    pub fn check_valid(&self) -> PgResult<()> {
        if self.has_null() {
            return Err(PgError::error("array must not contain nulls")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .into());
        }
        Ok(())
    }

    /// resize_intArrayType, including C's quirky multi-dim dims rewrite.
    pub fn resize(&mut self, num: usize) -> &mut Self {
        if num == 0 {
            *self = IntArray::empty();
            return self;
        }
        if num != self.nelems() {
            let off = self.data_off_words();
            self.w.resize(off + num, 0);
            let nbytes = (off + num) * 4;
            self.w[0] = varatt::set_varsize_4b_word(nbytes as u32) as i32;
            let mut n = num as i32;
            for i in 0..self.ndim() {
                self.w[4 + i] = n;
                n = 1;
            }
        }
        self
    }

    /// copy_intArrayType: fresh 1-D array with the flat element data.
    pub fn copy_flat(&self) -> IntArray {
        let mut r = IntArray::new(self.nelems());
        if !self.is_empty() {
            r.elems_mut().copy_from_slice(self.elems());
        }
        r
    }

    pub fn sort(&mut self, ascending: bool) {
        if ascending {
            self.elems_mut().sort_unstable();
        } else {
            self.elems_mut().sort_unstable_by(|a, b| b.cmp(a));
        }
    }

    /// _int_unique: in-place adjacent dedup of a sorted array + resize.
    pub fn unique(&mut self) {
        let e = self.elems_mut();
        let mut num = e.len();
        if num > 1 {
            let mut w = 0usize;
            for r in 1..num {
                if e[r] != e[w] {
                    w += 1;
                    e[w] = e[r];
                }
            }
            num = w + 1;
        }
        self.resize(num);
    }

    /// PREPAREARR.
    pub fn prepare(&mut self) {
        self.sort(true);
        self.unique();
    }
}

pub fn inner_int_contains(a: &[i32], b: &[i32]) -> bool {
    let (na, nb) = (a.len(), b.len());
    let (mut i, mut j, mut n) = (0usize, 0usize, 0usize);
    while i < na && j < nb {
        if a[i] < b[j] {
            i += 1;
        } else if a[i] == b[j] {
            n += 1;
            i += 1;
            j += 1;
        } else {
            break;
        }
    }
    n == nb
}

pub fn inner_int_overlap(a: &[i32], b: &[i32]) -> bool {
    let (na, nb) = (a.len(), b.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < na && j < nb {
        if a[i] < b[j] {
            i += 1;
        } else if a[i] == b[j] {
            return true;
        } else {
            j += 1;
        }
    }
    false
}

pub fn inner_int_union(a: &IntArray, b: &IntArray) -> PgResult<IntArray> {
    a.check_valid()?;
    b.check_valid()?;
    let mut r = if a.is_empty() && b.is_empty() {
        IntArray::empty()
    } else if a.is_empty() {
        b.copy_flat()
    } else if b.is_empty() {
        a.copy_flat()
    } else {
        let (da, db) = (a.elems(), b.elems());
        let mut r = IntArray::new(da.len() + db.len());
        let mut k = 0usize;
        {
            let dr = r.elems_mut();
            let (mut i, mut j) = (0usize, 0usize);
            while i < da.len() && j < db.len() {
                if da[i] == db[j] {
                    dr[k] = da[i];
                    i += 1;
                    j += 1;
                } else if da[i] < db[j] {
                    dr[k] = da[i];
                    i += 1;
                } else {
                    dr[k] = db[j];
                    j += 1;
                }
                k += 1;
            }
            while i < da.len() {
                dr[k] = da[i];
                i += 1;
                k += 1;
            }
            while j < db.len() {
                dr[k] = db[j];
                j += 1;
                k += 1;
            }
        }
        r.resize(k);
        r
    };
    if r.nelems() > 1 {
        r.unique();
    }
    Ok(r)
}

pub fn inner_int_inter(a: &IntArray, b: &IntArray) -> IntArray {
    if a.is_empty() || b.is_empty() {
        return IntArray::empty();
    }
    let (da, db) = (a.elems(), b.elems());
    let mut r = IntArray::new(da.len().min(db.len()));
    let mut k = 0usize;
    {
        let dr = r.elems_mut();
        let (mut i, mut j) = (0usize, 0usize);
        while i < da.len() && j < db.len() {
            if da[i] < db[j] {
                i += 1;
            } else if da[i] == db[j] {
                if k == 0 || dr[k - 1] != db[j] {
                    dr[k] = db[j];
                    k += 1;
                }
                i += 1;
                j += 1;
            } else {
                j += 1;
            }
        }
    }
    if k == 0 {
        IntArray::empty()
    } else {
        r.resize(k);
        r
    }
}

/// internal_size: decoded cardinality of a range-compressed key; -1 on
/// int32 overflow.
pub fn internal_size(a: &[i32]) -> i32 {
    let mut size: i64 = 0;
    let mut i = 0usize;
    while i < a.len() {
        if i == 0 || a[i] != a[i - 1] {
            size += a[i + 1] as i64 - a[i] as i64 + 1;
        }
        i += 2;
    }
    if size > i32::MAX as i64 || size < i32::MIN as i64 {
        return -1;
    }
    size as i32
}

pub fn hashval(val: i32, siglen: usize) -> usize {
    (val as u32 as usize) % (siglen * 8)
}

pub fn getbit(sign: &[u8], i: usize) -> bool {
    (sign[i / 8] >> (i % 8)) & 1 != 0
}

pub fn hash_into(sign: &mut [u8], val: i32, siglen: usize) {
    let i = hashval(val, siglen);
    sign[i / 8] |= 1 << (i % 8);
}

pub fn gensign(sign: &mut [u8], a: &[i32], siglen: usize) {
    for &v in a {
        hash_into(sign, v, siglen);
    }
}
