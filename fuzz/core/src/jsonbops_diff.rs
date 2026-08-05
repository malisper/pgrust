//! jsonbops_diff: differential fuzz driver — shipped Rust `adt_jsonb`
//! two-doc ops/mutate/getfield surface vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C (csrc/pg_jsonbops.c +
//! csrc/jsonbfam/). Sibling of jsonbio_diff (shares the C family, the
//! errcode-class contract, and the driver helpers).
//!
//! Crate under test: crates/backend/utils/adt/jsonb (ops.rs, mutate.rs,
//! getfield.rs, tojsonb.rs jsonb_object forms, builtins.rs wrappers, plus
//! container.rs/iter.rs/build.rs they drive). The RUST SIDE RUNS THE
//! SHIPPED fc_* WRAPPERS on a native LocalFcinfo frame.
//!
//! Comparison planes: value bytes (result container images, text results,
//! bool/i32/i64 exact), SQL-NULL-ness, error-verdict, errcode/sqlstate
//! class. Message text out of scope.
//!
//! Errcode classes as jsonbio_diff plus 10 = 2202E (array subscript),
//! 11 = 22004 (null value not allowed). C parse rc 100+class (doc a) /
//! 200+class (doc b / newval) assert as divergences: the Rust arms only
//! drive an entry after their OWN fc_jsonb_in parses succeeded.
//!
//! SESSION PINS (mirroring the C oracle's shims, both installed once per
//! process): UTF8 database encoding; DATABASE COLLATION C —
//! pg_locale_seams::varstr_cmp_locale := varlena::varstrfastcmp_c, exactly
//! the collate_is_c arm the C oracle's pg_newlocale_from_collation shim
//! pins (compareJsonbScalarValue passes DEFAULT_COLLATION_OID on both
//! sides); real detoast installed for the text[]-argument reads.
//!
//! text[] ARGUMENTS: the driver builds ONE flat PG ArrayType image per
//! iteration (1-D, optional NULL elements via the null bitmap; 2-D {n,2}
//! and {n,1} shapes for the jsonb_object dimension checks) and hands the
//! SAME bytes to both sides — array construction is environment, the
//! deconstruction under test is each side's own (vendored
//! deconstruct_array_builtin vs arrayfuncs::deconstruct_array_builtin).
//!
//! Input layout:
//!   [sel][flags][aux: i32 LE][u16 l1][s1][u16 l2][s2][u16 l3][s3]
//!   [u16 ld][doc1][rest = doc2]
//! sel % 11 = arm; flags/aux are per-arm (documented at each arm).
//! Aux strings s1-s3: <=256 bytes, UTF-8, NUL-free. Docs: jsonbio_diff's
//! take_json screen (<=2048B, UTF-8, NUL-free, bracket depth <=64).

use core::ffi::c_char;
use std::ffi::CString;

use datum::Datum;
use types_fmgr::PGFunction;

use crate::jsonbio_diff::{
    err_class, fc_call, grow, init_session_env, take_json, varlena_data,
};

extern "C" {
    fn pg_diff_jbops_cmp(
        a: *const c_char, b: *const c_char,
        cmp_out: *mut i32, eq_out: *mut u8,
    ) -> i32;
    fn pg_diff_jbops_contains(
        which: i32, a: *const c_char, b: *const c_char, out: *mut u8,
    ) -> i32;
    fn pg_diff_jbops_exists(
        a: *const c_char, key: *const u8, keylen: i32, out: *mut u8,
    ) -> i32;
    fn pg_diff_jbops_exists_arr(
        all: i32, a: *const c_char, arr: *const u8, out: *mut u8,
    ) -> i32;
    fn pg_diff_jbops_hash(
        ext: i32, a: *const c_char, seed: i64, out: *mut i64,
    ) -> i32;
    fn pg_diff_jbops_getfield(
        which: i32, a: *const c_char, key: *const u8, keylen: i32, idx: i32,
        out: *mut u8, outcap: i32, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_jbops_path(
        which: i32, a: *const c_char, arr: *const u8, newval: *const c_char,
        flag: i32, out: *mut u8, outcap: i32, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_jbops_delete(
        which: i32, a: *const c_char, key: *const u8, keylen: i32, idx: i32,
        arr: *const u8, out: *mut u8, outcap: i32, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_jbops_concat(
        a: *const c_char, b: *const c_char,
        out: *mut u8, outcap: i32, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_jbops_object(
        two: i32, arr1: *const u8, arr2: *const u8,
        out: *mut u8, outcap: i32, outlen: *mut i32,
    ) -> i32;
}

const MAX_AUX: usize = 256;
const CBUF: usize = 1 << 16;
const TEXTOID: u32 = 25;

fn init_ops_env() {
    init_session_env();
    use std::sync::Once;
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        // catch_unwind tolerates another lane's harness installing these
        // seams first (double-install panics; all lanes share one test
        // binary — same convention as arrayfuncs_diff::init_seams). All
        // images here are inline, for which every installed impl is the
        // identity copy.
        // Database-collation-C pin (see module header).
        let _ = std::panic::catch_unwind(|| {
            pg_locale_seams::varstr_cmp_locale::set(|_collid, a, b| {
                Ok(varlena::varstrfastcmp_c(a, b))
            })
        });
        // Real detoast for the flat text[] argument reads (inline images
        // only in this harness; the seam is the shipped detoast_attr).
        let _ = std::panic::catch_unwind(|| {
            detoast_seams::detoast_attr::set(detoast::detoast_attr)
        });
    });
    // If the varstr pin above LOST to the real pg_locale (jsonpath_diff
    // installs pg_locale::init_seams; seam installs are first-wins), the
    // real varstr_cmp_locale needs the per-thread default locale armed —
    // pinned to C it is behaviorally identical to this module's
    // varstrfastcmp_c pin (jsonpath_diff::setup convention).
    if !pg_locale::default_locale_installed() {
        pg_locale::set_default_locale_c_for_tests();
    }
}

/// 8-aligned byte buffer (PG datum images are read with aligned int32/
/// pointer loads on both sides; Vec<u8> guarantees nothing).
struct AlignedBuf {
    words: Vec<u64>,
    len: usize,
}

impl AlignedBuf {
    fn from_bytes(b: &[u8]) -> Self {
        let mut words = vec![0u64; b.len().div_ceil(8).max(1)];
        // SAFETY: words covers >= b.len() bytes.
        unsafe {
            core::ptr::copy_nonoverlapping(
                b.as_ptr(),
                words.as_mut_ptr().cast::<u8>(),
                b.len(),
            );
        }
        AlignedBuf { words, len: b.len() }
    }

    fn ptr(&self) -> *const u8 {
        self.words.as_ptr().cast()
    }

    fn datum(&self) -> Datum {
        Datum::from_usize(self.ptr() as usize)
    }

    #[allow(dead_code)]
    fn bytes(&self) -> &[u8] {
        // SAFETY: from_bytes wrote exactly len initialized bytes.
        unsafe { core::slice::from_raw_parts(self.ptr(), self.len) }
    }
}

/// text varlena image: 4B header (VARSIZE = 4 + n, shifted per varatt).
fn mk_text(data: &[u8]) -> AlignedBuf {
    let total = (4 + data.len()) as u32;
    let mut v = Vec::with_capacity(4 + data.len());
    v.extend_from_slice(&(total << 2).to_le_bytes());
    v.extend_from_slice(data);
    AlignedBuf::from_bytes(&v)
}

const MAXALIGN: usize = 8;

fn maxalign(n: usize) -> usize {
    (n + MAXALIGN - 1) & !(MAXALIGN - 1)
}

fn intalign(n: usize) -> usize {
    (n + 3) & !3
}

/// Flat PG text[] ArrayType image (see module header). `dims`: the dims
/// vector (product must equal elems.len()); empty elems => ndim 0
/// (construct_empty_array shape).
fn mk_text_array(elems: &[Option<&[u8]>], dims: &[i32]) -> AlignedBuf {
    let n = elems.len();
    let ndim = if n == 0 { 0 } else { dims.len() };
    debug_assert!(n == 0 || dims.iter().product::<i32>() as usize == n);
    let has_null = elems.iter().any(Option::is_none);
    let header = 16 + 2 * 4 * ndim;
    let data_off = if has_null {
        maxalign(header + n.div_ceil(8))
    } else {
        maxalign(header)
    };
    let mut v = vec![0u8; data_off];
    // vl_len_ patched at the end
    v[4..8].copy_from_slice(&(ndim as i32).to_le_bytes());
    let dataoffset: i32 = if has_null { data_off as i32 } else { 0 };
    v[8..12].copy_from_slice(&dataoffset.to_le_bytes());
    v[12..16].copy_from_slice(&TEXTOID.to_le_bytes());
    for (i, d) in dims.iter().enumerate().take(ndim) {
        let o = 16 + 4 * i;
        v[o..o + 4].copy_from_slice(&d.to_le_bytes());
        let o = 16 + 4 * ndim + 4 * i;
        v[o..o + 4].copy_from_slice(&1i32.to_le_bytes()); // lower bounds = 1
    }
    if has_null {
        for (i, e) in elems.iter().enumerate() {
            if e.is_some() {
                v[header + i / 8] |= 1 << (i % 8);
            }
        }
    }
    for e in elems.iter().flatten() {
        // element: 4B-header text, 'i' alignment between elements
        let pad = intalign(v.len()) - v.len();
        v.extend(std::iter::repeat_n(0, pad));
        let total = (4 + e.len()) as u32;
        v.extend_from_slice(&(total << 2).to_le_bytes());
        v.extend_from_slice(e);
    }
    let total = v.len() as u32;
    v[0..4].copy_from_slice(&(total << 2).to_le_bytes());
    AlignedBuf::from_bytes(&v)
}

struct Input<'a> {
    flags: u8,
    aux: i32,
    s: [&'a [u8]; 3],
    doc1: CString,
    doc2: Option<CString>,
    raw2: &'a [u8],
}

fn take_aux(payload: &[u8]) -> Option<&[u8]> {
    if payload.len() > MAX_AUX || payload.contains(&0) {
        return None;
    }
    std::str::from_utf8(payload).ok()?;
    Some(payload)
}

fn parse_input(data: &[u8]) -> Option<Input<'_>> {
    let flags = *data.first()?;
    let mut off = 1;
    let aux = i32::from_le_bytes(data.get(off..off + 4)?.try_into().ok()?);
    off += 4;
    let mut fields: [&[u8]; 4] = [&[]; 4];
    for f in &mut fields {
        let l = u16::from_le_bytes(data.get(off..off + 2)?.try_into().ok()?) as usize;
        off += 2;
        *f = data.get(off..off + l)?;
        off += l;
    }
    let raw2 = data.get(off..)?;
    let doc1 = take_json(fields[3])?;
    let s0 = take_aux(fields[0])?;
    let s1 = take_aux(fields[1])?;
    let s2 = take_aux(fields[2])?;
    Some(Input {
        flags,
        aux,
        s: [s0, s1, s2],
        doc1,
        doc2: take_json(raw2),
        raw2,
    })
}

/// The aux-string text[] for this iteration: count = flags b1-b2 (0-3),
/// NULL element injected at position aux%count when flags b3.
fn input_array(inp: &Input<'_>, dims2: bool) -> AlignedBuf {
    let count = ((inp.flags >> 1) & 3) as usize;
    let mut elems: Vec<Option<&[u8]>> =
        inp.s.iter().take(count).map(|s| Some(*s)).collect();
    if inp.flags & 8 != 0 && !elems.is_empty() {
        let pos = (inp.aux.unsigned_abs() as usize) % elems.len();
        elems[pos] = None;
    }
    let n = elems.len() as i32;
    let dims: Vec<i32> = if dims2 {
        // even n: the catalog {n/2, 2} pair shape; odd n: {n, 1} — the
        // "array must have two columns" arm
        if n % 2 == 0 && n > 0 { vec![n / 2, 2] } else { vec![n, 1] }
    } else {
        vec![n]
    };
    mk_text_array(&elems, &dims)
}

/// mcx + parsed doc prelude shared by every arm (parse-verdict parity is
/// jsonbio_diff arm 0's charter; ops arms only run on shared successes).
macro_rules! prelude {
    ($inp:ident, $m:ident, $jb:ident) => {
        let cx = mcx::MemoryContext::new("jsonbops_fuzz");
        let $m = cx.mcx();
        let Some($jb) = parse_rust($m, &$inp.doc1) else { return };
    };
}

fn parse_rust(m: mcx::Mcx<'_>, cs: &CString) -> Option<Datum> {
    let (r, _) = fc_call(
        adt_jsonb::builtins::fc_jsonb_in,
        m,
        [Datum::from_usize(cs.as_ptr() as usize)],
    );
    r.ok()
}

pub fn jsonbops_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init_ops_env();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let Some(inp) = parse_input(payload) else { return };
    match sel % 11 {
        0 => cmp_arm(&inp),
        1 => contains_arm(&inp),
        2 => exists_arm(&inp),
        3 => exists_arr_arm(&inp),
        4 => hash_arm(&inp),
        5 => getfield_arm(&inp),
        6 => extract_path_arm(&inp),
        7 => path_mutate_arm(&inp),
        8 => delete_arm(&inp),
        9 => concat_arm(&inp),
        _ => object_arm(&inp),
    }
}

/// One C cmp+eq oracle serves all 7 shipped wrappers.
fn cmp_arm(inp: &Input<'_>) {
    let Some(doc2) = &inp.doc2 else { return };
    prelude!(inp, m, jb1);
    let Some(jb2) = parse_rust(m, doc2) else { return };
    let (mut c_cmp, mut c_eq) = (0i32, 0u8);
    let crc = unsafe {
        pg_diff_jbops_cmp(inp.doc1.as_ptr(), doc2.as_ptr(), &mut c_cmp, &mut c_eq)
    };
    assert!(crc == 0, "cmp C rc {crc} on parsed docs {:?} {:?}", inp.doc1, doc2);
    let b = adt_jsonb::builtins::fc_jsonb_cmp;
    let (r, _) = fc_call(b, m, [jb1, jb2]);
    let r_cmp = r.expect("jsonb_cmp cannot fail post-parse").as_i32();
    // RATIFIED PLATFORM NON-SURFACE (uuid_diff precedent): string elements
    // compare via varstr_cmp's C-collation memcmp, whose MAGNITUDE is
    // implementation-defined — witnessed three values for one input pair on
    // 2026-07-31: fleet sancov glibc C=1, docker postgres:18.3 glibc=22,
    // pgrust varstrfastcmp_c=49 (jsonb_cmp('"ab"','"a1e0000bc"')). The
    // defined surface is the SIGN; the six bool wrappers below assert the
    // exact verdict plane on top of it.
    assert!(
        r_cmp.signum() == c_cmp.signum(),
        "jsonb_cmp DIVERGENCE {:?} vs {:?}: C={c_cmp} Rust={r_cmp}",
        inp.doc1, doc2
    );
    assert!(u8::from(r_cmp == 0) == c_eq, "cmp/eq C self-skew");
    type BoolMap = (PGFunction, fn(i32) -> bool);
    let maps: [BoolMap; 6] = [
        (adt_jsonb::builtins::fc_jsonb_eq, |c| c == 0),
        (adt_jsonb::builtins::fc_jsonb_ne, |c| c != 0),
        (adt_jsonb::builtins::fc_jsonb_lt, |c| c < 0),
        (adt_jsonb::builtins::fc_jsonb_gt, |c| c > 0),
        (adt_jsonb::builtins::fc_jsonb_le, |c| c <= 0),
        (adt_jsonb::builtins::fc_jsonb_ge, |c| c >= 0),
    ];
    for (i, (f, map)) in maps.into_iter().enumerate() {
        let (r, _) = fc_call(f, m, [jb1, jb2]);
        let got = r.expect("cmp-family wrapper cannot fail post-parse").as_bool();
        assert!(
            got == map(c_cmp),
            "cmp-family[{i}] DIVERGENCE {:?} vs {:?}: C cmp={c_cmp} Rust={got}",
            inp.doc1, doc2
        );
    }
}

fn contains_arm(inp: &Input<'_>) {
    let Some(doc2) = &inp.doc2 else { return };
    prelude!(inp, m, jb1);
    let Some(jb2) = parse_rust(m, doc2) else { return };
    let which = i32::from(inp.flags & 1);
    let mut c_out = 0u8;
    let crc = unsafe {
        pg_diff_jbops_contains(which, inp.doc1.as_ptr(), doc2.as_ptr(), &mut c_out)
    };
    assert!(crc == 0, "contains C rc {crc} on parsed docs");
    let f = if which == 1 {
        adt_jsonb::builtins::fc_jsonb_contained
    } else {
        adt_jsonb::builtins::fc_jsonb_contains
    };
    let (r, _) = fc_call(f, m, [jb1, jb2]);
    let got = r.expect("contains cannot fail post-parse").as_bool();
    assert!(
        got == (c_out != 0),
        "contains({which}) DIVERGENCE {:?} vs {:?}: C={c_out} Rust={got}",
        inp.doc1, doc2
    );
}

fn exists_arm(inp: &Input<'_>) {
    prelude!(inp, m, jb);
    let key = inp.s[0];
    let mut c_out = 0u8;
    let crc = unsafe {
        pg_diff_jbops_exists(inp.doc1.as_ptr(), key.as_ptr(), key.len() as i32,
                             &mut c_out)
    };
    assert!(crc == 0, "exists C rc {crc}");
    let kt = mk_text(key);
    let (r, _) = fc_call(adt_jsonb::builtins::fc_jsonb_exists, m, [jb, kt.datum()]);
    let got = r.expect("exists cannot fail").as_bool();
    assert!(
        got == (c_out != 0),
        "exists DIVERGENCE {:?} ? {:?}: C={c_out} Rust={got}",
        inp.doc1,
        String::from_utf8_lossy(key)
    );
}

fn exists_arr_arm(inp: &Input<'_>) {
    prelude!(inp, m, jb);
    let all = i32::from(inp.flags & 1);
    let arr = input_array(inp, false);
    let mut c_out = 0u8;
    let crc = unsafe {
        pg_diff_jbops_exists_arr(all, inp.doc1.as_ptr(), arr.ptr(), &mut c_out)
    };
    let f = if all == 1 {
        adt_jsonb::builtins::fc_jsonb_exists_all
    } else {
        adt_jsonb::builtins::fc_jsonb_exists_any
    };
    let (r, _) = fc_call(f, m, [jb, arr.datum()]);
    match r {
        Ok(d) => assert!(
            crc == 0 && d.as_bool() == (c_out != 0),
            "exists_arr({all}) DIVERGENCE {:?} keys={:?}: C=(rc {crc} {c_out}) Rust={}",
            inp.doc1, inp.s, d.as_bool()
        ),
        Err(e) => assert!(
            crc == err_class(&e),
            "exists_arr({all}) VERDICT DIVERGENCE {:?}: C=rc {crc} Rust=class {} ({})",
            inp.doc1, err_class(&e), e.message
        ),
    }
}

fn hash_arm(inp: &Input<'_>) {
    prelude!(inp, m, jb);
    let ext = i32::from(inp.flags & 1);
    let seed = if inp.flags & 32 != 0 {
        (i64::from(inp.aux) << 32) | i64::from(inp.aux.unsigned_abs())
    } else {
        i64::from(inp.aux)
    };
    let mut c_out = 0i64;
    let crc = unsafe { pg_diff_jbops_hash(ext, inp.doc1.as_ptr(), seed, &mut c_out) };
    assert!(crc == 0, "hash C rc {crc}");
    let got = if ext == 1 {
        let (r, _) = fc_call(
            adt_jsonb::builtins::fc_jsonb_hash_extended,
            m,
            [jb, Datum::from_i64(seed)],
        );
        r.expect("hash_extended cannot fail").as_i64()
    } else {
        let (r, _) = fc_call(adt_jsonb::builtins::fc_jsonb_hash, m, [jb]);
        i64::from(r.expect("hash cannot fail").as_i32())
    };
    assert!(
        got == c_out,
        "hash({ext},{seed}) DIVERGENCE {:?}: C={c_out:#x} Rust={got:#x}",
        inp.doc1
    );
}

/// Compare a C entry returning (-1 | class | 0 + value bytes) against a
/// Rust fc result whose Ok payload renders through `render`.
fn assert_value_plane(
    label: &str,
    input: &CString,
    crc: i32,
    c_bytes: &[u8],
    r: Result<Option<Vec<u8>>, (i32, String)>,
) {
    match r {
        Ok(None) => assert!(
            crc == -1,
            "{label} NULL DIVERGENCE {input:?}: C=rc {crc} Rust=SQL NULL"
        ),
        Ok(Some(bytes)) => assert!(
            crc == 0 && bytes == c_bytes,
            "{label} VALUE DIVERGENCE {input:?}: C=(rc {crc} len {}) Rust len {}",
            c_bytes.len(),
            bytes.len()
        ),
        Err((class, msg)) => assert!(
            crc == class,
            "{label} VERDICT DIVERGENCE {input:?}: C=rc {crc} Rust=class {class} ({msg})"
        ),
    }
}

/// Run a C -2-retry loop.
fn c_retry(mut f: impl FnMut(*mut u8, i32, *mut i32) -> i32) -> (i32, Vec<u8>) {
    let mut buf = vec![0u8; CBUF];
    let mut len = 0i32;
    loop {
        let rc = f(buf.as_mut_ptr(), buf.len() as i32, &mut len);
        if rc != -2 {
            buf.truncate(if rc == 0 { len.max(0) as usize } else { 0 });
            return (rc, buf);
        }
        grow(&mut buf, len);
    }
}

type FcOut = Result<Option<Vec<u8>>, (i32, String)>;

fn run_fc<const N: usize>(f: PGFunction, m: mcx::Mcx<'_>, args: [Datum; N]) -> FcOut {
    let (r, isnull) = fc_call(f, m, args);
    match r {
        Ok(_) if isnull => Ok(None),
        Ok(d) => Ok(Some(varlena_data(d).to_vec())),
        Err(e) => Err((err_class(&e), e.message.to_string())),
    }
}

fn getfield_arm(inp: &Input<'_>) {
    prelude!(inp, m, jb);
    let which = i32::from(inp.flags & 3);
    let key = inp.s[0];
    let (crc, c_bytes) = c_retry(|b, cap, l| unsafe {
        pg_diff_jbops_getfield(which, inp.doc1.as_ptr(), key.as_ptr(),
                               key.len() as i32, inp.aux, b, cap, l)
    });
    let kt = mk_text(key);
    let arg1 = if which < 2 { kt.datum() } else { Datum::from_i32(inp.aux) };
    let f: PGFunction = match which {
        0 => adt_jsonb::builtins::fc_jsonb_object_field,
        1 => adt_jsonb::builtins::fc_jsonb_object_field_text,
        2 => adt_jsonb::builtins::fc_jsonb_array_element,
        _ => adt_jsonb::builtins::fc_jsonb_array_element_text,
    };
    let r = run_fc(f, m, [jb, arg1]);
    assert_value_plane(
        &format!("getfield{which} key={:?} idx={}",
                 String::from_utf8_lossy(key), inp.aux),
        &inp.doc1, crc, &c_bytes, r,
    );
}

fn extract_path_arm(inp: &Input<'_>) {
    prelude!(inp, m, jb);
    let text_mode = inp.flags & 1 != 0;
    let dims2 = inp.flags & 16 != 0; // ndim>1 error arm
    let arr = input_array(inp, dims2);
    let which = i32::from(text_mode);
    let (crc, c_bytes) = c_retry(|b, cap, l| unsafe {
        pg_diff_jbops_path(which, inp.doc1.as_ptr(), arr.ptr(),
                           core::ptr::null(), 0, b, cap, l)
    });
    let f: PGFunction = if text_mode {
        adt_jsonb::builtins::fc_jsonb_extract_path_text
    } else {
        adt_jsonb::builtins::fc_jsonb_extract_path
    };
    let r = run_fc(f, m, [jb, arr.datum()]);
    assert_value_plane(
        &format!("extract_path(text={text_mode}) path={:?}", inp.s),
        &inp.doc1, crc, &c_bytes, r,
    );
}

fn path_mutate_arm(inp: &Input<'_>) {
    let Some(doc2) = &inp.doc2 else { return };
    prelude!(inp, m, jb);
    let sub = (inp.flags >> 1) % 3; // 0 set, 1 insert, 2 delete_path
    let flag = i32::from(inp.flags & 1); // create_missing / insert_after
    let dims2 = inp.flags & 16 != 0;
    let arr = input_array(inp, dims2);
    let (which, label) = match sub {
        0 => (3, "jsonb_set"),
        1 => (4, "jsonb_insert"),
        _ => (2, "jsonb_delete_path"),
    };
    if which != 2 && parse_rust(m, doc2).is_none() {
        return;
    }
    let (crc, c_bytes) = c_retry(|b, cap, l| unsafe {
        pg_diff_jbops_path(which, inp.doc1.as_ptr(), arr.ptr(),
                           doc2.as_ptr(), flag, b, cap, l)
    });
    let r = if which == 2 {
        run_fc(adt_jsonb::builtins::fc_jsonb_delete_path, m,
               [jb, arr.datum()])
    } else {
        let jb2 = parse_rust(m, doc2).expect("checked above");
        let f: PGFunction = if which == 3 {
            adt_jsonb::builtins::fc_jsonb_set
        } else {
            adt_jsonb::builtins::fc_jsonb_insert
        };
        run_fc(f, m, [jb, arr.datum(), jb2, Datum::from_bool(flag != 0)])
    };
    assert_value_plane(
        &format!("{label}(flag={flag}) path={:?} new={doc2:?}", inp.s),
        &inp.doc1, crc, &c_bytes, r,
    );
}

fn delete_arm(inp: &Input<'_>) {
    prelude!(inp, m, jb);
    let which = i32::from(inp.flags & 3) % 3;
    let key = inp.s[0];
    // bit4 drives the text[]-arm ndim>1 subscript-error plane, as in the
    // path/object arms (fc_jsonb_delete_array checks arr_ndim first).
    let arr = input_array(inp, inp.flags & 16 != 0);
    let (crc, c_bytes) = c_retry(|b, cap, l| unsafe {
        pg_diff_jbops_delete(which, inp.doc1.as_ptr(), key.as_ptr(),
                             key.len() as i32, inp.aux, arr.ptr(), b, cap, l)
    });
    let kt = mk_text(key);
    let r = match which {
        0 => run_fc(adt_jsonb::builtins::fc_jsonb_delete, m, [jb, kt.datum()]),
        1 => run_fc(adt_jsonb::builtins::fc_jsonb_delete_idx, m,
                    [jb, Datum::from_i32(inp.aux)]),
        _ => run_fc(adt_jsonb::builtins::fc_jsonb_delete_array, m,
                    [jb, arr.datum()]),
    };
    assert_value_plane(
        &format!("delete{which} key={:?} idx={} keys={:?}",
                 String::from_utf8_lossy(key), inp.aux, inp.s),
        &inp.doc1, crc, &c_bytes, r,
    );
}

fn concat_arm(inp: &Input<'_>) {
    let Some(doc2) = &inp.doc2 else { return };
    prelude!(inp, m, jb1);
    let Some(jb2) = parse_rust(m, doc2) else { return };
    let (crc, c_bytes) = c_retry(|b, cap, l| unsafe {
        pg_diff_jbops_concat(inp.doc1.as_ptr(), doc2.as_ptr(), b, cap, l)
    });
    let r = run_fc(adt_jsonb::builtins::fc_jsonb_concat, m, [jb1, jb2]);
    assert_value_plane(
        &format!("concat rhs={doc2:?}"),
        &inp.doc1, crc, &c_bytes, r,
    );
}

fn object_arm(inp: &Input<'_>) {
    // No jsonb doc input: text[]-driven constructors. doc1 is unused but
    // must have parsed (prelude keeps the corpus regime uniform).
    prelude!(inp, m, _jb);
    let two = i32::from(inp.flags & 1);
    let dims2 = inp.flags & 16 != 0;
    let arr1 = input_array(inp, dims2);
    // second array: raw2 bytes as one element, count from flags b5
    let e2: Vec<Option<&[u8]>> = if inp.flags & 32 != 0 {
        match take_aux(inp.raw2) {
            Some(s) => vec![Some(s)],
            None => vec![],
        }
    } else {
        inp.s.iter().take(((inp.flags >> 1) & 3) as usize).map(|s| Some(*s)).collect()
    };
    let arr2 = mk_text_array(&e2, &[e2.len() as i32]);
    let (crc, c_bytes) = c_retry(|b, cap, l| unsafe {
        pg_diff_jbops_object(two, arr1.ptr(), arr2.ptr(), b, cap, l)
    });
    let r = if two == 1 {
        run_fc(adt_jsonb::builtins::fc_jsonb_object_two_arg, m,
               [arr1.datum(), arr2.datum()])
    } else {
        run_fc(adt_jsonb::builtins::fc_jsonb_object, m, [arr1.datum()])
    };
    assert_value_plane(
        &format!("jsonb_object(two={two}, dims2={dims2}) keys={:?}", inp.s),
        &inp.doc1, crc, &c_bytes, r,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(sel: u8, flags: u8, aux: i32, s: [&[u8]; 3], d1: &[u8], d2: &[u8]) -> Vec<u8> {
        let mut v = vec![sel, flags];
        v.extend_from_slice(&aux.to_le_bytes());
        for f in [s[0], s[1], s[2], d1] {
            v.extend_from_slice(&(f.len() as u16).to_le_bytes());
            v.extend_from_slice(f);
        }
        v.extend_from_slice(d2);
        v
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        let none: [&[u8]; 3] = [b"", b"", b""];
        let keys: [&[u8]; 3] = [b"a", b"b", b"zz"];
        // cmp: orders, ties, cross-type ranks, string collation lanes
        jsonbops_diff(&enc(0, 0, 0, none, b"{\"a\":1}", b"{\"a\":2}"));
        jsonbops_diff(&enc(0, 0, 0, none, b"[1,2]", b"[1,2]"));
        jsonbops_diff(&enc(0, 0, 0, none, b"\"abc\"", b"\"abd\""));
        jsonbops_diff(&enc(0, 0, 0, none, b"[\"a\"]", b"{\"a\":1}"));
        jsonbops_diff(&enc(0, 0, 0, none, b"1e5", b"100000"));
        // contains / contained
        jsonbops_diff(&enc(1, 0, 0, none, b"{\"a\":1,\"b\":2}", b"{\"a\":1}"));
        jsonbops_diff(&enc(1, 1, 0, none, b"{\"a\":1}", b"{\"a\":1,\"b\":2}"));
        jsonbops_diff(&enc(1, 0, 0, none, b"[1,2,3]", b"[3,1]"));
        jsonbops_diff(&enc(1, 0, 0, none, b"[1,[2,3]]", b"[[3]]"));
        // exists
        jsonbops_diff(&enc(2, 0, 0, [b"a", b"", b""], b"{\"a\":1}", b""));
        jsonbops_diff(&enc(2, 0, 0, [b"x", b"", b""], b"[\"x\",\"y\"]", b""));
        // exists_any/all incl. empty array (all => true quirk) and a NULL key
        jsonbops_diff(&enc(3, 0 | (2 << 1), 0, keys, b"{\"a\":1}", b""));
        jsonbops_diff(&enc(3, 1 | (2 << 1), 0, keys, b"{\"a\":1,\"b\":2}", b""));
        jsonbops_diff(&enc(3, 1, 0, none, b"{\"a\":1}", b"")); // empty => true
        jsonbops_diff(&enc(3, (3 << 1) | 8, 1, keys, b"{\"b\":1}", b""));
        // hash / hash_extended
        jsonbops_diff(&enc(4, 0, 0, none, b"{\"a\":[1,true,null,\"s\"]}", b""));
        jsonbops_diff(&enc(4, 1, 42, none, b"{\"a\":[1,true,null,\"s\"]}", b""));
        jsonbops_diff(&enc(4, 1 | 32, -7, none, b"[]", b""));
        // getfield
        jsonbops_diff(&enc(5, 0, 0, [b"a", b"", b""], b"{\"a\":{\"b\":1}}", b""));
        jsonbops_diff(&enc(5, 1, 0, [b"a", b"", b""], b"{\"a\":null}", b""));
        jsonbops_diff(&enc(5, 2, -1, none, b"[1,2,3]", b""));
        jsonbops_diff(&enc(5, 3, 5, none, b"[1,2,3]", b""));
        jsonbops_diff(&enc(5, 2, i32::MIN, none, b"[1,2,3]", b""));
        // extract_path (incl. VT-whitespace subscript regression class)
        jsonbops_diff(&enc(6, 2 << 1, 0, [b"a", b"0", b""], b"{\"a\":[7]}", b""));
        jsonbops_diff(&enc(6, 1 | (2 << 1), 0, [b"a", b"0", b""], b"{\"a\":[7]}", b""));
        jsonbops_diff(&enc(6, 1 << 1, 0, [b"\x0b1", b"", b""], b"[10,20]", b""));
        jsonbops_diff(&enc(6, (1 << 1) | 8, 0, [b"a", b"", b""], b"{\"a\":1}", b""));
        jsonbops_diff(&enc(6, (2 << 1) | 16, 0, [b"a", b"b", b""], b"{\"a\":1}", b""));
        // set / insert / delete_path
        jsonbops_diff(&enc(7, 1 | (2 << 1), 0, [b"a", b"0", b""], b"{\"a\":[0]}", b"9"));
        jsonbops_diff(&enc(7, (1 << 1) | (1 << 1), 0, [b"a", b"", b""], b"{\"a\":[0]}", b"9"));
        jsonbops_diff(&enc(7, 2 | (1 << 1), 0, [b"-1", b"", b""], b"[1,2]", b"9"));
        jsonbops_diff(&enc(7, 4 | (2 << 1), 0, [b"a", b"0", b""], b"{\"a\":[5]}", b""));
        jsonbops_diff(&enc(7, (2 << 1) | 8, 0, [b"a", b"0", b""], b"{\"a\":[5]}", b"1"));
        // delete
        jsonbops_diff(&enc(8, 0, 0, [b"a", b"", b""], b"{\"a\":1,\"b\":2}", b""));
        jsonbops_diff(&enc(8, 1, -1, none, b"[1,2,3]", b""));
        jsonbops_diff(&enc(8, 1, 0, none, b"{\"a\":1}", b"")); // 22023
        jsonbops_diff(&enc(8, 2 | (2 << 1), 0, keys, b"{\"a\":1,\"zz\":2}", b""));
        // concat
        jsonbops_diff(&enc(9, 0, 0, none, b"{\"a\":1}", b"{\"b\":2}"));
        jsonbops_diff(&enc(9, 0, 0, none, b"[1]", b"\"x\""));
        jsonbops_diff(&enc(9, 0, 0, none, b"{\"a\":1}", b"[1]"));
        // jsonb_object: pairs, odd count error, 2-D, null key error, two-arg
        jsonbops_diff(&enc(10, 2 << 1, 0, [b"k1", b"v1", b""], b"0", b""));
        jsonbops_diff(&enc(10, 3 << 1, 0, keys, b"0", b"")); // odd => error
        jsonbops_diff(&enc(10, (2 << 1) | 16, 0, [b"k1", b"v1", b""], b"0", b""));
        jsonbops_diff(&enc(10, (2 << 1) | 8, 0, [b"k1", b"v1", b""], b"0", b""));
        jsonbops_diff(&enc(10, 1 | (2 << 1) | 32, 0, [b"k1", b"k2", b""], b"0", b"vv"));
        jsonbops_diff(&enc(10, 0, 0, none, b"0", b"")); // empty => {}
    }

    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/jsonbops_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/jsonbops_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                jsonbops_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }
}
