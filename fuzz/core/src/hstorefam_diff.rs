//! hstorefam_diff: differential fuzz driver for crates/contrib/hstore vs
//! verbatim vendored PostgreSQL 18.3 C (csrc/pg_hstorefam_io.c, upstream sha
//! 62d6c7d3df; lane p1-mb-contribc). Selector = data[0] % 5:
//!
//!   0 in/out family — hstore_in over NUL-free cstring bytes, hard AND soft
//!     (ErrorSaveNode vs the C escontext shim) modes per a flag byte;
//!     verdict + exact sqlstate + image bytes compared. On success the SAME
//!     image drives hstore_out (cstring), hstore_send (wire bytes),
//!     hstore_to_json / _loose (text), akeys/avals/to_array/to_matrix
//!     (array images), hstore_hash / hash_extended on both sides.
//!   1 recv — hstore_recv over raw wire bytes (malformed input first-class):
//!     verdict + exact sqlstate + image. The leading pair-count word is
//!     clamped to <= 65537 (alloc-shaping carve: for any count exceeding
//!     the message's own capacity both sides take the identical
//!     insufficient-data path; the count-limit arm itself is driven by
//!     tests::recv_pair_count_limit at the exact C boundary).
//!   2 constructors — hstore_from_text (NULL-able args),
//!     hstore_from_array (0/1/2/3-D driven), hstore_from_arrays (bounds
//!     mismatch + null-key arms) over driver-built text[] images fed
//!     byte-identically to both sides.
//!   3 pairwise ops — two canonical images (driver-built through the Rust
//!     unique+build path, same bytes to both sides): fetchval / exists /
//!     defined / delete / delete_hstore / concat / contains / contained /
//!     cmp+eq+ne+gt+ge+lt+le / hash / hash_extended.
//!   4 array-keyed ops — image (X) text[] keys (with NULL elements and
//!     duplicates): exists_any / exists_all / delete_array /
//!     slice_to_hstore / slice_to_array.
//!
//! Comparison planes: value bytes (hstore images, out cstrings, send wire,
//! json text, array images) + error verdict + EXACT sqlstate (both sides
//! carry the real MAKE_SQLSTATE word) + soft-error occurred flag + no-panic.
//!
//! DUPLICATE-KEY TIE (certified non-surface, multirange-tie-ruling
//! pattern): hstoreUniquePairs keeps one of several equal-key pairs, and
//! which one is qsort-tie-order-dependent — C runs pg_qsort (Bentley &
//! McIlroy, unstable at n>=7), the Rust port a stable sort; PostgreSQL
//! documents the surviving duplicate as unspecified. When images differ,
//! the driver re-derives the pre-unique candidate multiset (from the C
//! parse-only entry for arm 0, from its own decoded inputs for arms 1/2)
//! and accepts iff key sequences match and every divergent value is a
//! certified candidate for its key. Everything else is a real divergence.
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - arm 0: input is NUL-free (cstring contract).
//!   - arm 1: pair-count clamp (above).
//!   - arms 2-4: array images are well-formed (driver-built via
//!     construct_md_array on the Rust side, handed byte-identically to the
//!     C side — the afx driver precondition); corrupt array headers belong
//!     to arrayfuncs_diff.
//!   - encoding pinned UTF8 on both sides (client == server; recv's
//!     pq_getmsgtext verify plane compares through it).
//!
//! CARVED OUT (exception rows): hstore_from_record / populate_record
//! (composite typcache + catalog), skeys/svals/each (SRF machinery; kernels
//! covered via akeys/avals/to_array), hstore_to_jsonb(_loose) (jsonb value
//! machinery), subscript handler (parse/exec plumbing), gist/gin opclasses,
//! hstore_compat old-format upgrade (no producer on either side).

#![allow(dead_code)]

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::sync::Once;

use datum::Datum;
use hstore::repr::{build_hstore, unique_pairs, HstoreView, Pair};
use types_fmgr::{ErrorSaveNode, PGFunction};

extern "C" {
    fn pg_hst_reset();
    fn pg_hst_sqlstate() -> c_int;
    fn pg_hst_soft_sqlstate() -> c_int;
    fn pg_hst_in(
        str_: *const c_char,
        soft: c_int,
        img: *mut *const u8,
        imglen: *mut c_int,
    ) -> c_int;
    fn pg_hst_recv(wire: *const u8, wirelen: c_int, img: *mut *const u8, imglen: *mut c_int)
        -> c_int;
    fn pg_hst_out(img: *const u8, out: *mut *const c_char) -> c_int;
    fn pg_hst_send(img: *const u8, out: *mut *const u8, outlen: *mut c_int) -> c_int;
    fn pg_hst_from_text(
        key: *const u8,
        keylen: c_int,
        key_null: c_int,
        val: *const u8,
        vallen: c_int,
        val_null: c_int,
        img: *mut *const u8,
        imglen: *mut c_int,
    ) -> c_int;
    fn pg_hst_from_arrays(
        karr: *const u8,
        k_null: c_int,
        varr: *const u8,
        v_null: c_int,
        img: *mut *const u8,
        imglen: *mut c_int,
    ) -> c_int;
    fn pg_hst_from_array(arr: *const u8, img: *mut *const u8, imglen: *mut c_int) -> c_int;
    fn pg_hst_fetchval(
        img: *const u8,
        key: *const u8,
        keylen: c_int,
        out: *mut *const u8,
        outlen: *mut c_int,
    ) -> c_int;
    fn pg_hst_exists(img: *const u8, key: *const u8, keylen: c_int) -> c_int;
    fn pg_hst_defined(img: *const u8, key: *const u8, keylen: c_int) -> c_int;
    fn pg_hst_bool2(which: c_int, a: *const u8, b: *const u8) -> c_int;
    fn pg_hst_binop(
        which: c_int,
        a: *const u8,
        b: *const u8,
        blen_for_text: c_int,
        img: *mut *const u8,
        imglen: *mut c_int,
    ) -> c_int;
    fn pg_hst_unop_array(
        which: c_int,
        a: *const u8,
        img: *mut *const u8,
        imglen: *mut c_int,
    ) -> c_int;
    fn pg_hst_slice_to_array(
        a: *const u8,
        keys: *const u8,
        img: *mut *const u8,
        imglen: *mut c_int,
    ) -> c_int;
    fn pg_hst_cmp_ops(a: *const u8, b: *const u8, out: *mut i32) -> c_int;
    fn pg_hst_hash(img: *const u8, out: *mut u32) -> c_int;
    fn pg_hst_hash_extended(img: *const u8, seed: u64, out: *mut u64) -> c_int;
    fn pg_hst_to_json(img: *const u8, loose: c_int, out: *mut *const u8, outlen: *mut c_int)
        -> c_int;
    fn pg_hst_parse_pairs(str_: *const c_char) -> c_int;
    fn pg_hst_parse_pair(
        i: c_int,
        k: *mut *const c_char,
        klen: *mut c_int,
        v: *mut *const c_char,
        vlen: *mut c_int,
        isnull: *mut c_int,
    );
}

/// Byte-cursor over the fuzz payload; exhausted reads return zeros so every
/// input length is valid.
struct Rdr<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, pos: 0 }
    }
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    fn u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        for s in &mut b {
            *s = self.u8();
        }
        u64::from_le_bytes(b)
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        let start = self.pos.min(self.d.len());
        let end = (self.pos + n).min(self.d.len());
        let mut v = self.d[start..end].to_vec();
        v.resize(n, 0);
        self.pos += n;
        v
    }
    fn rest(&self) -> &'a [u8] {
        &self.d[self.pos.min(self.d.len())..]
    }
}

// ---------------- Rust-side plumbing ----------------

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = std::panic::catch_unwind(hstore::init_seams);
    });
}

fn pin_encoding() {
    // Thread-locals: pin on the executing thread, every exec (cheap).
    // Client == server == UTF8; the C oracle verifies UTF8 (header).
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("PG_UTF8 valid");
    let _ = mbutils::SetClientEncoding(wchar::PG_UTF8);
}

fn fc(name: &str) -> PGFunction {
    dfmgr::load_external_function("hstore", name, true)
        .expect("hstore library registered")
        .unwrap_or_else(|| panic!("hstore fn {name} resolves"))
}

/// One fc call on a fresh frame; returns (result, isnull).
fn run_fc<const N: usize>(
    f: PGFunction,
    mcx: mcx::Mcx<'_>,
    args: &[(Datum, bool); N],
    esc: Option<&mut ErrorSaveNode>,
) -> (types_error::PgResult<Datum>, bool) {
    let mut fci = types_fmgr::LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `mcx` outlives this call.
    unsafe { fci.set_result_mcx(mcx) };
    for (i, (d, isnull)) in args.iter().enumerate() {
        if *isnull {
            fci.set_arg_null(i);
        } else {
            fci.set_arg(i, *d);
        }
    }
    if let Some(node) = esc {
        fci.context = node.fm_node_ptr();
    }
    let r = f(None, &mut fci);
    let isnull = fci.isnull;
    (r, isnull)
}

fn image_of<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live 4B-header varlena datum built by the fc call.
    unsafe {
        let total = types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, total)
    }
}

fn varlena_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena datum built by the fc call.
    unsafe {
        let total = types_tuple::varatt::varsize_any(p);
        let hdr = if types_tuple::varatt::varatt_is_1b(p) { 1 } else { 4 };
        core::slice::from_raw_parts(p.add(hdr), total - hdr)
    }
}

fn cstring_of<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: a live NUL-terminated cstring datum built by the fc call.
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const c_char) }.to_bytes()
}

fn sqlstate_of(e: &types_error::PgError) -> i32 {
    e.sqlstate().0
}

// ---------------- tie-relaxation (see header) ----------------

/// (key, value-or-null) candidate list, pre-unique, in input order.
type Candidates = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// images equal, or equal modulo the duplicate-key survivor tie.
fn assert_images_tie_equal(rimg: &[u8], cimg: &[u8], cands: &Candidates, what: &str) {
    if rimg == cimg {
        return;
    }
    let r = HstoreView::from_vardata(&rimg[4..]);
    let c = HstoreView::from_vardata(&cimg[4..]);
    assert_eq!(r.count(), c.count(), "{what}: pair count (tie check)");
    for i in 0..r.count() {
        assert_eq!(r.key(i), c.key(i), "{what}: key {i} (tie check)");
        let rv = (!r.val_isnull(i)).then(|| r.val(i).to_vec());
        let cv = (!c.val_isnull(i)).then(|| c.val(i).to_vec());
        if rv == cv {
            continue;
        }
        // divergent value: both must be certified candidates for a
        // DUPLICATED key
        let key = r.key(i);
        let vals: Vec<&Option<Vec<u8>>> =
            cands.iter().filter(|(k, _)| k == key).map(|(_, v)| v).collect();
        assert!(
            vals.len() >= 2 && vals.contains(&&rv) && vals.contains(&&cv),
            "{what}: REAL image divergence at key {:?}: rust {:?} c {:?} (candidates {:?})",
            String::from_utf8_lossy(key),
            rv.as_deref().map(String::from_utf8_lossy),
            cv.as_deref().map(String::from_utf8_lossy),
            vals.len(),
        );
        bump_ties();
    }
}

use std::sync::atomic::{AtomicU64, Ordering};
static TIE_ACCEPTS: AtomicU64 = AtomicU64::new(0);
fn bump_ties() {
    TIE_ACCEPTS.fetch_add(1, Ordering::Relaxed);
}

// ---------------- C-result helpers ----------------

struct COut {
    ret: i32,
    bytes: Vec<u8>,
    sqlstate: i32,
}

fn c_img(ret: i32, img: *const u8, len: i32) -> COut {
    let bytes = if ret == 0 {
        // SAFETY: arena pointer valid until the next pg_hst_reset.
        unsafe { core::slice::from_raw_parts(img, len as usize) }.to_vec()
    } else {
        Vec::new()
    };
    COut { ret, bytes, sqlstate: unsafe { pg_hst_sqlstate() } }
}

// ---------------- shared: both-sides hstore_in ----------------

/// Returns the agreed image (None on agreed error) after asserting parity.
fn diff_in(text: &[u8], soft: bool, mcx: mcx::Mcx<'_>) -> Option<Vec<u8>> {
    debug_assert!(!text.contains(&0));
    let cs = CString::new(text).expect("NUL-free by construction");

    let (mut cip, mut cil): (*const u8, c_int) = (std::ptr::null(), 0);
    let cret = unsafe { pg_hst_in(cs.as_ptr(), soft as c_int, &mut cip, &mut cil) };
    let c = c_img(if cret == 0 { 0 } else { -1 }, cip, cil);
    let c_soft_state = unsafe { pg_hst_soft_sqlstate() };

    let mut esc = ErrorSaveNode::new(true);
    let (rres, r_isnull) = run_fc::<1>(
        fc("hstore_in"),
        mcx,
        &[(Datum::from_usize(cs.as_ptr() as usize), false)],
        soft.then_some(&mut esc),
    );
    let dbg = || format!("hstore_in soft={soft} input={:?}", String::from_utf8_lossy(text));

    match (&rres, cret) {
        (Err(e), -1) => {
            assert_eq!(sqlstate_of(e), c.sqlstate, "in: hard-error sqlstate ({})", dbg());
            None
        }
        (Ok(_), 1) => {
            // soft error on the C side; Rust must have captured one too
            assert!(soft, "C soft error outside soft mode ({})", dbg());
            assert!(
                esc.ctx.error_occurred(),
                "in: C soft error, Rust ok ({})",
                dbg()
            );
            let re = esc.ctx.error().expect("occurred implies error");
            assert_eq!(sqlstate_of(re), c_soft_state, "in: soft sqlstate ({})", dbg());
            assert!(r_isnull, "in: soft-error result must be NULL ({})", dbg());
            None
        }
        (Ok(d), 0) => {
            assert!(
                !soft || !esc.ctx.error_occurred(),
                "in: Rust soft error, C ok ({})",
                dbg()
            );
            assert!(!r_isnull, "in: Rust NULL on C success ({})", dbg());
            let rimg = image_of(*d).to_vec();
            let cands = c_parse_candidates(&cs);
            assert_images_tie_equal(&rimg, &c.bytes, &cands, &dbg());
            // hand back the RUST image (both certified) for downstream arms
            Some(rimg)
        }
        (Err(e), 1) => panic!(
            "in: C soft error but Rust HARD error {} ({})",
            e.message,
            dbg()
        ),
        (Err(e), 0) => panic!("in: Rust error {} vs C ok ({})", e.message, dbg()),
        (Ok(_), -1) => {
            assert!(
                !(soft && esc.ctx.error_occurred()),
                "in: Rust soft error vs C hard error ({})",
                dbg()
            );
            panic!("in: Rust ok vs C error sqlstate {} ({})", c.sqlstate, dbg())
        }
        (_, r) => panic!("in: unexpected C ret {r} ({})", dbg()),
    }
}

/// Pre-unique candidates from the C parse-only entry (arm-0 tie oracle).
fn c_parse_candidates(cs: &CString) -> Candidates {
    let n = unsafe { pg_hst_parse_pairs(cs.as_ptr()) };
    if n < 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n {
        let (mut k, mut v): (*const c_char, *const c_char) =
            (std::ptr::null(), std::ptr::null());
        let (mut klen, mut vlen, mut isnull): (c_int, c_int, c_int) = (0, 0, 0);
        unsafe {
            pg_hst_parse_pair(i, &mut k, &mut klen, &mut v, &mut vlen, &mut isnull);
        }
        // SAFETY: arena pointers valid until pg_hst_reset.
        let key = unsafe { core::slice::from_raw_parts(k.cast::<u8>(), klen as usize) }.to_vec();
        let val = if isnull != 0 {
            None
        } else {
            Some(
                unsafe { core::slice::from_raw_parts(v.cast::<u8>(), vlen as usize) }.to_vec(),
            )
        };
        out.push((key, val));
    }
    out
}

// ---------------- arm 0: in/out family ----------------

fn inout_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    let seed = r.u64();
    let soft = flags & 1 != 0;
    let text: Vec<u8> = r.rest().iter().copied().filter(|&b| b != 0).take(4096).collect();

    let ctx = mcx::MemoryContext::new("hstorefam-inout");
    let mcx = ctx.mcx();

    // dfmgr lookup-miss arm of the crate's builtin-library table.
    assert!(matches!(
        dfmgr::load_external_function("hstore", "no_such_hstore_function", false),
        Ok(None)
    ));
    // hstore_version_diag: new-format-only producer, always 2 (the C
    // counterpart reads hstore_compat.c's valid_old/valid_new probes, whose
    // old-format arm has no producer on either side — carved).
    let (vd, _) = run_fc::<1>(fc("hstore_version_diag"), mcx, &[(Datum::null(), true)], None);
    assert_eq!(vd.expect("version_diag infallible").as_u64() as u32, 2);

    let Some(img) = diff_in(&text, soft, mcx) else {
        return;
    };
    downstream_unops(&img, seed, mcx);
}

/// out/send/json/array/hash faces over one certified image.
fn downstream_unops(img: &[u8], seed: u64, mcx: mcx::Mcx<'_>) {
    let d = Datum::from_usize(img.as_ptr() as usize);

    // repr accessor faces (postcondition arm: same values by a second
    // route as key()/val()/val_isnull(), which the C-compared paths use).
    {
        let hs = HstoreView::from_vardata(&img[4..]);
        assert_eq!(hs.pool_len(), hs.pool_bytes().len().min(hs.pool_len()));
        for i in 0..hs.count() {
            assert_eq!(hs.keylen(i), hs.key(i).len());
            if !hs.val_isnull(i) {
                assert_eq!(hs.vallen(i), hs.val(i).len());
            }
            assert_eq!(hs.pair(i).isnull(), hs.val_isnull(i));
        }
        if hs.count() == 0 {
            assert_eq!(hs.pool_len(), 0);
        }
    }

    // hstore_out
    let (rres, _) = run_fc::<1>(fc("hstore_out"), mcx, &[(d, false)], None);
    let rd = rres.expect("hstore_out is infallible on a valid image");
    let mut cp: *const c_char = std::ptr::null();
    assert_eq!(unsafe { pg_hst_out(img.as_ptr(), &mut cp) }, 0, "C hstore_out errored");
    let cout = unsafe { std::ffi::CStr::from_ptr(cp) }.to_bytes();
    assert_eq!(cstring_of(rd), cout, "hstore_out bytes");

    // hstore_send
    let (rres, _) = run_fc::<1>(fc("hstore_send"), mcx, &[(d, false)], None);
    let rd = rres.expect("hstore_send is infallible on a valid image");
    let (mut sp, mut sl): (*const u8, c_int) = (std::ptr::null(), 0);
    assert_eq!(unsafe { pg_hst_send(img.as_ptr(), &mut sp, &mut sl) }, 0);
    let cwire = unsafe { core::slice::from_raw_parts(sp, sl as usize) };
    assert_eq!(varlena_payload(rd), cwire, "hstore_send wire bytes");

    // to_json / to_json_loose
    for loose in [0, 1] {
        let name = if loose == 1 { "hstore_to_json_loose" } else { "hstore_to_json" };
        let (rres, _) = run_fc::<1>(fc(name), mcx, &[(d, false)], None);
        let rd = rres.expect("to_json on a valid image");
        let (mut jp, mut jl): (*const u8, c_int) = (std::ptr::null(), 0);
        assert_eq!(unsafe { pg_hst_to_json(img.as_ptr(), loose, &mut jp, &mut jl) }, 0);
        let cjson = unsafe { core::slice::from_raw_parts(jp, jl as usize) };
        assert_eq!(varlena_payload(rd), cjson, "{name} bytes");
    }

    // akeys/avals/to_array/to_matrix (array images)
    for (which, name) in [
        (0, "hstore_akeys"),
        (1, "hstore_avals"),
        (2, "hstore_to_array"),
        (3, "hstore_to_matrix"),
    ] {
        let (rres, _) = run_fc::<1>(fc(name), mcx, &[(d, false)], None);
        let rd = rres.expect("array face on a valid image");
        let (mut ap, mut al): (*const u8, c_int) = (std::ptr::null(), 0);
        assert_eq!(unsafe { pg_hst_unop_array(which, img.as_ptr(), &mut ap, &mut al) }, 0);
        let carr = unsafe { core::slice::from_raw_parts(ap, al as usize) };
        assert_eq!(image_of(rd), carr, "{name} array image");
    }

    // hash / hash_extended
    let (rres, _) = run_fc::<1>(fc("hstore_hash"), mcx, &[(d, false)], None);
    let mut ch: u32 = 0;
    assert_eq!(unsafe { pg_hst_hash(img.as_ptr(), &mut ch) }, 0);
    assert_eq!(
        rres.expect("hash infallible").as_u64() as u32,
        ch,
        "hstore_hash"
    );
    let (rres, _) = run_fc::<2>(
        fc("hstore_hash_extended"),
        mcx,
        &[(d, false), (Datum::from_u64(seed), false)],
        None,
    );
    let mut che: u64 = 0;
    assert_eq!(unsafe { pg_hst_hash_extended(img.as_ptr(), seed, &mut che) }, 0);
    assert_eq!(rres.expect("hash_extended infallible").as_u64(), che, "hstore_hash_extended");
}

// ---------------- arm 1: recv ----------------

fn recv_case(r: &mut Rdr<'_>) {
    let mut wire: Vec<u8> = r.rest().to_vec();
    // pair-count clamp (header DOMAIN CARVES)
    if wire.len() >= 4 {
        let pcount = i32::from_be_bytes(wire[..4].try_into().unwrap());
        if pcount > 65537 {
            wire[..4].copy_from_slice(&65537i32.to_be_bytes());
        }
    }

    let ctx = mcx::MemoryContext::new("hstorefam-recv");
    let mcx = ctx.mcx();

    let (mut cip, mut cil): (*const u8, c_int) = (std::ptr::null(), 0);
    let cret = unsafe { pg_hst_recv(wire.as_ptr(), wire.len() as c_int, &mut cip, &mut cil) };
    let c = c_img(cret, cip, cil);

    let mut si = stringinfo::StringInfo::with_capacity_in(mcx, wire.len() + 1)
        .expect("stringinfo alloc");
    si.append_bytes(&wire).expect("stringinfo append");
    let (rres, _) = run_fc::<1>(
        fc("hstore_recv"),
        mcx,
        &[(Datum::from_usize(core::ptr::addr_of_mut!(si) as usize), false)],
        None,
    );

    match (&rres, c.ret) {
        (Err(e), -1) => {
            assert_eq!(
                sqlstate_of(e),
                c.sqlstate,
                "recv sqlstate (msg {:?}, wire {:x?})",
                e.message,
                &wire[..wire.len().min(64)]
            );
        }
        (Ok(d), 0) => {
            let rimg = image_of(*d).to_vec();
            let cands = wire_candidates(&wire);
            assert_images_tie_equal(&rimg, &c.bytes, &cands, "hstore_recv");
        }
        (Err(e), 0) => panic!(
            "recv: Rust error {} vs C ok (wire {:x?})",
            e.message,
            &wire[..wire.len().min(64)]
        ),
        (Ok(_), _) => panic!(
            "recv: Rust ok vs C error sqlstate {} (wire {:x?})",
            c.sqlstate,
            &wire[..wire.len().min(64)]
        ),
        (_, r) => panic!("recv: unexpected C ret {r}"),
    }
}

/// Pre-unique candidates decoded straight from a wire message that BOTH
/// sides accepted (deterministic transcription of the recv loop).
fn wire_candidates(wire: &[u8]) -> Candidates {
    let mut out = Vec::new();
    let rd = |pos: &mut usize| -> Option<i32> {
        if *pos + 4 > wire.len() {
            return None;
        }
        let v = i32::from_be_bytes(wire[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        Some(v)
    };
    let mut pos = 0usize;
    let Some(pcount) = rd(&mut pos) else { return out };
    for _ in 0..pcount.max(0) {
        let Some(klen) = rd(&mut pos) else { return out };
        if klen < 0 || pos + klen as usize > wire.len() {
            return out;
        }
        let key = wire[pos..pos + klen as usize].to_vec();
        pos += klen as usize;
        let Some(vlen) = rd(&mut pos) else { return out };
        let val = if vlen < 0 {
            None
        } else {
            if pos + vlen as usize > wire.len() {
                return out;
            }
            let v = wire[pos..pos + vlen as usize].to_vec();
            pos += vlen as usize;
            Some(v)
        };
        out.push((key, val));
    }
    out
}

// ---------------- pair decode + canonical image (arms 2-4) ----------------

fn decode_pairs(r: &mut Rdr<'_>, maxn: usize) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let n = (r.u8() as usize) % (maxn + 1);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let klen = (r.u8() as usize) % 11;
        let key = r.bytes(klen);
        let vnull = r.u8() & 3 == 0;
        let vlen = (r.u8() as usize) % 11;
        let val = if vnull { None } else { Some(r.bytes(vlen)) };
        out.push((key, val));
    }
    out
}

fn canonical_image(pairs: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u8> {
    let ps: Vec<Pair> = pairs
        .iter()
        .map(|(k, v)| Pair { key: k.clone(), val: v.clone(), needfree: false })
        .collect();
    build_hstore(&unique_pairs(ps))
}

/// driver-built text[] image (fed byte-identically to both sides)
fn text_array_image<'m>(
    mcx: mcx::Mcx<'m>,
    elems: &[Option<Vec<u8>>],
    ndim: i32,
    dims: &[i32],
    lbs: &[i32],
) -> mcx::PgVec<'m, u8> {
    if ndim == 0 {
        return arrayfuncs::construct::construct_empty_array(mcx, types_core::TEXTOID)
            .expect("empty array");
    }
    let mut datums: Vec<Datum> = Vec::with_capacity(elems.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(elems.len());
    for e in elems {
        match e {
            Some(b) => {
                datums.push(types_fmgr::varlena_result(
                    varlena::cstring_to_text(mcx, b).expect("text alloc"),
                ));
                nulls.push(false);
            }
            None => {
                datums.push(Datum::null());
                nulls.push(true);
            }
        }
    }
    arrayfuncs::construct::construct_md_array(
        mcx,
        &datums,
        Some(&nulls),
        ndim,
        dims,
        lbs,
        types_core::TEXTOID,
        -1,
        false,
        b'i',
    )
    .expect("array build")
}

// ---------------- arm 2: constructors ----------------

fn constructors_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    let ctx = mcx::MemoryContext::new("hstorefam-ctor");
    let mcx = ctx.mcx();

    match flags % 3 {
        0 => {
            // hstore_from_text (NOT STRICT: NULL-able both args)
            let key_null = flags & 4 != 0;
            let val_null = flags & 8 != 0;
            let klen = (r.u8() as usize) % 17;
            let key = r.bytes(klen);
            let vlen = (r.u8() as usize) % 17;
            let val = r.bytes(vlen);

            let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
            let cret = unsafe {
                pg_hst_from_text(
                    key.as_ptr(),
                    key.len() as c_int,
                    key_null as c_int,
                    val.as_ptr(),
                    val.len() as c_int,
                    val_null as c_int,
                    &mut ip,
                    &mut il,
                )
            };
            let c = c_img(cret, ip, il);

            let kt = text_datum(mcx, &key);
            let vt = text_datum(mcx, &val);
            let (rres, r_isnull) = run_fc::<2>(
                fc("hstore_from_text"),
                mcx,
                &[(kt, key_null), (vt, val_null)],
                None,
            );
            match (&rres, c.ret) {
                (Ok(_), 1) => assert!(r_isnull, "from_text NULL verdict"),
                (Ok(d), 0) => {
                    assert!(!r_isnull, "from_text nullness");
                    assert_eq!(image_of(*d), c.bytes, "from_text image");
                }
                (Err(e), -1) => assert_eq!(sqlstate_of(e), c.sqlstate, "from_text sqlstate"),
                (r, cr) => panic!("from_text verdict: rust {:?} C {cr}", r.is_ok()),
            }
        }
        1 => {
            // hstore_from_array: 0/1/2/3-D
            let pairs = decode_pairs(r, 8);
            let mut elems: Vec<Option<Vec<u8>>> = Vec::new();
            for (k, v) in &pairs {
                elems.push(Some(k.clone()));
                elems.push(v.clone());
            }
            if flags & 4 != 0 && !elems.is_empty() {
                elems[0] = None; // null-key arm
            }
            if flags & 8 != 0 {
                elems.pop(); // odd-length arm
            }
            let n = elems.len() as i32;
            let (ndim, dims): (i32, Vec<i32>) = match (flags >> 4) % 4 {
                0 if n == 0 => (0, vec![]),
                0 | 1 => (1, vec![n]),
                2 if n % 3 == 0 && n > 0 => (2, vec![n / 3, 3]),  // "two columns" arm
                2 if n % 2 == 0 && n > 0 => (2, vec![n / 2, 2]),
                2 => (1, vec![n]),
                _ if n % 2 == 0 && n > 0 => (3, vec![n / 2, 2, 1]),
                _ => (1, vec![n]),
            };
            let lbs = vec![1i32; ndim as usize];
            let arr = text_array_image(mcx, &elems, ndim, &dims, &lbs);

            let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
            let cret = unsafe { pg_hst_from_array(arr.as_ptr(), &mut ip, &mut il) };
            let c = c_img(cret, ip, il);

            let (rres, _) = run_fc::<1>(
                fc("hstore_from_array"),
                mcx,
                &[(Datum::from_usize(arr.as_ptr() as usize), false)],
                None,
            );
            match (&rres, c.ret) {
                (Ok(d), 0) => {
                    let cands: Candidates = pair_candidates_from_elems(&elems);
                    assert_images_tie_equal(image_of(*d), &c.bytes, &cands, "from_array");
                }
                (Err(e), -1) => assert_eq!(
                    sqlstate_of(e),
                    c.sqlstate,
                    "from_array sqlstate (msg {:?})",
                    e.message
                ),
                (r, cr) => panic!(
                    "from_array verdict: rust {:?} C ret {cr} sqlstate {}",
                    r.as_ref().map(|_| ()).map_err(|e| e.message.clone()),
                    c.sqlstate
                ),
            }
        }
        _ => {
            // hstore_from_arrays (NOT STRICT; bounds arms)
            let pairs = decode_pairs(r, 8);
            let mut keys: Vec<Option<Vec<u8>>> =
                pairs.iter().map(|(k, _)| Some(k.clone())).collect();
            let mut vals: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, v)| v.clone()).collect();
            if flags & 4 != 0 && !keys.is_empty() {
                keys[0] = None; // null-key arm
            }
            let k_null = flags & 8 != 0;
            let v_null = flags & 16 != 0;
            if flags & 32 != 0 {
                vals.push(Some(b"extra".to_vec())); // bounds-mismatch arm
            }
            let klb = if flags & 64 != 0 { 2 } else { 1 }; // lbound-mismatch arm
            let nk = keys.len() as i32;
            let nv = vals.len() as i32;
            // ARR_NDIM > 1 arms ("wrong number of array subscripts"), driven
            // on the key array and on the value array independently.
            let k2d = flags & 128 != 0 && nk > 0 && nk % 2 == 0;
            let v2d = flags & 2 != 0 && nv > 0 && nv % 2 == 0;
            let karr = if k2d {
                text_array_image(mcx, &keys, 2, &[nk / 2, 2], &[1, 1])
            } else {
                text_array_image(mcx, &keys, if nk == 0 { 0 } else { 1 }, &[nk], &[klb])
            };
            let varr = if v2d {
                text_array_image(mcx, &vals, 2, &[nv / 2, 2], &[1, 1])
            } else {
                text_array_image(mcx, &vals, if nv == 0 { 0 } else { 1 }, &[nv], &[1])
            };

            let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
            let cret = unsafe {
                pg_hst_from_arrays(
                    karr.as_ptr(),
                    k_null as c_int,
                    varr.as_ptr(),
                    v_null as c_int,
                    &mut ip,
                    &mut il,
                )
            };
            let c = c_img(cret, ip, il);

            let (rres, r_isnull) = run_fc::<2>(
                fc("hstore_from_arrays"),
                mcx,
                &[
                    (Datum::from_usize(karr.as_ptr() as usize), k_null),
                    (Datum::from_usize(varr.as_ptr() as usize), v_null),
                ],
                None,
            );
            match (&rres, c.ret) {
                (Ok(_), 1) => assert!(r_isnull, "from_arrays NULL verdict"),
                (Ok(d), 0) => {
                    let mut cands: Candidates = Vec::new();
                    for (i, k) in keys.iter().enumerate() {
                        if let Some(k) = k {
                            let v = if v_null { None } else { vals.get(i).cloned().flatten() };
                            cands.push((k.clone(), v));
                        }
                    }
                    assert_images_tie_equal(image_of(*d), &c.bytes, &cands, "from_arrays");
                }
                (Err(e), -1) => assert_eq!(
                    sqlstate_of(e),
                    c.sqlstate,
                    "from_arrays sqlstate (msg {:?})",
                    e.message
                ),
                (r, cr) => panic!(
                    "from_arrays verdict: rust {:?} C ret {cr} sqlstate {}",
                    r.as_ref().map(|_| ()).map_err(|e| e.message.clone()),
                    c.sqlstate
                ),
            }
        }
    }
}

fn pair_candidates_from_elems(elems: &[Option<Vec<u8>>]) -> Candidates {
    let mut out = Vec::new();
    for ch in elems.chunks_exact(2) {
        if let Some(k) = &ch[0] {
            out.push((k.clone(), ch[1].clone()));
        }
    }
    out
}

/// 4B-header text datum for a driver arg (owned by mcx).
fn text_datum(mcx: mcx::Mcx<'_>, payload: &[u8]) -> Datum {
    types_fmgr::varlena_result(varlena::cstring_to_text(mcx, payload).expect("text alloc"))
}

// ---------------- arm 3: pairwise ops ----------------

fn pairwise_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    let seed = r.u64();
    let a_pairs = decode_pairs(r, 8);
    let b_pairs = decode_pairs(r, 8);
    let a = canonical_image(&a_pairs);
    let b = canonical_image(&b_pairs);
    let klen = (r.u8() as usize) % 11;
    let probe = r.bytes(klen);

    let ctx = mcx::MemoryContext::new("hstorefam-ops");
    let mcx = ctx.mcx();
    let da = Datum::from_usize(a.as_ptr() as usize);
    let db = Datum::from_usize(b.as_ptr() as usize);

    // fetchval / exists / defined with a fuzz probe key AND a real key
    let mut probes: Vec<Vec<u8>> = vec![probe];
    if let Some((k, _)) = a_pairs.first() {
        probes.push(k.clone());
    }
    for key in &probes {
        let dk = text_datum(mcx, key);

        let (rres, r_isnull) = run_fc::<2>(fc("hstore_fetchval"), mcx, &[(da, false), (dk, false)], None);
        let rd = rres.expect("fetchval infallible");
        let (mut fp, mut fl): (*const u8, c_int) = (std::ptr::null(), 0);
        let cret = unsafe {
            pg_hst_fetchval(a.as_ptr(), key.as_ptr(), key.len() as c_int, &mut fp, &mut fl)
        };
        if r_isnull {
            assert_eq!(cret, 1, "fetchval NULL verdict (key {key:?})");
        } else {
            assert_eq!(cret, 0, "fetchval verdict (key {key:?})");
            let cv = unsafe { core::slice::from_raw_parts(fp, fl as usize) };
            assert_eq!(varlena_payload(rd), cv, "fetchval bytes");
        }

        for name in ["hstore_exists", "hstore_defined"] {
            let (rres, _) = run_fc::<2>(fc(name), mcx, &[(da, false), (dk, false)], None);
            let rb = rres.expect("bool op infallible").as_u64() != 0;
            let cb = unsafe {
                if name == "hstore_exists" {
                    pg_hst_exists(a.as_ptr(), key.as_ptr(), key.len() as c_int)
                } else {
                    pg_hst_defined(a.as_ptr(), key.as_ptr(), key.len() as c_int)
                }
            };
            assert!(cb >= 0, "{name} C error");
            assert_eq!(rb, cb == 1, "{name} (key {key:?})");
        }

        // delete(text)
        let (rres, _) = run_fc::<2>(fc("hstore_delete"), mcx, &[(da, false), (dk, false)], None);
        let rd = rres.expect("delete infallible");
        let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
        assert_eq!(
            unsafe { pg_hst_binop(0, a.as_ptr(), key.as_ptr(), key.len() as c_int, &mut ip, &mut il) },
            0
        );
        let cimg = unsafe { core::slice::from_raw_parts(ip, il as usize) };
        assert_eq!(image_of(rd), cimg, "hstore_delete image");
    }

    // delete_hstore / concat
    for (which, name) in [(2, "hstore_delete_hstore"), (3, "hstore_concat")] {
        let (rres, _) = run_fc::<2>(fc(name), mcx, &[(da, false), (db, false)], None);
        let rd = rres.expect("binop infallible");
        let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
        assert_eq!(unsafe { pg_hst_binop(which, a.as_ptr(), b.as_ptr(), 0, &mut ip, &mut il) }, 0);
        let cimg = unsafe { core::slice::from_raw_parts(ip, il as usize) };
        assert_eq!(image_of(rd), cimg, "{name} image");
    }

    // contains / contained
    for (which, name) in [(2, "hstore_contains"), (3, "hstore_contained")] {
        let (rres, _) = run_fc::<2>(fc(name), mcx, &[(da, false), (db, false)], None);
        let rb = rres.expect("bool infallible").as_u64() != 0;
        let cb = unsafe { pg_hst_bool2(which, a.as_ptr(), b.as_ptr()) };
        assert!(cb >= 0, "{name} C error");
        assert_eq!(rb, cb == 1, "{name}");
    }

    // cmp family
    let mut cops = [0i32; 7];
    assert_eq!(unsafe { pg_hst_cmp_ops(a.as_ptr(), b.as_ptr(), cops.as_mut_ptr()) }, 0);
    for (i, name) in [
        "hstore_cmp",
        "hstore_eq",
        "hstore_ne",
        "hstore_gt",
        "hstore_ge",
        "hstore_lt",
        "hstore_le",
    ]
    .iter()
    .enumerate()
    {
        let (rres, _) = run_fc::<2>(fc(name), mcx, &[(da, false), (db, false)], None);
        let rd = rres.expect("cmp infallible");
        let rv = if i == 0 { rd.as_u64() as u32 as i32 } else { (rd.as_u64() != 0) as i32 };
        assert_eq!(rv, cops[i], "{name}");
    }

    if flags & 1 != 0 {
        downstream_unops(&a, seed, mcx);
    }
}

// ---------------- arm 4: array-keyed ops ----------------

fn arraykey_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    let hs_pairs = decode_pairs(r, 8);
    let a = canonical_image(&hs_pairs);

    // key array: mix of real keys, fuzz keys, NULLs, duplicates
    let nkeys = (r.u8() as usize) % 7;
    let mut keys: Vec<Option<Vec<u8>>> = Vec::with_capacity(nkeys);
    for _ in 0..nkeys {
        let sel = r.u8();
        keys.push(match sel % 4 {
            0 => None,
            1 => hs_pairs.get((sel as usize / 4) % hs_pairs.len().max(1)).map(|(k, _)| k.clone()),
            _ => {
                let kl = (r.u8() as usize) % 9;
                Some(r.bytes(kl))
            }
        });
    }
    let ctx = mcx::MemoryContext::new("hstorefam-akeys");
    let mcx = ctx.mcx();
    let n = keys.len() as i32;
    let ndim = if n == 0 {
        0
    } else if flags & 1 != 0 && n % 2 == 0 {
        2
    } else {
        1
    };
    let dims: Vec<i32> = if ndim == 2 { vec![n / 2, 2] } else { vec![n] };
    let lbs = vec![1i32; ndim.max(1) as usize];
    let karr = text_array_image(mcx, &keys, ndim, &dims[..ndim.max(1) as usize], &lbs);

    let da = Datum::from_usize(a.as_ptr() as usize);
    let dk = Datum::from_usize(karr.as_ptr() as usize);

    for (which, name) in [(0, "hstore_exists_any"), (1, "hstore_exists_all")] {
        let (rres, _) = run_fc::<2>(fc(name), mcx, &[(da, false), (dk, false)], None);
        let cb = unsafe { pg_hst_bool2(which, a.as_ptr(), karr.as_ptr()) };
        match (&rres, cb) {
            (Ok(d), 0 | 1) => assert_eq!(d.as_u64() != 0, cb == 1, "{name}"),
            (Err(e), -1) => {
                assert_eq!(sqlstate_of(e), unsafe { pg_hst_sqlstate() }, "{name} sqlstate")
            }
            (r, c) => panic!("{name} verdict: rust {:?} C {c}", r.is_ok()),
        }
    }

    for (which, name) in [(1, "hstore_delete_array"), (4, "hstore_slice_to_hstore")] {
        let (rres, _) = run_fc::<2>(fc(name), mcx, &[(da, false), (dk, false)], None);
        let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
        let cret =
            unsafe { pg_hst_binop(which, a.as_ptr(), karr.as_ptr(), 0, &mut ip, &mut il) };
        match (&rres, cret) {
            (Ok(d), 0) => {
                let cimg = unsafe { core::slice::from_raw_parts(ip, il as usize) };
                assert_eq!(image_of(*d), cimg, "{name} image");
            }
            (Err(e), -1) => {
                assert_eq!(sqlstate_of(e), unsafe { pg_hst_sqlstate() }, "{name} sqlstate")
            }
            (r, c) => panic!("{name} verdict: rust {:?} C {c}", r.is_ok()),
        }
    }

    // slice_to_array
    let (rres, _) = run_fc::<2>(fc("hstore_slice_to_array"), mcx, &[(da, false), (dk, false)], None);
    let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
    let cret = unsafe { pg_hst_slice_to_array(a.as_ptr(), karr.as_ptr(), &mut ip, &mut il) };
    match (&rres, cret) {
        (Ok(d), 0) => {
            let cimg = unsafe { core::slice::from_raw_parts(ip, il as usize) };
            assert_eq!(image_of(*d), cimg, "slice_to_array image");
        }
        (Err(e), -1) => {
            assert_eq!(sqlstate_of(e), unsafe { pg_hst_sqlstate() }, "slice_to_array sqlstate")
        }
        (r, c) => panic!("slice_to_array verdict: rust {:?} C {c}", r.is_ok()),
    }
}

// ---------------- entry ----------------

pub fn hstorefam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.is_empty() {
        return;
    }
    init_env();
    pin_encoding();
    unsafe { pg_hst_reset() };
    let mut r = Rdr::new(data);
    let sel = r.u8() % 5;
    match sel {
        0 => inout_case(&mut r),
        1 => recv_case(&mut r),
        2 => constructors_case(&mut r),
        3 => pairwise_case(&mut r),
        _ => arraykey_case(&mut r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(data: &[u8]) {
        hstorefam_diff(data);
    }

    /// hstore regress literals (contrib/hstore/sql) through arm 0, hard+soft.
    #[test]
    fn in_regress_literals() {
        let lits: &[&str] = &[
            "",
            "a=>b",
            " a=>b",
            "a =>b",
            "a=>b ",
            "a=> b",
            "\"a\"=>\"b\"",
            " \"a\"=>\"b\"",
            "aa=>ba,cc=>dd",
            "aa=>ba, cc=>dd",
            "aa=>null",
            "aa=>NuLl",
            "aa=>\"NuLl\"",
            "aa=>\"\"",
            "\"\"=>aa",
            "a=>b,",
            "a,b",
            "a=b",
            "a=>b,c",
            "=>b",
            "a=>b=>c",
            "a\\=>b=>c",
            "\\==>b",
            "a=>b, c=>d ,e=>f",
            "1-a=>anything at all",
            "a=>1, b=>2, c=>3",
            "cq=>l, cq=>NULL",
            "cq=>l, cq=>m, cq=>n",
            "aa=>1, cq=>l, b=>g, fg=>f, \"1\"=>NULL",
        ];
        for lit in lits {
            for soft in [0u8, 1u8] {
                let mut data = vec![0u8, soft, 0, 0, 0, 0, 0, 0, 0, 0];
                data.extend_from_slice(lit.as_bytes());
                run(&data);
            }
        }
    }

    /// recv: valid wire images round-tripped from send, plus malformed arms.
    #[test]
    fn recv_wire_shapes() {
        // pcount 2: ("a"=>"x", "bb"=>NULL)
        let mut wire = Vec::new();
        wire.extend_from_slice(&2i32.to_be_bytes());
        wire.extend_from_slice(&1i32.to_be_bytes());
        wire.extend_from_slice(b"a");
        wire.extend_from_slice(&1i32.to_be_bytes());
        wire.extend_from_slice(b"x");
        wire.extend_from_slice(&2i32.to_be_bytes());
        wire.extend_from_slice(b"bb");
        wire.extend_from_slice(&(-1i32).to_be_bytes());
        let mut data = vec![1u8];
        data.extend_from_slice(&wire);
        run(&data);

        // malformed: truncated, negative key len, negative count, bad UTF8,
        // embedded NUL, count beyond message
        for wire in [
            vec![0, 0, 0, 1],
            {
                let mut w = Vec::new();
                w.extend_from_slice(&1i32.to_be_bytes());
                w.extend_from_slice(&(-5i32).to_be_bytes());
                w
            },
            (-3i32).to_be_bytes().to_vec(),
            {
                let mut w = Vec::new();
                w.extend_from_slice(&1i32.to_be_bytes());
                w.extend_from_slice(&2i32.to_be_bytes());
                w.extend_from_slice(&[0xff, 0xfe]);
                w.extend_from_slice(&(-1i32).to_be_bytes());
                w
            },
            {
                let mut w = Vec::new();
                w.extend_from_slice(&1i32.to_be_bytes());
                w.extend_from_slice(&2i32.to_be_bytes());
                w.extend_from_slice(&[b'a', 0x00]);
                w.extend_from_slice(&(-1i32).to_be_bytes());
                w
            },
            1000i32.to_be_bytes().to_vec(),
        ] {
            let mut data = vec![1u8];
            data.extend_from_slice(&wire);
            run(&data);
        }
    }

    /// The C pair-count limit arm at its exact boundary (the fuzz arm clamps
    /// wire counts for alloc shaping; this drives the uncllamped bound).
    /// C: pcount > MaxAllocSize/sizeof(Pairs) -> 54000 BEFORE any palloc.
    #[test]
    fn recv_pair_count_limit() {
        let _serial = crate::c_oracle_serial();
        init_env();
        pin_encoding();
        unsafe { pg_hst_reset() };
        let ctx = mcx::MemoryContext::new("recv-limit");
        let mcx = ctx.mcx();
        // sizeof(Pairs) = 40 on LP64 (2 ptr + 2 size_t + 2 bool, pad to 40)
        let bound: i32 = (0x3fff_ffffi64 / 40) as i32;
        for pcount in [bound + 1, i32::MAX] {
            let wire = pcount.to_be_bytes().to_vec();
            let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
            let cret =
                unsafe { pg_hst_recv(wire.as_ptr(), wire.len() as c_int, &mut ip, &mut il) };
            assert_eq!(cret, -1, "C accepts pcount {pcount}?");
            let c_state = unsafe { pg_hst_sqlstate() };

            let mut si = stringinfo::StringInfo::with_capacity_in(mcx, wire.len() + 1).unwrap();
            si.append_bytes(&wire).unwrap();
            let (rres, _) = run_fc::<1>(
                fc("hstore_recv"),
                mcx,
                &[(Datum::from_usize(core::ptr::addr_of_mut!(si) as usize), false)],
                None,
            );
            let e = rres.expect_err("recv must reject the pair count");
            assert_eq!(
                sqlstate_of(&e),
                c_state,
                "pair-count-limit sqlstate (pcount {pcount}, msg {:?})",
                e.message
            );
        }
    }

    /// Duplicate-key tie machinery: inputs with >= 7 dup-key pairs exercise
    /// pg_qsort's unstable region; the relaxation must certify, never mask.
    #[test]
    fn dup_key_ties() {
        for text in [
            "k=>1,k=>2,k=>3,k=>4,k=>5,k=>6,k=>7,k=>8",
            "a=>1,b=>2,a=>3,b=>4,a=>5,b=>6,a=>7,b=>8,c=>9",
            "x=>1,x=>1,x=>1,x=>1,x=>1,x=>1,x=>1,x=>1",
        ] {
            let mut data = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            data.extend_from_slice(text.as_bytes());
            run(&data);
        }
    }

    /// constructors + ops arms smoke through fixed vectors.
    #[test]
    fn ops_arms_smoke() {
        // arm 3 pairwise with two overlapping maps
        let mut data = vec![3u8, 1];
        data.extend_from_slice(&7u64.to_le_bytes());
        data.push(2); // a: 2 pairs
        data.extend_from_slice(&[1, b'k', 1, 1, b'v']);
        data.extend_from_slice(&[2, b'k', b'2', 1, 1, b'w']);
        data.push(2); // b: 2 pairs
        data.extend_from_slice(&[1, b'k', 1, 1, b'v']);
        data.extend_from_slice(&[1, b'z', 0, 0]);
        data.push(1);
        data.push(b'k');
        run(&data);

        // arm 2 from_text null arms
        for flags in [0u8, 4, 8, 12] {
            let mut d = vec![2u8, flags, 3, b'a', b'b', b'c', 2, b'x', b'y'];
            d.push(0);
            run(&d);
        }
        // arm 2 from_array / from_arrays shapes
        for flags in [1u8, 5, 9, 17, 33, 49, 2, 6, 34, 66, 10, 18] {
            let mut d = vec![2u8, flags, 3];
            d.extend_from_slice(&[1, b'a', 1, 1, b'1']);
            d.extend_from_slice(&[1, b'b', 0, 1, b'2']);
            d.extend_from_slice(&[1, b'a', 1, 1, b'3']);
            run(&d);
        }
        // arm 4 array-keyed
        let mut d = vec![4u8, 0, 2];
        d.extend_from_slice(&[1, b'a', 1, 1, b'1']);
        d.extend_from_slice(&[1, b'b', 0, 1, b'2']);
        d.push(4);
        d.extend_from_slice(&[0, 1, 2, 2, b'a', b'a', 3, 1, b'q']);
        run(&d);
    }

    /// hstore_version_diag postcondition: new-format-only crate always 2
    /// (Rust-only face; C counterpart is the hstore_compat carve).
    #[test]
    fn version_diag_postcondition() {
        init_env();
        let ctx = mcx::MemoryContext::new("verdiag");
        let img = canonical_image(&[(b"a".to_vec(), None)]);
        let (r, _) = run_fc::<1>(
            fc("hstore_version_diag"),
            ctx.mcx(),
            &[(Datum::from_usize(img.as_ptr() as usize), false)],
            None,
        );
        assert_eq!(r.expect("infallible").as_u64() as u32, 2);
    }
}
