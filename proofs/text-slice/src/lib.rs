//! Kani C≡Rust equivalence: the text/bytea length + slicing family
//! (ledger rows):
//!   textlen (1257/1317/1369/1381: length/char_length/character_length),
//!   textoctetlen (1374), byteaoctetlen (720/2010),
//!   textcat (1258), byteacat (2011),
//!   textpos (849) / strpos (868) [one fmgr body, fc_textpos],
//!   byteapos (2014),
//!   text_substr (877/936), text_substr_no_len (883/937),
//!   bytea_substr (2012/2085), bytea_substr_no_len (2013/2086),
//!   text_starts_with (3696), text_left (3060), text_right (3061).
//!
//! Rust side (shipped code, path-deps — never copied): the SHIPPED FMGR
//! WRAPPERS `varlena::builtins::fc_*` and
//! `adt_oracle_compat::builtins::fc_text_left/right`, invoked through real
//! `LocalFcinfo<N>` frames with an armed result mcx (text-cmp pattern).
//! Text/bytea args are real 1-byte-header inline varlena images built with
//! the shipped `types_tuple::varatt::set_varsize_short`, so header decode,
//! the raw-image slice path (fc_*_substr -> detoast_attr_slice), the value
//! cores, and Datum packing are all inside the theorem.
//!
//! REAL SEAM IMPLS INSTALLED (not stubs — the shipped implementations are
//! inside the theorem): mbutils_seams::{pg_database_encoding_max_length,
//! pg_mbstrlen_with_len, pg_mblen_range} -> mbutils's real functions
//! (including its novel ascii_run SWAR fast path in mbstrlen — proven
//! equivalent to C's plain per-char loop here), and
//! detoast_seams::detoast_attr_slice -> the real detoast crate function
//! (its inline-image clamp tail is the code under proof; external/
//! compressed arms are unreachable for the constructed inline images —
//! their seams stay uninstalled as loud canaries).
//!
//! C side: c/pg_text_slice.c — verbatim REL_18_STABLE varlena.c/mbutils.c/
//! wchar.c/detoast.c bodies (provenance + every shim documented there).
//!
//! Fences and claims (mirror into the ledger):
//!  - ENCODING FENCE (GetDatabaseEncoding state fixed to concrete values,
//!    tz-seam-style): every mb-sensitive function is proven TWICE — one
//!    harness per concrete encoding, PG_UTF8 (eml=4) and PG_LATIN1 (eml=1,
//!    the single-byte representative) — the per-encoding case-split (the
//!    symbolic-encoding form solved but cost ~2x per pass; the split is the
//!    cheaper standing gate and doubles as the coverage witness). Other
//!    encodings are out of scope. Encoding-independent bytea/octetlen
//!    harnesses run once (textoctetlen keeps the symbolic {UTF8,LATIN1}
//!    selector as a free universality bonus).
//!  - COLLATION FENCE: collid = C_COLLATION_OID (text-cmp fence). The
//!    nondeterministic-collation arms (locale seams) are out of scope and
//!    poisoned on the C side / uninstalled-seam canaries on the Rust side.
//!  - DETOASTING of external/compressed images out of scope: inputs are
//!    inline images (post-PG_GETARG / inline-datum caller contract).
//!  - Bounds: payload len <= 8 each arg, fully symbolic bytes; substr/left/
//!    right take FULL-i32 symbolic start/length/n (the clamp arithmetic,
//!    including the pg_add_s32_overflow/checked_add overflow edges, is
//!    proven over the whole domain).
//!  - Error parity: value-space + verdict + sqlstate class (PgError::error
//!    and format machinery stubbed via proof_support — message text and
//!    Location leave the proof; the shipped .with_sqlstate calls remain
//!    load-bearing and are asserted against the C ereport errcode class).
//!  - Allocation: harness contexts are bump-family; mcx::vec_with_capacity_in
//!    / vec_append_bytes are stubbed to a static proof heap
//!    (proof_support::mcx_stubs contract) — "modulo static-buffer allocator
//!    model (allocation strategy out of scope)"; all bytes written through
//!    the allocation stay in the theorem.
//!  - DATATYPE-INVARIANT FENCE (text_starts_with, text_left, text_right,
//!    UTF8 arm only): payloads assumed valid UTF-8 (shipped
//!    wchar::pg_utf_mblen/pg_utf8_islegal as the fence predicate);
//!    text_starts_with additionally NUL-free both args. Outside the
//!    invariant C and Rust genuinely diverge (C's char-walks ereport or
//!    read stale bytes where Rust's byte-prefix logic doesn't) — text
//!    values violating encoding validity/NUL-freeness cannot be
//!    constructed through any input path, so the fence is the datatype
//!    contract, not a proof-of-convenience. control_starts_with_unfenced
//!    documents the divergence (EXPECTED FAIL).
//!
//! Negative controls (run with the DEFAULT solver; MUST FAIL):
//!  - control_byteapos_short_needle: C sees a one-shorter needle.
//!  - control_starts_with_unfenced: the invariant fence removed.
//!
//! Run (timeouts per run-all.sh, the recipe of record: 350s for the
//! per-commit/latin1/bytea tier and the controls, 600s for the release-gate
//! utf8 mb-walk tier — measured under multi-agent load, inflated ~2-3x):
//!   timeout 350 cargo kani -Z c-ffi -Z stubbing --solver kissat \
//!        --c-lib c/pg_text_slice.c --harness proofs::<name> --exact

// WAVE-8 varlena byte kernels (2026-07-28): self-contained sibling module;
// links c/pg_varlena_wave8.c (+ ../hash/pg_hashfn.c for the hash rows) next
// to c/pg_text_slice.c. See src/wave8.rs and runqueue-wave8.txt.
#[cfg(kani)]
mod wave8;

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use mcx::{Mcx, MemoryContext, PgVec};
    use proof_support;
    use std::os::raw::c_int;
    use types_core::C_COLLATION_OID;
    use types_error::{PgError, PgResult};
    use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};
    use types_tuple::varatt;
    use wchar::{PG_LATIN1, PG_UTF8};

    extern "C" {
        fn pg_set_db_encoding(enc: c_int) -> c_int;
        fn pg_take_err() -> c_int;

        fn pg_textlen(d: *const u8, len: c_int) -> c_int;
        fn pg_textoctetlen(len: c_int) -> c_int;
        fn pg_byteaoctetlen(len: c_int) -> c_int;
        fn pg_text_catenate(
            d1: *const u8,
            len1: c_int,
            d2: *const u8,
            len2: c_int,
            out: *mut u8,
        ) -> c_int;
        fn pg_bytea_catenate(
            d1: *const u8,
            len1: c_int,
            d2: *const u8,
            len2: c_int,
            out: *mut u8,
        ) -> c_int;
        fn pg_text_substring(
            d: *const u8,
            len: c_int,
            start: c_int,
            length: c_int,
            length_not_specified: c_int,
            out: *mut u8,
        ) -> c_int;
        fn pg_bytea_substring(
            d: *const u8,
            len: c_int,
            s: c_int,
            l: c_int,
            length_not_specified: c_int,
            out: *mut u8,
        ) -> c_int;
        fn pg_textpos(
            t1: *const u8,
            len1: c_int,
            t2: *const u8,
            len2: c_int,
            collid: u32,
        ) -> c_int;
        fn pg_text_starts_with(
            d1: *const u8,
            len1: c_int,
            d2: *const u8,
            len2: c_int,
            collid: u32,
        ) -> c_int;
        fn pg_byteapos(t1: *const u8, len1: c_int, t2: *const u8, len2: c_int) -> c_int;
        fn pg_text_left(d: *const u8, len: c_int, n: c_int, out: *mut u8) -> c_int;
        fn pg_text_right(d: *const u8, len: c_int, n: c_int, out: *mut u8) -> c_int;
    }

    const CAP: usize = 8;
    const COLL_C: u32 = C_COLLATION_OID;
    /// C-side ereport sentinel (shim 4 in c/pg_text_slice.c).
    const PG_CERR: c_int = -2_100_000_000;

    // C errflag classes (shim 4) <-> Rust sqlstates.
    fn err_class(e: &PgError) -> c_int {
        use types_error::*;
        if e.sqlstate == ERRCODE_SUBSTRING_ERROR {
            1
        } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
            2
        } else if e.sqlstate == ERRCODE_INDETERMINATE_COLLATION {
            3
        } else if e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED {
            4
        } else {
            -1
        }
    }

    /// Install the REAL seam implementations + pin the encoding state on
    /// both sides. Called exactly once per harness.
    fn install_env(enc: i32) {
        mbutils_seams::pg_database_encoding_max_length::set(
            mbutils::pg_database_encoding_max_length,
        );
        mbutils_seams::pg_mbstrlen_with_len::set(mbutils::pg_mbstrlen_with_len);
        mbutils_seams::pg_mblen_range::set(mbutils::pg_mblen_range);
        mbutils::SetDatabaseEncoding(enc).expect("valid backend encoding");
        let _ = unsafe { pg_set_db_encoding(enc) };
    }

    /// The real detoast slice path — needed (reachable) only by the substr
    /// harnesses; elsewhere the seam stays an uninstalled loud canary so the
    /// detoast/pglz code never enters those formulas.
    fn install_detoast() {
        detoast_seams::detoast_attr_slice::set(detoast::detoast_attr_slice);
    }

    /// Symbolic encoding over the fence {PG_UTF8, PG_LATIN1}.
    fn sym_enc() -> i32 {
        if kani::any() {
            PG_UTF8
        } else {
            PG_LATIN1
        }
    }

    /// One inline 1-byte-header varlena image over symbolic payload bytes +
    /// symbolic length <= CAP, built with the SHIPPED header encoder
    /// (text-cmp pattern).
    struct VarImg {
        img: [u8; CAP + varatt::VARHDRSZ_SHORT],
        len: usize,
    }

    fn sym_varlena() -> VarImg {
        let payload: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut img = [0u8; CAP + varatt::VARHDRSZ_SHORT];
        // SAFETY: img is writable and len + 1 <= VARATT_SHORT_MAX.
        unsafe { varatt::set_varsize_short(img.as_mut_ptr(), len + varatt::VARHDRSZ_SHORT) };
        let mut i = 0;
        while i < CAP {
            img[i + varatt::VARHDRSZ_SHORT] = payload[i];
            i += 1;
        }
        VarImg { img, len }
    }

    impl VarImg {
        fn datum(&self) -> Datum {
            Datum::from_usize(self.img.as_ptr() as usize)
        }
        fn data(&self) -> *const u8 {
            self.img[varatt::VARHDRSZ_SHORT..].as_ptr()
        }
        fn payload(&self) -> &[u8] {
            &self.img[varatt::VARHDRSZ_SHORT..varatt::VARHDRSZ_SHORT + self.len]
        }
        fn clen(&self) -> c_int {
            self.len as c_int
        }
    }

    /// DATATYPE-INVARIANT FENCE, UTF8 arm: payload is a whole number of
    /// legal UTF-8 characters (shipped wchar kernels as the predicate —
    /// themselves proven C-equivalent in proofs/utf8). LATIN1: all bytes
    /// legal, no fence.
    fn assume_valid(enc: i32, v: &VarImg) {
        if enc == PG_UTF8 {
            let data = v.payload();
            let mut i = 0usize;
            while i < data.len() {
                let l = wchar::pg_utf_mblen(&data[i..]) as usize;
                kani::assume(i + l <= data.len());
                kani::assume(wchar::pg_utf8_islegal(&data[i..], l as i32));
                i += l;
            }
        }
    }

    fn assume_nul_free(v: &VarImg) {
        let data = v.payload();
        let mut i = 0usize;
        while i < data.len() {
            kani::assume(data[i] != 0);
            i += 1;
        }
    }

    /// The shipped fc_* wrapper shape.
    type FcFn = fn(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> PgResult<Datum>;

    /// Run a shipped fc_* wrapper on a real N-arg frame with an armed
    /// result mcx (the new-by-ref result convention).
    fn call<const N: usize>(fc: FcFn, args: [Datum; N], collid: u32, mcx: Mcx<'_>) -> PgResult<Datum> {
        let mut f = LocalFcinfo::<N>::new(collid);
        for (slot, d) in f.args.iter_mut().zip(args) {
            *slot = NullableDatum::value(d);
        }
        // SAFETY: the context outlives the call (harness stack frame).
        unsafe { f.set_result_mcx(mcx) };
        fc(None, &mut f)
    }

    /// [`call`] without an armed result mcx, for scalar-result wrappers.
    /// result_mcx() is only reachable through the packed-arg detoast branch,
    /// which is UNSAT for the constructed inline images — if it ever fired
    /// it would panic loudly (result_mcx_unarmed), failing the proof.
    fn call_scalar<const N: usize>(fc: FcFn, args: [Datum; N], collid: u32) -> PgResult<Datum> {
        let mut f = LocalFcinfo::<N>::new(collid);
        for (slot, d) in f.args.iter_mut().zip(args) {
            *slot = NullableDatum::value(d);
        }
        fc(None, &mut f)
    }

    /// Payload bytes of a shipped 4B-header result image.
    unsafe fn out_payload<'a>(d: Datum) -> &'a [u8] {
        let p = d.as_usize() as *const u8;
        let total = varatt::varsize_any(p);
        core::slice::from_raw_parts(p.add(varatt::VARHDRSZ), total - varatt::VARHDRSZ)
    }

    /// Assert the C call and the Rust call agreed on scalar value + error
    /// verdict + sqlstate class. Single property: external kissat re-solves
    /// the whole formula per property, so the parity claim is one assert.
    fn check_i32(r: PgResult<Datum>, c: c_int) {
        let cerr = unsafe { pg_take_err() };
        let ok = match r {
            Ok(d) => c != PG_CERR && cerr == 0 && d.as_i32() == c,
            Err(e) => c == PG_CERR && err_class(&e) == cerr,
        };
        assert!(ok, "C/Rust divergence (value or error verdict/class)");
    }

    /// Assert C (out buffer + returned length) == Rust (result image
    /// payload), or matching error verdict + class. Single property (see
    /// check_i32).
    fn check_bytes(r: PgResult<Datum>, c: c_int, out: &[u8]) {
        let cerr = unsafe { pg_take_err() };
        let ok = match r {
            Ok(d) => {
                let rp = unsafe { out_payload(d) };
                let mut same = c != PG_CERR && cerr == 0 && rp.len() == c as usize;
                let mut i = 0usize;
                while i < rp.len() && i < out.len() {
                    same = same && rp[i] == out[i];
                    i += 1;
                }
                same && rp.len() <= out.len()
            }
            Err(e) => c == PG_CERR && err_class(&e) == cerr,
        };
        assert!(ok, "C/Rust divergence (bytes or error verdict/class)");
    }

    /// Stub for `mcx::vec_append_bytes` under proof: identical semantics for
    /// within-capacity appends (every call site under proof reserves its
    /// exact final size first); a capacity overrun fails the proof loudly
    /// instead of re-entering the real allocator's grow path.
    pub fn stub_vec_append_bytes(v: &mut PgVec<'_, u8>, bytes: &[u8]) -> PgResult<()> {
        assert!(
            v.len() + bytes.len() <= v.capacity(),
            "proof stub: append beyond reserved capacity"
        );
        let old = v.len();
        // SAFETY: capacity checked above; src/dst disjoint; set_len covers
        // exactly the bytes written.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), v.as_mut_ptr().add(old), bytes.len());
            v.set_len(old + bytes.len());
        }
        Ok(())
    }

    /// Stub for `mbutils::byte_sequence` (the invalid-byte-sequence error
    /// MESSAGE detail: a symbolic-capacity String build that walls symex).
    /// Message text leaves the proof; the error VALUE/verdict/sqlstate stay
    /// in (same contract as stub_format).
    pub fn stub_byte_sequence(_mbstr: &[u8], _mblen: i32, _len: i32) -> String {
        String::new()
    }

    /// Reachability-canary stubs for the pglz decompress arms of
    /// detoast_attr_slice: the harness images are inline uncompressed, so
    /// these MUST be unreachable — the stub panics loudly if the partition
    /// predicate is wrong, and keeps the (dead) pglz decompress loops out of
    /// the unwound formula (dead unreachable loop copies still enter the
    /// formula — TRIAGE unwind-slack lesson).
    pub fn stub_decompress_unreachable<'mcx>(
        _mcx: Mcx<'mcx>,
        _attr: &[u8],
    ) -> PgResult<mcx::PgVec<'mcx, u8>> {
        panic!("toast_decompress_datum reached — harness images are inline");
    }

    pub fn stub_decompress_slice_unreachable<'mcx>(
        _mcx: Mcx<'mcx>,
        _attr: &[u8],
        _slicelength: i32,
    ) -> PgResult<mcx::PgVec<'mcx, u8>> {
        panic!("toast_decompress_datum_slice reached — harness images are inline");
    }

    /// Stub for `mcx::local_pool_on`: its OnceLock (std Once queue + thread
    /// parker + dispatch semaphores) walls symex. `false` selects the
    /// global PoolMutex recycling arm (single CAS) — pool SELECTION is
    /// allocation strategy, out of every equivalence claim.
    pub fn stub_local_pool_on() -> bool {
        false
    }

    /// Stub for `std::env::var` (mcx::local_pool_on's one-shot
    /// PGRUST_MCX_POOL_STRIPE read on the MemoryContext construction path):
    /// OsString/env machinery walls symex. Environment state is harness
    /// scaffolding, not part of any equivalence claim; "not present" selects
    /// the default pool posture either way (the pools only recycle buffers).
    pub fn stub_env_var<K: AsRef<std::ffi::OsStr>>(
        _key: K,
    ) -> Result<String, std::env::VarError> {
        Err(std::env::VarError::NotPresent)
    }

    // Common stub set: allocation -> proof heap; error message plumbing out
    // of the proof (value/verdict/sqlstate stay in).
    macro_rules! text_slice_proof {
        ($(#[$attr:meta])* fn $name:ident() $body:block) => {
            #[kani::proof]
            $(#[$attr])*
            #[kani::stub(mcx::vec_with_capacity_in, proof_support::mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(mcx::vec_append_bytes, stub_vec_append_bytes)]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
            #[kani::stub(std::env::var, stub_env_var)]
            #[kani::stub(mbutils::byte_sequence, stub_byte_sequence)]
            #[kani::stub(mcx::local_pool_on, stub_local_pool_on)]
            #[kani::stub(detoast::toast_decompress_datum, stub_decompress_unreachable)]
            #[kani::stub(detoast::toast_decompress_datum_slice, stub_decompress_slice_unreachable)]
            fn $name() $body
        };
    }

    // ================= cost probes (not standing gates) =================

    text_slice_proof! {
        #[kani::unwind(12)]
        fn probe_env_only() {
            let enc = sym_enc();
            install_env(enc);
            let a = sym_varlena();
            let c = unsafe { pg_textlen(a.data(), a.clen()) };
            kani::assume(c != PG_CERR);
            let _ = unsafe { pg_take_err() };
            assert!(c >= 0);
        }
    }

    text_slice_proof! {
        #[kani::unwind(12)]
        fn probe_ctx_construct() {
            let ctx = MemoryContext::new_bump("proof");
            let _ = ctx.mcx();
            assert!(true);
        }
    }

    text_slice_proof! {
        #[kani::unwind(12)]
        fn probe_ctx_forget() {
            let ctx = MemoryContext::new_bump("proof");
            let _ = ctx.mcx();
            core::mem::forget(ctx);
            assert!(true);
        }
    }

    text_slice_proof! {
        #[kani::unwind(12)]
        fn probe_ctx_only() {
            let a = sym_varlena();
            let r = call_scalar(varlena::builtins::fc_byteaoctetlen, [a.datum()], COLL_C);
            assert!(r.is_ok());
        }
    }

    // ================= lengths =================

    fn body_textlen(enc: i32) {
        install_env(enc);
        let a = sym_varlena();
        let c = unsafe { pg_textlen(a.data(), a.clen()) };
        let r = call_scalar(varlena::builtins::fc_textlen, [a.datum()], COLL_C);
        check_i32(r, c);
    }

    text_slice_proof! {
        #[kani::unwind(10)]
        fn eq_textlen_utf8() { body_textlen(PG_UTF8); }
    }

    text_slice_proof! {
        #[kani::unwind(10)]
        fn eq_textlen_latin1() { body_textlen(PG_LATIN1); }
    }

    text_slice_proof! {
        #[kani::unwind(12)]
        fn eq_textoctetlen() {
            let enc = sym_enc();
            install_env(enc);
            let a = sym_varlena();
            let c = unsafe { pg_textoctetlen(a.clen()) };
            let r = call_scalar(varlena::builtins::fc_textoctetlen, [a.datum()], COLL_C);
            check_i32(r, c);
        }
    }

    text_slice_proof! {
        #[kani::unwind(12)]
        fn eq_byteaoctetlen() {
            let a = sym_varlena();
            let c = unsafe { pg_byteaoctetlen(a.clen()) };
            let r = call_scalar(varlena::builtins::fc_byteaoctetlen, [a.datum()], COLL_C);
            check_i32(r, c);
        }
    }

    // ================= catenate =================

    text_slice_proof! {
        #[kani::unwind(20)]
        fn eq_textcat() {
            // text_catenate is encoding-independent; seams stay uninstalled
            // canaries.
            let (a, b) = (sym_varlena(), sym_varlena());
            let ctx = MemoryContext::new_bump("proof");
            let mut out = [0u8; 2 * CAP];
            let c = unsafe {
                pg_text_catenate(a.data(), a.clen(), b.data(), b.clen(), out.as_mut_ptr())
            };
            let r = call(varlena::builtins::fc_textcat, [a.datum(), b.datum()], COLL_C, ctx.mcx());
            core::mem::forget(ctx);
            kani::cover!(a.len > 0 && b.len > 0);
            check_bytes(r, c, &out);
        }
    }

    text_slice_proof! {
        #[kani::unwind(20)]
        fn eq_byteacat() {
            let (a, b) = (sym_varlena(), sym_varlena());
            let ctx = MemoryContext::new_bump("proof");
            let mut out = [0u8; 2 * CAP];
            let c = unsafe {
                pg_bytea_catenate(a.data(), a.clen(), b.data(), b.clen(), out.as_mut_ptr())
            };
            let r = call(varlena::builtins::fc_byteacat, [a.datum(), b.datum()], COLL_C, ctx.mcx());
            core::mem::forget(ctx);
            check_bytes(r, c, &out);
        }
    }

    // ================= substr =================
    // Raw-image argument (fc_*_substr take arg_varlena_raw and route through
    // detoast_attr_slice) — the shipped slice/clamp path is in-theorem.
    // start/length fully symbolic i32 (overflow edges included).

    fn body_text_substr(enc: i32) {
        install_detoast();
        install_env(enc);
        let a = sym_varlena();
        let start: i32 = kani::any();
        let length: i32 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; CAP];
        let c = unsafe {
            pg_text_substring(a.data(), a.clen(), start, length, 0, out.as_mut_ptr())
        };
        let r = call(
            varlena::builtins::fc_text_substr,
            [a.datum(), Datum::from_i32(start), Datum::from_i32(length)],
            COLL_C,
            ctx.mcx(),
        );
        // Drop of MemoryContext (arena/acct recycling) is allocator
        // machinery outside every claim — and a measured symex wall. Leak it.
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_substr_utf8() { body_text_substr(PG_UTF8); }
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_substr_latin1() { body_text_substr(PG_LATIN1); }
    }

    /// Clamp-regime reachability witnesses for the substr family, kept out
    /// of the equality harnesses (each cover is a separate external-solver
    /// pass): proper slice from a negative start + the error arm.
    text_slice_proof! {
        #[kani::unwind(14)]
        fn cover_text_substr_regimes() {
            install_detoast();
            install_env(PG_UTF8);
            let a = sym_varlena();
            let start: i32 = kani::any();
            let length: i32 = kani::any();
            let mut out = [0u8; CAP];
            let c = unsafe {
                pg_text_substring(a.data(), a.clen(), start, length, 0, out.as_mut_ptr())
            };
            let _ = unsafe { pg_take_err() };
            kani::cover!(start < 0 && c > 0 && (c as usize) < a.len);
            kani::cover!(c == PG_CERR);
        }
    }

    fn body_text_substr_no_len(enc: i32) {
        install_detoast();
        install_env(enc);
        let a = sym_varlena();
        let start: i32 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; CAP];
        let c = unsafe {
            pg_text_substring(a.data(), a.clen(), start, -1, 1, out.as_mut_ptr())
        };
        let r = call(
            varlena::builtins::fc_text_substr_no_len,
            [a.datum(), Datum::from_i32(start)],
            COLL_C,
            ctx.mcx(),
        );
        // Drop of MemoryContext (arena/acct recycling) is allocator
        // machinery outside every claim — and a measured symex wall. Leak it.
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_substr_no_len_utf8() { body_text_substr_no_len(PG_UTF8); }
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_substr_no_len_latin1() { body_text_substr_no_len(PG_LATIN1); }
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_bytea_substr() {
            install_detoast();
            let a = sym_varlena();
            let start: i32 = kani::any();
            let length: i32 = kani::any();
            let ctx = MemoryContext::new_bump("proof");
            let mut out = [0u8; CAP];
            let c = unsafe {
                pg_bytea_substring(a.data(), a.clen(), start, length, 0, out.as_mut_ptr())
            };
            let r = call(
                varlena::builtins::fc_bytea_substr,
                [a.datum(), Datum::from_i32(start), Datum::from_i32(length)],
                COLL_C,
                ctx.mcx(),
            );
            core::mem::forget(ctx);
            check_bytes(r, c, &out);
        }
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_bytea_substr_no_len() {
            install_detoast();
            let a = sym_varlena();
            let start: i32 = kani::any();
            let ctx = MemoryContext::new_bump("proof");
            let mut out = [0u8; CAP];
            let c = unsafe {
                pg_bytea_substring(a.data(), a.clen(), start, -1, 1, out.as_mut_ptr())
            };
            let r = call(
                varlena::builtins::fc_bytea_substr_no_len,
                [a.datum(), Datum::from_i32(start)],
                COLL_C,
                ctx.mcx(),
            );
            core::mem::forget(ctx);
            check_bytes(r, c, &out);
        }
    }

    // ================= position =================

    fn body_textpos(enc: i32) {
        install_env(enc);
        let (a, b) = (sym_varlena(), sym_varlena());
        let c = unsafe { pg_textpos(a.data(), a.clen(), b.data(), b.clen(), COLL_C) };
        let r = call_scalar(varlena::builtins::fc_textpos, [a.datum(), b.datum()], COLL_C);
        check_i32(r, c);
    }

    /// B-M-H reachability witnesses (hit past position 1 + miss), kept out
    /// of the equality harnesses (each cover is a separate solver pass).
    text_slice_proof! {
        #[kani::unwind(10)]
        fn cover_textpos_regimes() {
            install_env(PG_LATIN1);
            let (a, b) = (sym_varlena(), sym_varlena());
            let c = unsafe { pg_textpos(a.data(), a.clen(), b.data(), b.clen(), COLL_C) };
            let _ = unsafe { pg_take_err() };
            kani::cover!(b.len > 1 && c > 1);
            kani::cover!(b.len > 1 && c == 0);
        }
    }

    text_slice_proof! {
        // unwind(10) truncates mbutils::ascii_run / pg_encoding_
        // mbstrlen_with_len ("Not unwinding loop .. iteration 10" +
        // VERIFICATION FAILED = unwinding-assertion artifact, decoded
        // 2026-07-28); 11 is the minimal bound past the truncated
        // iteration (13 blew the 6GiB RSS cap — unwind slack is
        // catastrophic here, TRIAGE lesson).
        #[kani::unwind(11)]
        fn eq_textpos_utf8() { body_textpos(PG_UTF8); }
    }

    text_slice_proof! {
        #[kani::unwind(10)]
        fn eq_textpos_latin1() { body_textpos(PG_LATIN1); }
    }

    text_slice_proof! {
        #[kani::unwind(10)]
        fn eq_byteapos() {
            let (a, b) = (sym_varlena(), sym_varlena());
            let c = unsafe { pg_byteapos(a.data(), a.clen(), b.data(), b.clen()) };
            let r = call_scalar(varlena::builtins::fc_byteapos, [a.datum(), b.datum()], COLL_C);
            check_i32(r, c);
        }
    }

    // ================= starts_with =================
    // DATATYPE-INVARIANT FENCE: valid encoding + NUL-free (see module doc).

    fn body_starts_with(enc: i32) {
        install_env(enc);
        let (a, b) = (sym_varlena(), sym_varlena());
        assume_valid(enc, &a);
        assume_valid(enc, &b);
        assume_nul_free(&a);
        assume_nul_free(&b);
        let c = unsafe {
            pg_text_starts_with(a.data(), a.clen(), b.data(), b.clen(), COLL_C)
        };
        let r = call_scalar(
            varlena::builtins::fc_text_starts_with,
            [a.datum(), b.datum()],
            COLL_C,
        );
        let cerr = unsafe { pg_take_err() };
        let ok = match r {
            Ok(d) => c != PG_CERR && cerr == 0 && d.as_bool() == (c != 0),
            Err(e) => c == PG_CERR && err_class(&e) == cerr,
        };
        assert!(ok, "C/Rust divergence (verdict or error class)");
    }

    text_slice_proof! {
        #[kani::unwind(12)]
        fn eq_text_starts_with_utf8() { body_starts_with(PG_UTF8); }
    }

    text_slice_proof! {
        #[kani::unwind(12)]
        fn eq_text_starts_with_latin1() { body_starts_with(PG_LATIN1); }
    }

    // ================= left / right =================
    // n fully symbolic i32; UTF8 arm under the valid-encoding invariant
    // fence (C routes through text_substring's char walk, Rust through
    // pg_mbcharcliplen — outside the invariant their ereport behavior
    // genuinely differs; see module doc).

    fn body_text_left(enc: i32) {
        install_env(enc);
        let a = sym_varlena();
        assume_valid(enc, &a);
        let n: i32 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; CAP];
        let c = unsafe { pg_text_left(a.data(), a.clen(), n, out.as_mut_ptr()) };
        let r = call(
            adt_oracle_compat::builtins::fc_text_left,
            [a.datum(), Datum::from_i32(n)],
            COLL_C,
            ctx.mcx(),
        );
        // Drop of MemoryContext (arena/acct recycling) is allocator
        // machinery outside every claim — and a measured symex wall. Leak it.
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_left_utf8() { body_text_left(PG_UTF8); }
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_left_latin1() { body_text_left(PG_LATIN1); }
    }

    fn body_text_right(enc: i32) {
        install_env(enc);
        let a = sym_varlena();
        assume_valid(enc, &a);
        let n: i32 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; CAP];
        let c = unsafe { pg_text_right(a.data(), a.clen(), n, out.as_mut_ptr()) };
        let r = call(
            adt_oracle_compat::builtins::fc_text_right,
            [a.datum(), Datum::from_i32(n)],
            COLL_C,
            ctx.mcx(),
        );
        // Drop of MemoryContext (arena/acct recycling) is allocator
        // machinery outside every claim — and a measured symex wall. Leak it.
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_right_utf8() { body_text_right(PG_UTF8); }
    }

    text_slice_proof! {
        #[kani::unwind(14)]
        fn eq_text_right_latin1() { body_text_right(PG_LATIN1); }
    }

    // ============ negative controls (MUST FAIL: rig non-vacuity) ============
    // Run with the DEFAULT solver (kissat never terminates on failing
    // harnesses).

    /// C sees a one-byte-shorter needle than the shipped wrapper.
    /// Expected: VERIFICATION FAILED with a decodable counterexample.
    text_slice_proof! {
        #[kani::unwind(12)]
        fn control_byteapos_short_needle() {
            let (a, b) = (sym_varlena(), sym_varlena());
            kani::assume(b.len >= 1);
            let c = unsafe { pg_byteapos(a.data(), a.clen(), b.data(), b.clen() - 1) };
            let r = call_scalar(varlena::builtins::fc_byteapos, [a.datum(), b.datum()], COLL_C);
            check_i32(r, c);
        }
    }

    /// eq_text_starts_with WITHOUT the datatype-invariant fence: documents
    /// that outside valid-encoding/NUL-free text the C and Rust bodies
    /// genuinely diverge (C's substring char-walk ereports / reads stale
    /// bytes; Rust's byte-prefix compare doesn't). Expected: VERIFICATION
    /// FAILED — the fence is load-bearing.
    text_slice_proof! {
        #[kani::unwind(12)]
        fn control_starts_with_unfenced() {
            let enc = PG_UTF8;
            install_env(enc);
            let (a, b) = (sym_varlena(), sym_varlena());
            let c = unsafe {
                pg_text_starts_with(a.data(), a.clen(), b.data(), b.clen(), COLL_C)
            };
            let r = call_scalar(
                varlena::builtins::fc_text_starts_with,
                [a.datum(), b.datum()],
                COLL_C,
            );
            let cerr = unsafe { pg_take_err() };
            let ok = match r {
                Ok(d) => c != PG_CERR && cerr == 0 && d.as_bool() == (c != 0),
                Err(e) => c == PG_CERR && err_class(&e) == cerr,
            };
            assert!(ok, "C/Rust divergence (verdict or error class)");
        }
    }
}
