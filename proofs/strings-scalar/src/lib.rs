//! Kani C≡Rust equivalence: strings/toast-hdr scalar-verdict batch
//! (tail-triage lane, pre-built 2026-07-29 — compile-gated only, ZERO solves).
//!
//! Ledger rows hosted here (oid / function / theorem):
//!   1244 byteain          — escaped-style pass-one accept/reject + decoded
//!        byte-count parity (symbolic len<=8, cstring contract: no interior
//!        NUL), plus a decoded-content harness at cap 4. The "\x" hex arm is
//!        FENCED OUT (delegates to hex_decode, owned by proofs/hex); the C
//!        side traps on it so the fence failing is loud. Value-space only:
//!        error message text/location out of proof (PgError/format stubs);
//!        allocation modulo static-buffer allocator model.
//!   1269 pg_column_size   — fc wrapper vs C body per typlen plane
//!        (-1 varlena / -2 cstring / >0 fixed), typlen via an identical
//!        harness-set seam both sides (catalog lookup out of theorem;
//!        Rust side: cached_arg_typlen stub). Varlena plane fences the
//!        indirect/expanded arms (C traps them) — trusted-header fence:
//!        images are 1B-short, 4B, or 18-byte ondisk toast pointers.
//!   2121 pg_column_compression — NULL / "pglz" / "lz4" / invalid-id
//!        which-verdict parity incl. result text bytes; same typlen seam +
//!        header fence; mcx-stubs for the 4-byte text result.
//!   6316 pg_column_toast_chunk_id — NULL-vs-va_valueid parity, same rig.
//!   5050 btvarstrequalimage — verdict + 42P22-error-class parity over fully
//!        symbolic collid and a fully symbolic deterministic seam value
//!        (tz-seam pattern: pg_newlocale_from_collation/`collation_is_
//!        deterministic` stubbed IDENTICALLY both sides; fence: C/POSIX
//!        collids are deterministic=true, C's builtin locales). Skew
//!        control proves the seam model is load-bearing.
//!
//! SCREENED OUT of this crate (cross-checked against the ledger + sibling
//! crates before building — do not re-host):
//!   - trim windows 401/881/882/885, 875/876/884, bytea trims 2015/6195/6196:
//!     owned by proofs/oracle-compat (wave-14 runqueue).
//!   - bpchar/varchar clip cores 1044/1046 (+ coerces 668/669, name casts
//!     408/409, typmods 2913-2916, lens 1318/1367/1372/1375): owned by
//!     proofs/oracle-compat (wave-14 runqueue, c/pg_varchar.c rig).
//!   - btnamecmp 359: owned by proofs/name-ascii (eq_btnamecmp_full64).
//!
//! Shipped-code edit (visibility-only): varlena/src/builtins.rs
//! `cached_arg_typlen` fn -> pub fn (Kani stub target; behavior unchanged).
//!
//! C side: c/pg_strings_scalar.c (REL_18_STABLE verbatim; see its header
//! for every shim) + c/varatt_rel18.h (verbatim varatt.h, LE arm).
//!
//! Runqueue: ./runqueue.txt. Compile gate:
//!   cargo kani --only-codegen -Z c-ffi -Z stubbing --c-lib c/pg_strings_scalar.c

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use mcx::{Mcx, MemoryContext, PgVec};
    // Load-bearing for #[kani::stub] path resolution (stub paths resolve
    // relative to the harness module — prove-target gotcha).
    #[allow(unused_imports)]
    use proof_support;
    use std::os::raw::{c_char, c_int};
    use types_core::{Oid, C_COLLATION_OID, POSIX_COLLATION_OID};
    use types_error::PgResult;
    use types_fmgr::FmgrInfo;

    extern "C" {
        // ---- section A: byteain ----
        fn pg_byteain(input: *const c_char, out: *mut u8, out_bc: *mut c_int) -> c_int;
        // ---- section B: toast headers ----
        fn pg_set_typlen(typlen: c_int) -> c_int;
        fn pg_toast_datum_size(attr: *const u8) -> u64;
        fn pg_toast_get_compression_id(attr: *const u8) -> c_int;
        fn pg_column_size_c(value: *const u8, value_as_cstring: *const c_char) -> i32;
        fn pg_column_compression_c(
            attr: *const u8,
            out_name: *mut c_char,
            out_name_len: *mut c_int,
        ) -> c_int;
        fn pg_column_toast_chunk_id_c(attr: *const u8, out_valueid: *mut u32) -> c_int;
        // ---- section C: btvarstrequalimage ----
        fn pg_set_locale_det(det: c_int) -> c_int;
        fn pg_btvarstrequalimage(collid: u32) -> c_int;
    }

    const PG_TOAST_TRAP: u64 = 0xDEAD_DEAD;

    // =====================================================================
    // shared stubs (oracle-compat/text-slice recipe)
    // =====================================================================

    /// Stub for `mcx::vec_append_bytes`: identical semantics for
    /// within-capacity appends (call sites under proof reserve first); a
    /// capacity overrun fails the proof loudly instead of re-entering the
    /// real grow path.
    #[allow(dead_code)] // referenced only from #[kani::stub] attributes
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

    /// Stub for `mcx::local_pool_on`: pool SELECTION is allocation strategy,
    /// out of every equivalence claim (OnceLock machinery walls symex).
    #[allow(dead_code)]
    pub fn stub_local_pool_on() -> bool {
        false
    }

    // ---- typlen seam (Rust side of the C pg_set_typlen static) ----
    static mut R_TYPLEN: i16 = 0;

    /// Stub for `varlena::builtins::cached_arg_typlen`: the fn_extra-memoized
    /// get_fn_expr_argtype + get_typlen catalog lookup leaves the theorem;
    /// both sides read the same harness-set typlen (state-seam pattern).
    #[allow(dead_code)]
    pub fn stub_cached_arg_typlen(_flinfo: &mut FmgrInfo, _argno: i16) -> PgResult<i16> {
        Ok(unsafe { R_TYPLEN })
    }

    fn set_typlen(t: i16) {
        unsafe { R_TYPLEN = t };
        unsafe { pg_set_typlen(t as c_int) };
    }

    // ---- deterministic-collation seam (Rust side of pg_set_locale_det) ----
    static mut R_DET: bool = true;

    fn seam_collation_is_deterministic(_collid: Oid) -> PgResult<bool> {
        Ok(unsafe { R_DET })
    }

    /// Install the locale seam + pin the symbolic deterministic answer on
    /// both sides. Fence (documented in the module header): C's builtin
    /// C/POSIX locales are deterministic=true, and the shipped fast path
    /// never consults the seam for them — so the universally quantified det
    /// value is constrained to true there.
    fn arm_det_seam(collid: Oid, det: bool) {
        kani::assume(
            !(collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID) || det,
        );
        unsafe { R_DET = det };
        unsafe { pg_set_locale_det(det as c_int) };
        pg_locale_seams::collation_is_deterministic::set(seam_collation_is_deterministic);
    }

    // Common stub set: allocation -> proof heap; error message plumbing out
    // of the proof (value/verdict stay in).
    macro_rules! ss_proof {
        ($(#[$attr:meta])* fn $name:ident() $body:block) => {
            #[kani::proof]
            $(#[$attr])*
            #[kani::stub(mcx::Mcx::allocate, proof_support::mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, proof_support::mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, proof_support::mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, proof_support::mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(mcx::vec_append_bytes, stub_vec_append_bytes)]
            #[kani::stub(mcx::local_pool_on, stub_local_pool_on)]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
            #[kani::stub(std::env::var, proof_support::stub_env_var)]
            #[kani::stub(varlena::builtins::cached_arg_typlen, stub_cached_arg_typlen)]
            fn $name() $body
        };
    }

    /// Harness-scoped bump context; forgotten by the caller after use
    /// (teardown is not part of any claim and walls symex — mcx-stubs recipe).
    fn with_ctx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
        let ctx = MemoryContext::new_bump("proof");
        let r = f(ctx.mcx());
        core::mem::forget(ctx);
        r
    }

    /// Static-slot context (datetime-b dummy_mcx recipe): NO MemoryContext
    /// construction, so the AcctWeak-registry retain sweep never enters the
    /// formula (real new_bump unrolled that Vec::retain unboundedly and
    /// walled the typlen-skew control >350s before its counterexample).
    /// Sound only where every Allocator entry point is stubbed and nothing
    /// reads context state — true for all ss_proof! harnesses.
    fn dummy_mcx() -> Mcx<'static> {
        const _: () = assert!(core::mem::size_of::<MemoryContext>() <= 1024);
        const _: () = assert!(core::mem::align_of::<MemoryContext>() <= 16);
        #[repr(align(16))]
        struct DummySlot([u8; 1024]);
        // SAFETY: the slot is never read or written through.
        unsafe impl Sync for DummySlot {}
        static SLOT: DummySlot = DummySlot([0u8; 1024]);
        // SAFETY: never dereferenced — allocator entry points are stubbed.
        let ctx: &'static MemoryContext = unsafe { &*(SLOT.0.as_ptr() as *const MemoryContext) };
        ctx.mcx()
    }

    // =====================================================================
    // A. byteain (row 1244) — escaped-style pass-one scalar verdicts
    // =====================================================================

    /// Symbolic NUL-terminated cstring of len<=CAP: unused tail
    /// zero-literal-filled (dead-symbolic-bytes law), interior bytes
    /// assumed non-NUL (the cstring contract byteain's C signature imposes;
    /// the Rust slice carries the same bytes).
    fn sym_cstring<const CAP: usize>() -> ([u8; CAP], usize) {
        // one spare slot for the C terminator: callers pass CAP = cap + 1
        let raw: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len < CAP);
        let mut buf = [0u8; CAP];
        let mut i = 0;
        while i < CAP - 1 {
            if i < len {
                kani::assume(raw[i] != 0);
                buf[i] = raw[i];
            }
            i += 1;
        }
        (buf, len)
    }

    fn fence_not_hex(buf: &[u8], len: usize) {
        kani::assume(!(len >= 2 && buf[0] == b'\\' && buf[1] == b'x'));
    }

    fn body_byteain(compare_content: bool, cap_plus_1: usize) {
        // buffers are fixed [u8; 9]; cap_plus_1 <= 9 narrows the length band
        let (buf, len) = sym_cstring::<9>();
        kani::assume(len < cap_plus_1);
        fence_not_hex(&buf, len);

        let mut c_out = [0u8; 9];
        let mut c_bc: c_int = 0;
        let c = unsafe {
            pg_byteain(
                buf.as_ptr() as *const c_char,
                c_out.as_mut_ptr(),
                &mut c_bc,
            )
        };
        assert!(c != -2, "hex-arm trap reached despite fence");

        with_ctx(|mcx| {
            let r = varlena::bytea::byteain(mcx, &buf[..len], None);
            match r {
                Ok(Some(v)) => {
                    assert!(c == 1, "C rejected where Rust accepted");
                    let img = v.as_bytes();
                    assert!(
                        img.len() == 4 + c_bc as usize,
                        "decoded byte-count mismatch"
                    );
                    if compare_content {
                        let mut i = 0;
                        while i < c_bc as usize {
                            assert!(img[4 + i] == c_out[i], "decoded byte mismatch");
                            i += 1;
                        }
                    }
                    core::mem::forget(v);
                }
                Ok(None) => panic!("soft-error escape without an escontext"),
                Err(e) => {
                    // verdict-only on the error arm (message out of proof)
                    core::mem::forget(e);
                    assert!(c == 0, "C accepted where Rust rejected");
                }
            }
        });
    }

    ss_proof! {
        /// row 1244: accept/reject + byte-count parity, all byte strings
        /// len<=8 (both escape forms + reject class in one theorem).
        #[kani::unwind(11)]
        fn eq_byteain_escape_verdict() { body_byteain(false, 9) }
    }

    ss_proof! {
        /// row 1244: decoded-content parity at len<=4 (result-image claim
        /// kept narrow: derived-offset stores — expect the ladder).
        /// unwind 10: sym_cstring::<9>'s fill loop runs CAP-1 = 8
        /// iterations (fleet 31ad423d unwinding assertion at unwind(7)).
        #[kani::unwind(10)]
        fn eq_byteain_escape_content_len4() { body_byteain(true, 5) }
    }

    ss_proof! {
        /// Regime coverage for the verdict theorem (vacuity insurance):
        /// plain byte, octal escape, double backslash, reject all reachable
        /// under the harness fences. DEFAULT solver.
        #[kani::unwind(11)]
        fn cover_byteain_regimes() {
            let (buf, len) = sym_cstring::<9>();
            fence_not_hex(&buf, len);
            let mut c_out = [0u8; 9];
            let mut c_bc: c_int = 0;
            let c = unsafe {
                pg_byteain(buf.as_ptr() as *const c_char, c_out.as_mut_ptr(), &mut c_bc)
            };
            kani::cover!(c == 1 && len > 0 && buf[0] != b'\\', "plain accept");
            kani::cover!(c == 1 && buf[0] == b'\\' && buf[1] == b'\\', "dbl-backslash accept");
            kani::cover!(
                c == 1 && buf[0] == b'\\' && (b'0'..=b'3').contains(&buf[1]),
                "octal accept"
            );
            kani::cover!(c == 0, "reject");
        }
    }

    ss_proof! {
        /// MUST FAIL (section-A rig non-vacuity control): C scans a
        /// one-byte-shorter cstring than the slice Rust sees — any input
        /// whose last byte changes the verdict/count is a counterexample
        /// (e.g. trailing '\\'). Run with the DEFAULT solver.
        #[kani::unwind(11)]
        fn control_byteain_len_skew() {
            let (mut buf, len) = sym_cstring::<9>();
            kani::assume(len >= 1);
            fence_not_hex(&buf, len);
            let full = buf;
            buf[len - 1] = 0; // C sees len-1 bytes
            let mut c_out = [0u8; 9];
            let mut c_bc: c_int = 0;
            let c = unsafe {
                pg_byteain(buf.as_ptr() as *const c_char, c_out.as_mut_ptr(), &mut c_bc)
            };
            kani::assume(c != -2);
            with_ctx(|mcx| {
                let r = varlena::bytea::byteain(mcx, &full[..len], None);
                match r {
                    Ok(Some(v)) => {
                        assert!(c == 1);
                        assert!(v.as_bytes().len() == 4 + c_bc as usize);
                        core::mem::forget(v);
                    }
                    Ok(None) => panic!("soft-error escape without an escontext"),
                    Err(e) => {
                        core::mem::forget(e);
                        assert!(c == 0);
                    }
                }
            });
        }
    }

    // =====================================================================
    // B. toast header accessors (rows 1269 / 2121 / 6316)
    // =====================================================================

    /// Symbolic candidate varlena image, 18 bytes (the ondisk toast-pointer
    /// size = 2 + 16; also covers 1B-short and 4B headers, whose accessors
    /// read at most 4 header bytes). Trusted-header fence (the C caller
    /// contract: a datum is one of the well-formed forms):
    ///   - external tag, if any, is ONDISK (indirect/expanded arms are out
    ///     of theorem — C side traps on them);
    ///   - 4B images are uncompressed or compressed per their 2 low bits
    ///     (no extra fence needed: accessors only decode header bits).
    fn sym_toast_image() -> [u8; 18] {
        let img: [u8; 18] = kani::any();
        // LE forms: 1B-external is b0 == 0x01; fence its tag to ONDISK (18)
        kani::assume(!(img[0] == 0x01) || img[1] == 18);
        img
    }

    fn fci1_with_mcx<'a>(
        mcx: Mcx<'a>,
        d: Datum,
    ) -> types_fmgr::LocalFcinfo<1> {
        let mut f = proof_support::fci([d]);
        // SAFETY: the harness context outlives the call and result reads.
        unsafe { f.set_result_mcx(mcx) };
        f
    }

    ss_proof! {
        /// row 1269 varlena plane (typlen -1): fc wrapper (datum unwrap +
        /// i32 pack in-theorem) vs C body over all fenced 18-byte images.
        fn eq_pg_column_size_varlena() {
            set_typlen(-1);
            let img = sym_toast_image();
            let c = unsafe { pg_column_size_c(img.as_ptr(), core::ptr::null()) };
            assert!((c as u64) != PG_TOAST_TRAP, "indirect/expanded trap reached");
            with_ctx(|mcx| {
                let mut flinfo = FmgrInfo::unresolved();
                let mut f = fci1_with_mcx(mcx, Datum::from_usize(img.as_ptr() as usize));
                let r = varlena::builtins::fc_pg_column_size(Some(&mut flinfo), &mut f);
                match r {
                    Ok(d) => assert!(c == d.as_i32(), "pg_column_size varlena mismatch"),
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("Rust errored on fenced varlena plane");
                    }
                }
            });
        }
    }

    ss_proof! {
        /// row 1269 cstring plane (typlen -2): strlen+1 both sides over
        /// symbolic NUL-terminated strings len<=8 (derived-length scan —
        /// expect the ladder; spot tier below it in the runqueue).
        #[kani::unwind(11)]
        fn eq_pg_column_size_cstring() {
            set_typlen(-2);
            let (buf, _len) = sym_cstring::<9>();
            let c = unsafe { pg_column_size_c(core::ptr::null(), buf.as_ptr() as *const c_char) };
            with_ctx(|mcx| {
                let mut flinfo = FmgrInfo::unresolved();
                let mut f = fci1_with_mcx(mcx, Datum::from_usize(buf.as_ptr() as usize));
                let r = varlena::builtins::fc_pg_column_size(Some(&mut flinfo), &mut f);
                match r {
                    Ok(d) => assert!(c == d.as_i32(), "pg_column_size cstring mismatch"),
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("Rust errored on cstring plane");
                    }
                }
            });
        }
    }

    ss_proof! {
        /// row 1269 fixed-width plane (symbolic typlen > 0): identity both
        /// sides through the seam.
        fn eq_pg_column_size_fixed() {
            let t: i16 = kani::any();
            kani::assume(t > 0);
            set_typlen(t);
            let img = [0u8; 18]; // untouched on this plane
            let c = unsafe { pg_column_size_c(img.as_ptr(), core::ptr::null()) };
            with_ctx(|mcx| {
                let mut flinfo = FmgrInfo::unresolved();
                let mut f = fci1_with_mcx(mcx, Datum::from_usize(img.as_ptr() as usize));
                let r = varlena::builtins::fc_pg_column_size(Some(&mut flinfo), &mut f);
                match r {
                    Ok(d) => assert!(c == d.as_i32(), "pg_column_size fixed mismatch"),
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("Rust errored on fixed plane");
                    }
                }
            });
        }
    }

    fn body_pg_column_compression(typlen: i16) {
        set_typlen(typlen);
        let img = sym_toast_image();
        let mut c_name = [0u8; 8];
        let mut c_name_len: c_int = 0;
        let c = unsafe {
            pg_column_compression_c(
                img.as_ptr(),
                c_name.as_mut_ptr() as *mut c_char,
                &mut c_name_len,
            )
        };
        with_ctx(|mcx| {
            let mut flinfo = FmgrInfo::unresolved();
            let mut f = fci1_with_mcx(mcx, Datum::from_usize(img.as_ptr() as usize));
            let r = varlena::builtins::fc_pg_column_compression(Some(&mut flinfo), &mut f);
            match r {
                Ok(d) => {
                    if f.isnull {
                        assert!(c == 0, "C non-NULL where Rust NULL");
                    } else {
                        assert!(c == 1, "C NULL/error where Rust returned text");
                        // result is a fresh 4B-header text image; compare
                        // payload to C's name bytes (4-5 literal bytes)
                        let p = d.as_usize() as *const u8;
                        // SAFETY: by-ref result image produced above in the
                        // harness context (wave-6: read-back is fine).
                        let img_r = unsafe {
                            core::slice::from_raw_parts(p, 4 + c_name_len as usize)
                        };
                        let word =
                            u32::from_ne_bytes([img_r[0], img_r[1], img_r[2], img_r[3]]);
                        assert!(
                            (word >> 2) as usize == 4 + c_name_len as usize,
                            "result text length mismatch"
                        );
                        let mut i = 0;
                        while i < c_name_len as usize {
                            assert!(img_r[4 + i] == c_name[i], "compression name mismatch");
                            i += 1;
                        }
                    }
                }
                Err(e) => {
                    core::mem::forget(e);
                    assert!(c == -1, "Rust errored where C did not (invalid cmid plane)");
                }
            }
        });
    }

    ss_proof! {
        /// row 2121 varlena plane: NULL / "pglz" / "lz4" / invalid-id
        /// which-verdict + result-text parity over all fenced images.
        #[kani::unwind(7)]
        fn eq_pg_column_compression_varlena() { body_pg_column_compression(-1) }
    }

    ss_proof! {
        /// row 2121 non-varlena plane: NULL both sides (typlen 4).
        fn eq_pg_column_compression_nonvarlena() { body_pg_column_compression(4) }
    }

    ss_proof! {
        /// row 6316: NULL-vs-va_valueid parity over all fenced images
        /// (typlen -1) — pure header decode + 16-byte toast-pointer copy.
        fn eq_pg_column_toast_chunk_id() {
            set_typlen(-1);
            let img = sym_toast_image();
            let mut c_valueid: u32 = 0;
            let c = unsafe { pg_column_toast_chunk_id_c(img.as_ptr(), &mut c_valueid) };
            with_ctx(|mcx| {
                let mut flinfo = FmgrInfo::unresolved();
                let mut f = fci1_with_mcx(mcx, Datum::from_usize(img.as_ptr() as usize));
                let r =
                    varlena::builtins::fc_pg_column_toast_chunk_id(Some(&mut flinfo), &mut f);
                match r {
                    Ok(d) => {
                        if f.isnull {
                            assert!(c == 0, "C valueid where Rust NULL");
                        } else {
                            assert!(c == 1, "C NULL where Rust valueid");
                            assert!(d.as_u32() == c_valueid, "va_valueid mismatch");
                        }
                    }
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("Rust errored on chunk_id plane");
                    }
                }
            });
        }
    }

    ss_proof! {
        /// Regime coverage for the toast rig (vacuity insurance): ondisk,
        /// inline-compressed, short, and plain-4B forms all reachable under
        /// the sym_toast_image fence; pglz/lz4/invalid ids reachable.
        fn cover_toast_regimes() {
            let img = sym_toast_image();
            let sz = unsafe { pg_toast_datum_size(img.as_ptr()) };
            let cm = unsafe { pg_toast_get_compression_id(img.as_ptr()) };
            kani::cover!(img[0] == 0x01 && sz != PG_TOAST_TRAP, "ondisk form");
            kani::cover!(img[0] & 0x03 == 0x02, "inline compressed form");
            kani::cover!(img[0] & 0x01 == 0x01 && img[0] != 0x01, "short form");
            kani::cover!(img[0] & 0x03 == 0x00, "plain 4B form");
            kani::cover!(cm == 0, "pglz id");
            kani::cover!(cm == 1, "lz4 id");
            kani::cover!(cm == 2, "invalid id");
        }
    }

    ss_proof! {
        /// MUST FAIL (section-B rig non-vacuity control): the typlen seam is
        /// skewed (C -1 varlena, Rust 4 fixed) — any image whose
        /// toast_datum_size != 4 is a counterexample. DEFAULT solver.
        /// CONCRETE image (2026-07-29): the original sym_toast_image()
        /// plane walled >350s before emitting a counterexample on the
        /// fleet AND locally (FAILED-without-playback = wall, encnames
        /// law) — one literal 3-byte short varlena (C sees 3, Rust fixed
        /// typlen sees 4) witnesses the seam in milliseconds; symbolic
        /// width buys nothing for a non-vacuity control.
        fn control_toast_typlen_skew() {
            unsafe { R_TYPLEN = 4 };
            unsafe { pg_set_typlen(-1) };
            // 1B short varlena, total size 3: header (3<<1)|1, 2 payload bytes.
            let img: [u8; 8] = [7, 0x41, 0x42, 0, 0, 0, 0, 0];
            let c = unsafe { pg_column_size_c(img.as_ptr(), core::ptr::null()) };
            kani::assume((c as u64) != PG_TOAST_TRAP);
            (|mcx: Mcx<'_>| {
                let mut flinfo = FmgrInfo::unresolved();
                let mut f = fci1_with_mcx(mcx, Datum::from_usize(img.as_ptr() as usize));
                let r = varlena::builtins::fc_pg_column_size(Some(&mut flinfo), &mut f);
                match r {
                    Ok(d) => assert!(c == d.as_i32()),
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("unexpected error");
                    }
                }
            })(dummy_mcx());
        }
    }

    // =====================================================================
    // C. btvarstrequalimage (row 5050)
    // =====================================================================

    ss_proof! {
        /// row 5050: verdict + 42P22-error-class parity, fully symbolic
        /// collid x fully symbolic deterministic seam value (fenced true on
        /// C/POSIX, C's builtin locales). Core-level: the fc wrapper adds
        /// only the fncollation unwrap.
        fn eq_btvarstrequalimage() {
            let collid: Oid = kani::any();
            let det: bool = kani::any();
            arm_det_seam(collid, det);
            let c = unsafe { pg_btvarstrequalimage(collid) };
            let r = varlena::btvarstrequalimage(collid);
            match r {
                Ok(b) => {
                    assert!(c == b as c_int, "btvarstrequalimage verdict mismatch");
                }
                Err(e) => {
                    core::mem::forget(e);
                    assert!(c == -1, "error-class mismatch (42P22 arm)");
                }
            }
            kani::cover!(collid == 0, "invalid-collid arm reachable");
            kani::cover!(c == 0, "nondeterministic arm reachable");
            kani::cover!(c == 1, "deterministic arm reachable");
        }
    }

    ss_proof! {
        /// MUST FAIL (section-C control): the deterministic seam is skewed
        /// (C sees !det) — proves the seam value is load-bearing. DEFAULT
        /// solver.
        fn control_btvarstrequalimage_det_skew() {
            let collid: Oid = kani::any();
            kani::assume(collid != 0);
            kani::assume(collid != C_COLLATION_OID && collid != POSIX_COLLATION_OID);
            let det: bool = kani::any();
            unsafe { R_DET = det };
            unsafe { pg_set_locale_det(!det as c_int) };
            pg_locale_seams::collation_is_deterministic::set(seam_collation_is_deterministic);
            let c = unsafe { pg_btvarstrequalimage(collid) };
            let r = varlena::btvarstrequalimage(collid);
            match r {
                Ok(b) => assert!(c == b as c_int),
                Err(e) => {
                    core::mem::forget(e);
                    assert!(c == -1);
                }
            }
        }
    }
}
