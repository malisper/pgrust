//! Kani C≡Rust equivalence: the oracle_compat string family + the
//! varchar/bpchar truncation/length family (WAVE 14, ledger rows):
//!
//!   oracle_compat.c side (c/pg_oracle_compat.c):
//!     btrim (884), ltrim (875), rtrim (876), btrim1 (885), ltrim1 (881),
//!     rtrim1 (401/882), byteatrim (2015), bytealtrim (6195),
//!     byteartrim (6196), lpad (873), rpad (874), translate (878),
//!     repeat (1622), text_reverse (3062).
//!   varchar.c side (c/pg_varchar.c):
//!     name_bpchar (408), bpchar_name (409), bpchar (668), varchar (669),
//!     char_bpchar (860), bpcharin (1044, clip core), varcharin (1046,
//!     clip core), bpcharlen (1318/1367/1372), bpcharoctetlen (1375),
//!     bpchartypmodin (2913, check core) / varchartypmodin (2915, check
//!     core), bpchartypmodout (2914) / varchartypmodout (2916).
//!
//! SKIPPED (not in this crate, with reasons):
//!   - ascii (1620) / chr (1621): ledger rows in-progress, owned by agent
//!     strings-misc (2026-07-28w5) — not double-harnessed here.
//!   - lower (870) / upper (871) / initcap (872) / casefold (6412): the
//!     case kernels route through formatting.c/pg_locale collation
//!     dispatch — a separate vendor+seam lane, out of this crate's scope.
//!   - bpcharsend (2431) / varcharsend (2433): pure textsend delegations —
//!     free siblings of varlena row 2415 (textsend) once its send-image
//!     rig lands; harnessing them here would duplicate that rig.
//!   - bpcharrecv (2430) / varcharrecv (2432): recv ABI pointer-datum wall
//!     (TRIAGE); their clip half IS covered here via rows 1044/1046.
//!
//! CLAIM-BEFORE-CODE CLASSES (the wave-14 law, mirror into the ledger):
//!   - SCALAR-VERDICT rows -> full symbolic harnesses: trim family via the
//!     surviving-WINDOW verdict (start offset, length — the ledger's
//!     "SCALAR SLICE" reading of dotrim), bpcharlen/bpcharoctetlen,
//!     bpchar_clip/varchar_clip decisions, bpchar/varchar coercion
//!     decisions + identity planes, anychar_typmodin, char_bpchar,
//!     bpchar_name (fixed 64-byte result block), name_bpchar.
//!   - EXPLICIT-LENGTH IMAGE rows (output length is an argument) -> full
//!     image harnesses under mcx-stubs: lpad/rpad LATIN1 arm (output =
//!     `len` chars = `len` bytes), repeat small-count arm, bpchar coercion
//!     pad arm (bounded typmod), anychar_typmodout (16-byte bounded,
//!     digit-emission band per the intout law).
//!   - DERIVED-LENGTH IMAGE rows (output length depends on content) ->
//!     concrete SPOT harnesses + wall note: translate (spots only —
//!     expected wall class per the RESULT-IMAGE + DERIVED-LENGTH laws),
//!     trim images (spots through the shipped fc_* wrappers; the window
//!     harnesses carry the logic claim), text_reverse UTF8 arm (spot;
//!     LATIN1 symbolic image harness written, expected ladder),
//!     lpad/rpad UTF8 arm (spots).
//!
//! Rust side (shipped code, path-deps — never copied): shipped cores
//! (`adt_oracle_compat::{dotrim_slice, dobyteatrim, lpad, rpad, translate,
//! repeat, text_reverse}`, `adt_varchar::{bpchar_clip, varchar_clip,
//! bpchar_name, anychar_typmodin, anychar_typmodout}`) and SHIPPED FMGR
//! WRAPPERS (`fc_btrim/fc_ltrim1/../fc_byteatrim`, `fc_bpchar`,
//! `fc_varchar`, `fc_char_bpchar`, `fc_bpchar_name`, `fc_name_bpchar`,
//! `fc_bpcharlen`, `fc_bpcharoctetlen`) invoked through real
//! `LocalFcinfo<N>` frames with an armed result mcx (text-slice pattern).
//! Args are inline 1-byte-header varlena images built with the shipped
//! `types_tuple::varatt::set_varsize_short`.
//!
//! C side: c/pg_oracle_compat.c + c/pg_varchar.c — verbatim REL_18_STABLE
//! bodies (provenance + every shim documented there). Harnesses link ONE
//! C file each (mbconv law: never whole-family --c-lib).
//!
//! Fences and claims (mirror into the ledger):
//!  - ENCODING FENCE: mb-sensitive harnesses are pinned per-encoding over
//!    {PG_UTF8, PG_LATIN1} (text-slice precedent); other encodings out of
//!    scope. Encoding-insensitive harnesses (bytea trims, repeat,
//!    char_bpchar, typmodin/out, octetlen) run once.
//!  - COLLATION: no function here reads collation (the case family that
//!    does is skipped); frames pass C_COLLATION_OID.
//!  - DETOASTING out of scope: inputs are inline images (post-PG_GETARG
//!    caller contract).
//!  - lpad/rpad UTF8 s1 DATATYPE-INVARIANT FENCE: C copies string1 chars
//!    with pg_mblen_unbounded (no range check) and reads past a truncated
//!    final char where Rust clamps to the payload (`end.min(len)`) — text
//!    values violating encoding validity cannot be constructed through any
//!    input path (text-slice precedent), so UTF8 pad harnesses fence s1 to
//!    whole legal UTF8 chars. s2 needs no fence (both sides range-check it
//!    via pg_mblen_range, error parity in-theorem).
//!  - name_bpchar NAME-INVARIANT FENCE: NameData is NUL-terminated within
//!    NAMEDATALEN (name[63] == 0 assumed); C's NameStr strlen walk is
//!    undefined without it, Rust's position() is total.
//!  - Error parity: value-space + verdict + sqlstate class (PgError::error
//!    and format machinery stubbed via proof_support — message text and
//!    Location leave the proof; shipped .with_sqlstate calls stay
//!    load-bearing). The bpchar coercion alloc-guard plane (C palloc's
//!    "invalid memory alloc request size") is VERDICT-ONLY (class 5).
//!  - Allocation: "modulo static-buffer allocator model" — mcx allocate/
//!    grow/deallocate + vec_with_capacity_in/vec_append_bytes stubbed to
//!    the proof heap (tiny-proof-heap ON per the derived-length law); all
//!    bytes written through the allocation stay in the theorem.
//!  - typmodin rows 2913/2915 are covered for the CHECK CORE only (the
//!    integer-list checks); ArrayGetIntegerTypmods (cstring[] walk +
//!    pg_strtoint32) stays out of theorem — do not let the ledger rows
//!    overstate. typmodout compares through REL_18's own pg_ltoa in place
//!    of snprintf("(%d)") (C-file shim 8 — the one formatting-mechanism
//!    substitution in this family).
//!
//! Negative controls (DEFAULT solver; MUST FAIL, decodable counterexample):
//!  - control_dobyteatrim_short_set (oracle C rig): C sees a one-shorter
//!    trim set.
//!  - control_varchar_clip_skew (varchar C rig): C sees atttypmod+1.
//!
//! Runqueue: ./runqueue.txt (per-harness c-lib, solver, timeout tier,
//! expected class). ZERO solves have been run — this lane is PRE-BUILD
//! (compile gate only: cargo kani --only-codegen -Z c-ffi -Z stubbing).

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use mcx::{Mcx, MemoryContext, PgVec};
    // Load-bearing for #[kani::stub] path resolution (stub paths resolve
    // relative to the harness module — prove-target gotcha).
    #[allow(unused_imports)]
    use proof_support;
    use std::os::raw::c_int;
    use types_core::C_COLLATION_OID;
    use types_error::{PgError, PgResult};
    use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};
    use types_tuple::varatt;
    use wchar::{PG_LATIN1, PG_UTF8};

    extern "C" {
        // ---- c/pg_oracle_compat.c ----
        fn pgoc_set_db_encoding(enc: c_int) -> c_int;
        fn pgoc_take_err() -> c_int;
        fn pgoc_lpad(
            s1: *const u8,
            s1len: c_int,
            len: c_int,
            s2: *const u8,
            s2len: c_int,
            out: *mut u8,
        ) -> c_int;
        fn pgoc_rpad(
            s1: *const u8,
            s1len: c_int,
            len: c_int,
            s2: *const u8,
            s2len: c_int,
            out: *mut u8,
        ) -> c_int;
        fn pgoc_dotrim(
            s: *const u8,
            slen: c_int,
            set: *const u8,
            setlen: c_int,
            doltrim: c_int,
            dortrim: c_int,
            out_start: *mut c_int,
            out: *mut u8,
        ) -> c_int;
        fn pgoc_dobyteatrim(
            s: *const u8,
            slen: c_int,
            set: *const u8,
            setlen: c_int,
            doltrim: c_int,
            dortrim: c_int,
            out_start: *mut c_int,
            out: *mut u8,
        ) -> c_int;
        fn pgoc_translate(
            s: *const u8,
            slen: c_int,
            from: *const u8,
            fromlen: c_int,
            to: *const u8,
            tolen: c_int,
            out: *mut u8,
        ) -> c_int;
        fn pgoc_repeat(s: *const u8, slen: c_int, count: c_int, out: *mut u8) -> c_int;
        fn pgoc_text_reverse(s: *const u8, len: c_int, out: *mut u8) -> c_int;

        // ---- c/pg_varchar.c ----
        fn pgvc_set_db_encoding(enc: c_int) -> c_int;
        fn pgvc_take_err() -> c_int;
        fn pgvc_anychar_typmodin(tl: *const c_int, n: c_int, out_typmod: *mut c_int) -> c_int;
        fn pgvc_anychar_typmodout(typmod: c_int, res: *mut u8) -> c_int;
        fn pgvc_bpchar_input(
            s: *const u8,
            len: usize,
            atttypmod: c_int,
            out_copy: *mut usize,
            out_total: *mut usize,
        ) -> c_int;
        fn pgvc_varchar_input(
            s: *const u8,
            len: usize,
            atttypmod: c_int,
            out_len: *mut usize,
        ) -> c_int;
        fn pgvc_bpchar(
            s: *const u8,
            len: c_int,
            maxlen: c_int,
            is_explicit: c_int,
            ret_source: *mut c_int,
            out: *mut u8,
            outcap: c_int,
        ) -> c_int;
        fn pgvc_varchar(
            s: *const u8,
            len: c_int,
            typmod: c_int,
            is_explicit: c_int,
            ret_source: *mut c_int,
            out: *mut u8,
        ) -> c_int;
        fn pgvc_char_bpchar(c: i8, out: *mut u8) -> c_int;
        fn pgvc_bpchar_name(s: *const u8, len: c_int, out: *mut u8) -> c_int;
        fn pgvc_name_bpchar(name: *const u8, out: *mut u8) -> c_int;
        fn pgvc_bpcharlen(s: *const u8, len: c_int) -> c_int;
        fn pgvc_bpcharoctetlen(len: c_int) -> c_int;
    }

    const CAP: usize = 8;
    const SETCAP: usize = 4;
    const COLL_C: u32 = C_COLLATION_OID;
    /// C-side ereport sentinel (shim 4 in both C files).
    const PG_CERR: c_int = -2_100_000_000;
    const NAMEDATALEN: usize = 64;

    // C errflag classes (shim 4) <-> Rust sqlstates. Oracle rig: 1 = 54000
    // (program limit), 4 = 22021 (bad byte sequence). Varchar rig: 2 = 22023
    // (invalid parameter), 3 = 22001 (right truncation), 4 = 22021,
    // 5 = alloc-guard (VERDICT-ONLY: not mapped, spot harness matches the
    // flag directly).
    fn err_class(e: &PgError) -> c_int {
        use types_error::*;
        if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
            1
        } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
            2
        } else if e.sqlstate == ERRCODE_STRING_DATA_RIGHT_TRUNCATION {
            3
        } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
            4
        } else {
            -1
        }
    }

    /// Pin the encoding state on both sides (oracle rig).
    fn install_oc(enc: i32) {
        mbutils::SetDatabaseEncoding(enc).expect("valid backend encoding");
        let _ = unsafe { pgoc_set_db_encoding(enc) };
    }

    /// Pin the encoding state on both sides (varchar rig).
    fn install_vc(enc: i32) {
        mbutils::SetDatabaseEncoding(enc).expect("valid backend encoding");
        let _ = unsafe { pgvc_set_db_encoding(enc) };
    }

    /// One inline 1-byte-header varlena image over symbolic payload bytes +
    /// symbolic length <= N, built with the SHIPPED header encoder
    /// (text-cmp/text-slice pattern).
    struct VarImg<const N: usize> {
        img: [u8; 32], // N + VARHDRSZ_SHORT <= 32 for every cap used here
        len: usize,
    }

    fn sym_varlena<const N: usize>() -> VarImg<N> {
        const { assert!(N + varatt::VARHDRSZ_SHORT <= 32) };
        let payload: [u8; N] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= N);
        let mut img = [0u8; 32];
        // SAFETY: img is writable and len + 1 <= VARATT_SHORT_MAX.
        unsafe { varatt::set_varsize_short(img.as_mut_ptr(), len + varatt::VARHDRSZ_SHORT) };
        let mut i = 0;
        while i < N {
            img[i + varatt::VARHDRSZ_SHORT] = payload[i];
            i += 1;
        }
        VarImg { img, len }
    }

    /// Concrete-payload image (spot harnesses).
    fn concrete_varlena<const N: usize>(bytes: &[u8]) -> VarImg<N> {
        assert!(bytes.len() <= N);
        let mut img = [0u8; 32];
        unsafe {
            varatt::set_varsize_short(img.as_mut_ptr(), bytes.len() + varatt::VARHDRSZ_SHORT)
        };
        let mut i = 0;
        while i < bytes.len() {
            img[i + varatt::VARHDRSZ_SHORT] = bytes[i];
            i += 1;
        }
        VarImg {
            img,
            len: bytes.len(),
        }
    }

    impl<const N: usize> VarImg<N> {
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

    /// DATATYPE-INVARIANT FENCE (UTF8 pads, s1 only): payload is a whole
    /// number of legal UTF-8 characters (shipped wchar kernels as the
    /// predicate — themselves proven C-equivalent in proofs/utf8).
    /// Currently unused: the UTF8 pad harnesses are concrete spots (claim
    /// law), so the fence is enforced by construction; kept for the
    /// follow-up symbolic UTF8 pad attempt the module doc names.
    #[allow(dead_code)]
    fn assume_valid_utf8(data: &[u8]) {
        let mut i = 0usize;
        while i < data.len() {
            let l = wchar::pg_utf_mblen(&data[i..]) as usize;
            kani::assume(i + l <= data.len());
            kani::assume(wchar::pg_utf8_islegal(&data[i..], l as i32));
            i += l;
        }
    }

    /// The shipped fc_* wrapper shape.
    type FcFn = fn(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> PgResult<Datum>;

    /// Run a shipped fc_* wrapper on a real N-arg frame with an armed
    /// result mcx (the new-by-ref result convention).
    fn call<const N: usize>(
        fc: FcFn,
        args: [Datum; N],
        collid: u32,
        mcx: Mcx<'_>,
    ) -> PgResult<Datum> {
        let mut f = LocalFcinfo::<N>::new(collid);
        for (slot, d) in f.args.iter_mut().zip(args) {
            *slot = NullableDatum::value(d);
        }
        // SAFETY: the context outlives the call (harness stack frame).
        unsafe { f.set_result_mcx(mcx) };
        fc(None, &mut f)
    }

    /// [`call`] without an armed result mcx, for scalar-result wrappers
    /// (result_mcx() unreachable for inline images; would panic loudly).
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

    /// Scalar value + error verdict + sqlstate-class parity, one property
    /// (external kissat re-solves per property). take_err: the rig's
    /// pgoc_take_err / pgvc_take_err.
    fn check_i32(r: PgResult<Datum>, c: c_int, cerr: c_int) {
        let ok = match r {
            Ok(d) => c != PG_CERR && cerr == 0 && d.as_i32() == c,
            Err(e) => {
                let cls = err_class(&e);
                core::mem::forget(e); // Box<PgError> drop glue walls (TRIAGE)
                c == PG_CERR && cls == cerr
            }
        };
        assert!(ok, "C/Rust divergence (value or error verdict/class)");
    }

    /// C (out buffer + returned length) == Rust (result image payload), or
    /// matching error verdict + class. Single property.
    fn check_bytes(r: PgResult<Datum>, c: c_int, out: &[u8], cerr: c_int) {
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
            Err(e) => {
                let cls = err_class(&e);
                core::mem::forget(e);
                c == PG_CERR && cls == cerr
            }
        };
        assert!(ok, "C/Rust divergence (bytes or error verdict/class)");
    }

    /// Surviving-window parity for the trim cores: Rust subslice vs the C
    /// (start, len) out-params, or matching error class.
    fn check_window(
        r: PgResult<&[u8]>,
        base: *const u8,
        c_len: c_int,
        c_start: c_int,
        cerr: c_int,
    ) {
        let ok = match r {
            Ok(w) => {
                let off = (w.as_ptr() as usize).wrapping_sub(base as usize);
                c_len != PG_CERR && cerr == 0 && off == c_start as usize && w.len() == c_len as usize
            }
            Err(e) => {
                let cls = err_class(&e);
                core::mem::forget(e);
                c_len == PG_CERR && cls == cerr
            }
        };
        assert!(ok, "C/Rust divergence (trim window or error verdict/class)");
    }

    /// Stub for `mcx::vec_append_bytes` under proof: identical semantics for
    /// within-capacity appends (every call site under proof reserves its
    /// final size first); a capacity overrun fails the proof loudly instead
    /// of re-entering the real allocator's grow path (text-slice pattern).
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

    /// Stub for `mbutils::byte_sequence` (invalid-byte-sequence MESSAGE
    /// detail: symbolic-capacity String build walls symex). Message text
    /// leaves the proof; error VALUE/verdict/sqlstate stay in.
    #[allow(dead_code)] // referenced only from #[kani::stub] attributes
    pub fn stub_byte_sequence(_mbstr: &[u8], _mblen: i32, _len: i32) -> String {
        String::new()
    }

    /// Stub for `mcx::local_pool_on` (OnceLock/std-Once machinery walls
    /// symex): `false` selects the global recycling arm — pool SELECTION is
    /// allocation strategy, out of every equivalence claim.
    #[allow(dead_code)] // referenced only from #[kani::stub] attributes
    pub fn stub_local_pool_on() -> bool {
        false
    }

    // Common stub set: allocation -> proof heap; error message plumbing out
    // of the proof (value/verdict/sqlstate stay in).
    macro_rules! oc_proof {
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
            #[kani::stub(mbutils::byte_sequence, stub_byte_sequence)]
            fn $name() $body
        };
    }

    /// Harness-scoped bump context; forgotten at the end of every harness
    /// that arms a result mcx (teardown is not part of any claim and walls
    /// symex — mcx-stubs recipe).
    fn with_ctx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
        let ctx = MemoryContext::new_bump("proof");
        let r = f(ctx.mcx());
        core::mem::forget(ctx);
        r
    }

    // =====================================================================
    // A. trim family — WINDOW verdict (scalar-verdict full harnesses)
    //    rows 875/876/884 (2-arg text trims), 401/881/882/885 (1-arg forms),
    //    2015/6195/6196 (bytea trims)
    //    c-lib: c/pg_oracle_compat.c
    // =====================================================================

    fn body_dotrim_window(enc: i32) {
        install_oc(enc);
        let s = sym_varlena::<CAP>();
        let set = sym_varlena::<SETCAP>();
        let dol: bool = kani::any();
        let dor: bool = kani::any();
        let mut c_start: c_int = 0;
        let mut c_out = [0u8; CAP];
        let c_len = unsafe {
            pgoc_dotrim(
                s.data(),
                s.clen(),
                set.data(),
                set.clen(),
                dol as c_int,
                dor as c_int,
                &mut c_start,
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let ctx = MemoryContext::new_bump("proof");
        let res = adt_oracle_compat::dotrim_slice(ctx.mcx(), s.payload(), set.payload(), dol, dor);
        check_window(res, s.payload().as_ptr(), c_len, c_start, cerr);
        core::mem::forget(ctx);
    }

    oc_proof! {
        /// rows 875/876/884 single-byte arm; symbolic trim flags cover
        /// ltrim/rtrim/btrim in one theorem. Expected class: fast.
        #[kani::unwind(11)]
        fn eq_dotrim_window_latin1() { body_dotrim_window(PG_LATIN1); }
    }

    oc_proof! {
        /// rows 875/876/884 mb arm (stringchars/setchars build + trim walks
        /// + mblen_range error plane in-theorem). Expected class: ladder.
        #[kani::unwind(11)]
        fn eq_dotrim_window_utf8() { body_dotrim_window(PG_UTF8); }
    }

    fn body_dotrim1_window(enc: i32, dol: bool, dor: bool) {
        install_oc(enc);
        let s = sym_varlena::<CAP>();
        let mut c_start: c_int = 0;
        let mut c_out = [0u8; CAP];
        let c_len = unsafe {
            pgoc_dotrim(
                s.data(),
                s.clen(),
                b" ".as_ptr(),
                1,
                dol as c_int,
                dor as c_int,
                &mut c_start,
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let ctx = MemoryContext::new_bump("proof");
        let res = adt_oracle_compat::dotrim_slice(ctx.mcx(), s.payload(), b" ", dol, dor);
        check_window(res, s.payload().as_ptr(), c_len, c_start, cerr);
        core::mem::forget(ctx);
    }

    oc_proof! {
        /// rows 885/881/401/882 (one-arg trims, set = " "): both trim-flag
        /// combos symbolic is wasteful here — btrim1 (true,true) subsumes
        /// the shared window logic; ltrim1/rtrim1 spot-verified through the
        /// shipped wrappers below. Symbolic over BOTH encodings (space is a
        /// singleton unit in each). Expected class: fast.
        #[kani::unwind(11)]
        fn eq_dotrim1_window() {
            let enc = if kani::any() { PG_UTF8 } else { PG_LATIN1 };
            body_dotrim1_window(enc, true, true);
        }
    }

    oc_proof! {
        /// rows 2015/6195/6196: dobyteatrim window, symbolic flags — pure
        /// slice cores both sides, encoding-independent. Expected: fast.
        #[kani::unwind(11)]
        fn eq_dobyteatrim_window() {
            let s = sym_varlena::<CAP>();
            let set = sym_varlena::<SETCAP>();
            let dol: bool = kani::any();
            let dor: bool = kani::any();
            let mut c_start: c_int = 0;
            let mut c_out = [0u8; CAP];
            let c_len = unsafe {
                pgoc_dobyteatrim(
                    s.data(), s.clen(),
                    set.data(), set.clen(),
                    dol as c_int, dor as c_int,
                    &mut c_start, c_out.as_mut_ptr(),
                )
            };
            let cerr = unsafe { pgoc_take_err() };
            let w = adt_oracle_compat::dobyteatrim(s.payload(), set.payload(), dol, dor);
            let off = (w.as_ptr() as usize).wrapping_sub(s.payload().as_ptr() as usize);
            assert!(
                cerr == 0
                    && c_len != PG_CERR
                    && off == c_start as usize
                    && w.len() == c_len as usize,
                "C/Rust divergence (byteatrim window)"
            );
        }
    }

    // ---- trim IMAGE spots through the shipped fc wrappers (derived-length
    // image class: concrete spots; window harnesses carry the logic) ----

    fn spot_trim2(fc: FcFn, s_bytes: &[u8], set_bytes: &[u8], dol: bool, dor: bool) {
        let s = concrete_varlena::<CAP>(s_bytes);
        let set = concrete_varlena::<SETCAP>(set_bytes);
        let mut c_start: c_int = 0;
        let mut c_out = [0u8; CAP];
        let c_len = unsafe {
            pgoc_dotrim(
                s.data(),
                s.clen(),
                set.data(),
                set.clen(),
                dol as c_int,
                dor as c_int,
                &mut c_start,
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let r = with_ctx(|mcx| call(fc, [s.datum(), set.datum()], COLL_C, mcx));
        check_bytes(r, c_len, &c_out, cerr);
    }

    fn spot_trim1(fc: FcFn, s_bytes: &[u8], dol: bool, dor: bool) {
        let s = concrete_varlena::<CAP>(s_bytes);
        let mut c_start: c_int = 0;
        let mut c_out = [0u8; CAP];
        let c_len = unsafe {
            pgoc_dotrim(
                s.data(),
                s.clen(),
                b" ".as_ptr(),
                1,
                dol as c_int,
                dor as c_int,
                &mut c_start,
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let r = with_ctx(|mcx| call(fc, [s.datum()], COLL_C, mcx));
        check_bytes(r, c_len, &c_out, cerr);
    }

    oc_proof! {
        /// rows 884/875/876 wrapper-image spots (LATIN1 concrete). fast.
        #[kani::unwind(11)]
        fn spot_fc_trims_text() {
            install_oc(PG_LATIN1);
            spot_trim2(adt_oracle_compat::builtins::fc_btrim, b"xxabxx", b"x", true, true);
            spot_trim2(adt_oracle_compat::builtins::fc_ltrim, b"xxab", b"xz", true, false);
            spot_trim2(adt_oracle_compat::builtins::fc_rtrim, b"abzz", b"z", false, true);
        }
    }

    oc_proof! {
        /// rows 885/881/401/882 wrapper-image spots (one-arg forms). fast.
        #[kani::unwind(11)]
        fn spot_fc_trims1() {
            install_oc(PG_LATIN1);
            spot_trim1(adt_oracle_compat::builtins::fc_btrim1, b"  ab  ", true, true);
            spot_trim1(adt_oracle_compat::builtins::fc_ltrim1, b"  ab", true, false);
            spot_trim1(adt_oracle_compat::builtins::fc_rtrim1, b"ab  ", false, true);
        }
    }

    oc_proof! {
        /// row 884 mb arm wrapper-image spot (2-byte UTF8 char in the set).
        #[kani::unwind(11)]
        fn spot_fc_btrim_utf8() {
            install_oc(PG_UTF8);
            // s = "é a é" (0xC3 0xA9 = é), set = "é " — trims to "a".
            spot_trim2(
                adt_oracle_compat::builtins::fc_btrim,
                &[0xC3, 0xA9, b' ', b'a', 0xC3, 0xA9],
                &[0xC3, 0xA9, b' '],
                true,
                true,
            );
        }
    }

    fn spot_byteatrim(fc: FcFn, s_bytes: &[u8], set_bytes: &[u8], dol: bool, dor: bool) {
        let s = concrete_varlena::<CAP>(s_bytes);
        let set = concrete_varlena::<SETCAP>(set_bytes);
        let mut c_start: c_int = 0;
        let mut c_out = [0u8; CAP];
        let c_len = unsafe {
            pgoc_dobyteatrim(
                s.data(),
                s.clen(),
                set.data(),
                set.clen(),
                dol as c_int,
                dor as c_int,
                &mut c_start,
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let r = with_ctx(|mcx| call(fc, [s.datum(), set.datum()], COLL_C, mcx));
        check_bytes(r, c_len, &c_out, cerr);
    }

    oc_proof! {
        /// rows 2015/6195/6196 wrapper-image spots (incl the empty-set
        /// zero-copy identity plane). fast.
        #[kani::unwind(11)]
        fn spot_fc_byteatrims() {
            spot_byteatrim(adt_oracle_compat::builtins::fc_byteatrim, &[0, 1, 2, 0], &[0], true, true);
            spot_byteatrim(adt_oracle_compat::builtins::fc_bytealtrim, &[9, 9, 7], &[9], true, false);
            spot_byteatrim(adt_oracle_compat::builtins::fc_byteartrim, &[7, 9, 9], &[9], false, true);
            // empty set: C returns the input untouched
            spot_byteatrim(adt_oracle_compat::builtins::fc_byteatrim, &[1, 2], &[], true, true);
        }
    }

    // =====================================================================
    // B. lpad / rpad — EXPLICIT-length image rows (873/874)
    // =====================================================================

    fn body_pad_latin1(left: bool) {
        install_oc(PG_LATIN1);
        let s1 = sym_varlena::<4>();
        let s2 = sym_varlena::<3>();
        let len: i32 = kani::any();
        kani::assume((-2..=6).contains(&len)); // negative plane in-theorem
        let mut c_out = [0u8; 16];
        let c = unsafe {
            (if left { pgoc_lpad } else { pgoc_rpad })(
                s1.data(),
                s1.clen(),
                len,
                s2.data(),
                s2.clen(),
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let fc = if left {
            adt_oracle_compat::builtins::fc_lpad
        } else {
            adt_oracle_compat::builtins::fc_rpad
        };
        let r = with_ctx(|mcx| {
            call(
                fc,
                [s1.datum(), Datum::from_i32(len), s2.datum()],
                COLL_C,
                mcx,
            )
        });
        check_bytes(r, c, &c_out, cerr);
    }

    oc_proof! {
        /// row 873 LATIN1 arm: output length == len argument (explicit) —
        /// the wave-14 interesting-win shape. Expected class: ladder.
        #[kani::unwind(9)]
        fn eq_lpad_latin1() { body_pad_latin1(true); }
    }

    oc_proof! {
        /// row 874 LATIN1 arm. Expected class: ladder.
        #[kani::unwind(9)]
        fn eq_rpad_latin1() { body_pad_latin1(false); }
    }

    fn body_pad_err_plane(left: bool) {
        install_oc(PG_UTF8);
        // Concrete short valid s1/s2; symbolic len over the 54000 error
        // region (UTF8 maxlen 4: 4*len + 4 overflows i32 or exceeds
        // MaxAllocSize for every len >= 0x1000_0000). Loops unreachable.
        let s1 = concrete_varlena::<4>(b"ab");
        let s2 = concrete_varlena::<3>(b"z");
        let len: i32 = kani::any();
        kani::assume(len >= 0x1000_0000);
        let mut c_out = [0u8; 4];
        let c = unsafe {
            (if left { pgoc_lpad } else { pgoc_rpad })(
                s1.data(),
                s1.clen(),
                len,
                s2.data(),
                s2.clen(),
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let fc = if left {
            adt_oracle_compat::builtins::fc_lpad
        } else {
            adt_oracle_compat::builtins::fc_rpad
        };
        let r = with_ctx(|mcx| {
            call(
                fc,
                [s1.datum(), Datum::from_i32(len), s2.datum()],
                COLL_C,
                mcx,
            )
        });
        kani::cover!(true, "pad error plane exercised");
        check_bytes(r, c, &c_out, cerr);
    }

    oc_proof! {
        /// row 873 requested-length-too-large plane (full symbolic error
        /// region, 54000 class parity). Expected class: fast.
        #[kani::unwind(5)]
        fn eq_lpad_err_plane() { body_pad_err_plane(true); }
    }

    oc_proof! {
        /// row 874 error plane. Expected class: fast.
        #[kani::unwind(5)]
        fn eq_rpad_err_plane() { body_pad_err_plane(false); }
    }

    fn spot_pad_utf8(left: bool, s1b: &[u8], len: i32, s2b: &[u8]) {
        install_oc(PG_UTF8);
        // UTF8 image arm is derived-offset (mb walk) -> spot proofs per the
        // claim law; s1 concrete/valid (see module-doc fence).
        let s1 = concrete_varlena::<4>(s1b);
        let s2 = concrete_varlena::<3>(s2b);
        let mut c_out = [0u8; 32];
        let c = unsafe {
            (if left { pgoc_lpad } else { pgoc_rpad })(
                s1.data(),
                s1.clen(),
                len,
                s2.data(),
                s2.clen(),
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let fc = if left {
            adt_oracle_compat::builtins::fc_lpad
        } else {
            adt_oracle_compat::builtins::fc_rpad
        };
        let r = with_ctx(|mcx| {
            call(
                fc,
                [s1.datum(), Datum::from_i32(len), s2.datum()],
                COLL_C,
                mcx,
            )
        });
        check_bytes(r, c, &c_out, cerr);
    }

    oc_proof! {
        /// rows 873/874 UTF8 mb-walk spots: 2-byte pad char with wraparound,
        /// truncation plane (len < s1 chars), empty-s2 plane. fast.
        #[kani::unwind(9)]
        fn spot_pads_utf8() {
            spot_pad_utf8(true, "ab".as_bytes(), 5, "\u{e9}".as_bytes());
            spot_pad_utf8(false, "\u{e9}a".as_bytes(), 5, "z\u{e9}".as_bytes());
            spot_pad_utf8(true, "abc".as_bytes(), 2, "z".as_bytes()); // truncate
            spot_pad_utf8(false, "ab".as_bytes(), 5, b""); // no pad chars
        }
    }

    // =====================================================================
    // C. translate (878) — DERIVED-length image row: concrete spots + wall
    //    note (symbolic form expected to wall per the RESULT-IMAGE /
    //    DERIVED-LENGTH laws; do not burn ladder time on it first).
    // =====================================================================

    fn spot_translate(s_b: &[u8], from_b: &[u8], to_b: &[u8]) {
        let s = concrete_varlena::<CAP>(s_b);
        let from = concrete_varlena::<SETCAP>(from_b);
        let to = concrete_varlena::<SETCAP>(to_b);
        let mut c_out = [0u8; 32];
        let c = unsafe {
            pgoc_translate(
                s.data(),
                s.clen(),
                from.data(),
                from.clen(),
                to.data(),
                to.clen(),
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgoc_take_err() };
        let r = with_ctx(|mcx| {
            call(
                adt_oracle_compat::builtins::fc_translate,
                [s.datum(), from.datum(), to.datum()],
                COLL_C,
                mcx,
            )
        });
        check_bytes(r, c, &c_out, cerr);
    }

    oc_proof! {
        /// row 878 spots (LATIN1): substitute / delete (from longer than
        /// to) / no-match / empty-string identity. fast.
        #[kani::unwind(11)]
        fn spot_translate_latin1() {
            install_oc(PG_LATIN1);
            spot_translate(b"abcabc", b"ab", b"xy"); // substitute
            spot_translate(b"abcabc", b"ab", b"x");  // delete 'b'
            spot_translate(b"abc", b"z", b"y");      // no match
            spot_translate(b"", b"a", b"b");         // identity plane
        }
    }

    oc_proof! {
        /// row 878 UTF8 spot: 2-byte source char substituted by 1-byte and
        /// deleted (mb from/to walks in-theorem). fast.
        #[kani::unwind(11)]
        fn spot_translate_utf8() {
            install_oc(PG_UTF8);
            spot_translate("a\u{e9}b".as_bytes(), "\u{e9}b".as_bytes(), "x".as_bytes());
        }
    }

    // =====================================================================
    // D. repeat (1622)
    // =====================================================================

    oc_proof! {
        /// row 1622 ok arm: count symbolic 0..=3 x payload cap 4 — output
        /// length = count*slen with both factors tiny (explicit-length
        /// class). Encoding-independent. Expected class: ladder.
        #[kani::unwind(6)]
        fn eq_repeat_small() {
            install_oc(PG_LATIN1);
            let s = sym_varlena::<4>();
            let count: i32 = kani::any();
            kani::assume((-1..=3).contains(&count)); // negative plane in-theorem
            let mut c_out = [0u8; 16];
            let c = unsafe { pgoc_repeat(s.data(), s.clen(), count, c_out.as_mut_ptr()) };
            let cerr = unsafe { pgoc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_oracle_compat::builtins::fc_repeat,
                    [s.datum(), Datum::from_i32(count)],
                    COLL_C,
                    mcx,
                )
            });
            check_bytes(r, c, &c_out, cerr);
        }
    }

    oc_proof! {
        /// row 1622 54000 plane: count symbolic over the full error region
        /// (slen = 4 concrete symbolic-byte payload; count*4 + 4 overflows
        /// or exceeds MaxAllocSize for every count >= 0x1000_0000; the
        /// copy loop is unreachable). NOTE the slen == 0, huge-count plane
        /// is EXCLUDED (both sides run a trivial 2^31-iteration loop —
        /// unbounded for the solver, value-identical). fast.
        /// unwind 6: concrete_varlena::<4>'s fill loop runs 4 iterations
        /// (fleet fail: unwinding assertion at unwind(4)).
        #[kani::unwind(6)]
        fn eq_repeat_err_plane() {
            install_oc(PG_LATIN1);
            let payload: [u8; 4] = kani::any();
            let s = concrete_varlena::<4>(&payload);
            let count: i32 = kani::any();
            kani::assume(count >= 0x1000_0000);
            let mut c_out = [0u8; 4];
            let c = unsafe { pgoc_repeat(s.data(), s.clen(), count, c_out.as_mut_ptr()) };
            let cerr = unsafe { pgoc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_oracle_compat::builtins::fc_repeat,
                    [s.datum(), Datum::from_i32(count)],
                    COLL_C,
                    mcx,
                )
            });
            kani::cover!(true, "repeat error plane exercised");
            check_bytes(r, c, &c_out, cerr);
        }
    }

    // =====================================================================
    // E. text_reverse (3062)
    // =====================================================================

    oc_proof! {
        /// row 3062 single-byte arm, full symbolic cap 8. RESULT-IMAGE
        /// class — expected ladder/wall (text-slice law); recorded as the
        /// attempt of record before falling back to the spot below.
        #[kani::unwind(11)]
        fn eq_text_reverse_latin1() {
            install_oc(PG_LATIN1);
            let s = sym_varlena::<CAP>();
            let mut c_out = [0u8; CAP];
            let c = unsafe { pgoc_text_reverse(s.data(), s.clen(), c_out.as_mut_ptr()) };
            let cerr = unsafe { pgoc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_oracle_compat::builtins::fc_text_reverse,
                    [s.datum()],
                    COLL_C,
                    mcx,
                )
            });
            check_bytes(r, c, &c_out, cerr);
        }
    }

    oc_proof! {
        /// row 3062 mb-arm spot: 2-byte char kept intact under reversal +
        /// a truncated-char error-plane spot (0xC3 tail missing -> 22021
        /// parity both sides). fast.
        #[kani::unwind(11)]
        fn spot_text_reverse_utf8() {
            install_oc(PG_UTF8);
            for bytes in [&b"a\xC3\xA9b"[..], &b"ab\xC3"[..]] {
                let s = concrete_varlena::<CAP>(bytes);
                let mut c_out = [0u8; CAP];
                let c = unsafe { pgoc_text_reverse(s.data(), s.clen(), c_out.as_mut_ptr()) };
                let cerr = unsafe { pgoc_take_err() };
                let r = with_ctx(|mcx| {
                    call(
                        adt_oracle_compat::builtins::fc_text_reverse,
                        [s.datum()],
                        COLL_C,
                        mcx,
                    )
                });
                check_bytes(r, c, &c_out, cerr);
            }
        }
    }

    // =====================================================================
    // F. varchar/bpchar family — c-lib: c/pg_varchar.c
    // =====================================================================

    fn body_bpcharlen(enc: i32) {
        install_vc(enc);
        let a = sym_varlena::<CAP>();
        let c = unsafe { pgvc_bpcharlen(a.data(), a.clen()) };
        let cerr = unsafe { pgvc_take_err() };
        let r = call_scalar(adt_varchar::builtins::fc_bpcharlen, [a.datum()], COLL_C);
        check_i32(r, c, cerr);
    }

    oc_proof! {
        /// rows 1318/1367/1372 single-byte arm (bpchartruelen trailing-blank
        /// scan in-theorem). fast.
        #[kani::unwind(10)]
        fn eq_bpcharlen_latin1() { body_bpcharlen(PG_LATIN1); }
    }

    oc_proof! {
        /// rows 1318/1367/1372 mb arm (truelen + pg_mbstrlen walk). fast.
        #[kani::unwind(10)]
        fn eq_bpcharlen_utf8() { body_bpcharlen(PG_UTF8); }
    }

    oc_proof! {
        /// row 1375: octet length (raw payload size; symbolic encoding is a
        /// free universality bonus — the body never dispatches on it). fast.
        #[kani::unwind(10)]
        fn eq_bpcharoctetlen() {
            let enc = if kani::any() { PG_UTF8 } else { PG_LATIN1 };
            install_vc(enc);
            let a = sym_varlena::<CAP>();
            let c = unsafe { pgvc_bpcharoctetlen(a.clen()) };
            let cerr = unsafe { pgvc_take_err() };
            let r = call_scalar(adt_varchar::builtins::fc_bpcharoctetlen, [a.datum()], COLL_C);
            check_i32(r, c, cerr);
        }
    }

    // ---- clip decision cores (rows 1044 / 1046; recv halves of 2430/2432) ----

    fn body_bpchar_clip(enc: i32) {
        install_vc(enc);
        let payload: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let s = &payload[..len];
        let atttypmod: i32 = kani::any(); // FULL i32: -1/invalid plane in-theorem
        let mut c_copy: usize = 0;
        let mut c_total: usize = 0;
        let c = unsafe { pgvc_bpchar_input(s.as_ptr(), len, atttypmod, &mut c_copy, &mut c_total) };
        let cerr = unsafe { pgvc_take_err() };
        let r = adt_varchar::bpchar_clip(s, atttypmod, None);
        let ok = match r {
            Ok(Some(clip)) => c == 0 && cerr == 0 && clip.copy == c_copy && clip.total == c_total,
            Ok(None) => false, // impossible without an escontext
            Err(e) => {
                let cls = err_class(&e);
                core::mem::forget(e);
                c == PG_CERR && cls == cerr
            }
        };
        assert!(ok, "C/Rust divergence (bpchar_clip decision)");
    }

    oc_proof! {
        /// row 1044 clip core, single-byte arm, FULL-i32 typmod (invalid
        /// typmod identity + truncation + 22001 planes in-theorem; image
        /// write out of theorem per the ledger row note). fast.
        #[kani::unwind(10)]
        fn eq_bpchar_clip_latin1() { body_bpchar_clip(PG_LATIN1); }
    }

    oc_proof! {
        /// row 1044 clip core, mb arm (mbstrlen + mbcharcliplen + 22021
        /// plane). Expected class: fast/ladder.
        #[kani::unwind(10)]
        fn eq_bpchar_clip_utf8() { body_bpchar_clip(PG_UTF8); }
    }

    fn body_varchar_clip(enc: i32) {
        install_vc(enc);
        let payload: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let s = &payload[..len];
        let atttypmod: i32 = kani::any();
        let mut c_len: usize = 0;
        let c = unsafe { pgvc_varchar_input(s.as_ptr(), len, atttypmod, &mut c_len) };
        let cerr = unsafe { pgvc_take_err() };
        let r = adt_varchar::varchar_clip(s, atttypmod, None);
        let ok = match r {
            Ok(Some(l)) => c == 0 && cerr == 0 && l == c_len,
            Ok(None) => false,
            Err(e) => {
                let cls = err_class(&e);
                core::mem::forget(e);
                c == PG_CERR && cls == cerr
            }
        };
        assert!(ok, "C/Rust divergence (varchar_clip decision)");
    }

    oc_proof! {
        /// row 1046 clip core, single-byte arm, FULL-i32 typmod. fast.
        #[kani::unwind(10)]
        fn eq_varchar_clip_latin1() { body_varchar_clip(PG_LATIN1); }
    }

    oc_proof! {
        /// row 1046 clip core, mb arm. fast/ladder.
        #[kani::unwind(10)]
        fn eq_varchar_clip_utf8() { body_varchar_clip(PG_UTF8); }
    }

    // ---- length coercions (rows 668 / 669), wrapper-level ----

    fn body_bpchar_coerce(enc: i32) {
        install_vc(enc);
        let s = sym_varlena::<CAP>();
        let maxlen: i32 = kani::any();
        // Bounded typmod: the blank-pad image (total <= 8 + 16) stays in
        // the theorem. The unbounded pad plane (typmod up to 2^31 => 2^31
        // pad bytes) is unbuildable in any harness; the alloc-guard wrap
        // plane is spot-proved below.
        kani::assume((-8..=20).contains(&maxlen));
        let is_explicit: bool = kani::any();
        let mut ret_source: c_int = 0;
        let mut c_out = [0u8; 64];
        let c = unsafe {
            pgvc_bpchar(
                s.data(),
                s.clen(),
                maxlen,
                is_explicit as c_int,
                &mut ret_source,
                c_out.as_mut_ptr(),
                64,
            )
        };
        let cerr = unsafe { pgvc_take_err() };
        let r = with_ctx(|mcx| {
            call(
                adt_varchar::builtins::fc_bpchar,
                [s.datum(), Datum::from_i32(maxlen), Datum::from_bool(is_explicit)],
                COLL_C,
                mcx,
            )
        });
        // Identity planes return the ORIGINAL arg datum on both sides.
        match r {
            Ok(d) if ret_source == 1 => {
                assert!(
                    cerr == 0 && c != PG_CERR && d.as_usize() == s.img.as_ptr() as usize,
                    "C/Rust divergence (bpchar identity plane)"
                );
            }
            other => {
                assert!(ret_source == 0, "C identity where Rust built/errored");
                check_bytes(other, c, &c_out, cerr);
            }
        }
    }

    oc_proof! {
        /// row 668 single-byte arm: identity planes + truncation + 22001 +
        /// blank-pad image (bounded typmod), is_explicit symbolic.
        /// Expected class: ladder (pad image explicit-length).
        #[kani::unwind(12)]
        fn eq_bpchar_coerce_latin1() { body_bpchar_coerce(PG_LATIN1); }
    }

    oc_proof! {
        /// row 668 mb arm. Expected class: ladder.
        #[kani::unwind(12)]
        fn eq_bpchar_coerce_utf8() { body_bpchar_coerce(PG_UTF8); }
    }

    oc_proof! {
        /// row 668 alloc-guard wrap plane spot (typmod = INT32_MAX, 2-byte
        /// UTF8 source so charlen < len and C's `palloc(maxlen + VARHDRSZ)`
        /// int request WRAPS before the Size sign-extension) — machine-
        /// checks the shipped comment-claim (varchar/src/lib.rs bpchar)
        /// that pgrust reproduces C's exact wrapped request. VERDICT-ONLY
        /// (class 5): both sides must error. fast.
        #[kani::unwind(6)]
        fn spot_bpchar_coerce_wrap() {
            install_vc(PG_UTF8);
            let s = concrete_varlena::<CAP>(&[0xC3, 0xA9]); // one 2-byte char
            let mut ret_source: c_int = 0;
            let mut c_out = [0u8; 8];
            let c = unsafe {
                pgvc_bpchar(
                    s.data(), s.clen(), i32::MAX, 1,
                    &mut ret_source, c_out.as_mut_ptr(), 8,
                )
            };
            let cerr = unsafe { pgvc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_varchar::builtins::fc_bpchar,
                    [s.datum(), Datum::from_i32(i32::MAX), Datum::from_bool(true)],
                    COLL_C,
                    mcx,
                )
            });
            let rust_err = r.is_err();
            if let Err(e) = r {
                core::mem::forget(e);
            }
            assert!(
                c == PG_CERR && cerr == 5 && rust_err,
                "C/Rust divergence (bpchar alloc-guard wrap plane)"
            );
        }
    }

    fn body_varchar_coerce(enc: i32) {
        install_vc(enc);
        let s = sym_varlena::<CAP>();
        // FULL-i32 typmod is safe here: the built image is bounded by the
        // source length. The INT32_MIN wrap plane (maxlen wraps positive ->
        // identity return) is in-theorem — machine-checks the shipped
        // comment-claim (varchar/src/lib.rs varchar).
        let typmod: i32 = kani::any();
        let is_explicit: bool = kani::any();
        let mut ret_source: c_int = 0;
        let mut c_out = [0u8; CAP];
        let c = unsafe {
            pgvc_varchar(
                s.data(),
                s.clen(),
                typmod,
                is_explicit as c_int,
                &mut ret_source,
                c_out.as_mut_ptr(),
            )
        };
        let cerr = unsafe { pgvc_take_err() };
        let r = with_ctx(|mcx| {
            call(
                adt_varchar::builtins::fc_varchar,
                [s.datum(), Datum::from_i32(typmod), Datum::from_bool(is_explicit)],
                COLL_C,
                mcx,
            )
        });
        match r {
            Ok(d) if ret_source == 1 => {
                assert!(
                    cerr == 0 && c != PG_CERR && d.as_usize() == s.img.as_ptr() as usize,
                    "C/Rust divergence (varchar identity plane)"
                );
            }
            other => {
                assert!(ret_source == 0, "C identity where Rust built/errored");
                check_bytes(other, c, &c_out, cerr);
            }
        }
    }

    oc_proof! {
        /// row 669 single-byte arm, FULL-i32 typmod + symbolic is_explicit
        /// (identity/truncate/22001 planes + INT32_MIN wrap claim). ladder.
        #[kani::unwind(10)]
        fn eq_varchar_coerce_latin1() { body_varchar_coerce(PG_LATIN1); }
    }

    oc_proof! {
        /// row 669 mb arm. ladder.
        #[kani::unwind(10)]
        fn eq_varchar_coerce_utf8() { body_varchar_coerce(PG_UTF8); }
    }

    // ---- char/name conversions (rows 860 / 409 / 408) ----

    oc_proof! {
        /// row 860: char -> bpchar(1), full symbolic i8 (fixed 1-byte
        /// image; the (char) cast plane included). fast.
        #[kani::unwind(4)]
        fn eq_char_bpchar() {
            install_vc(PG_LATIN1);
            let cval: i8 = kani::any();
            let mut c_out = [0u8; 1];
            let c = unsafe { pgvc_char_bpchar(cval, c_out.as_mut_ptr()) };
            let cerr = unsafe { pgvc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_varchar::builtins::fc_char_bpchar,
                    [Datum::from_i8(cval)],
                    COLL_C,
                    mcx,
                )
            });
            check_bytes(r, c, &c_out, cerr);
        }
    }

    oc_proof! {
        /// row 409 short arm (payload cap 8 -> NAMEDATALEN clip dead):
        /// trailing-blank trim + zero-padded 64-byte block, full compare
        /// (fixed-length result — scalar-verdict class, name-cmp
        /// precedent). Symbolic over both encodings (no mb dispatch on
        /// this arm). fast.
        #[kani::unwind(11)]
        fn eq_bpchar_name_short() {
            let enc = if kani::any() { PG_UTF8 } else { PG_LATIN1 };
            install_vc(enc);
            let a = sym_varlena::<CAP>();
            let mut c_out = [0u8; NAMEDATALEN];
            let c = unsafe { pgvc_bpchar_name(a.data(), a.clen(), c_out.as_mut_ptr()) };
            let cerr = unsafe { pgvc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_varchar::builtins::fc_bpchar_name,
                    [a.datum()],
                    COLL_C,
                    mcx,
                )
            });
            let ok = match r {
                Ok(d) => {
                    let name = unsafe {
                        core::slice::from_raw_parts(d.as_usize() as *const u8, NAMEDATALEN)
                    };
                    let mut same = cerr == 0 && c != PG_CERR;
                    let mut i = 0usize;
                    while i < NAMEDATALEN {
                        same = same && name[i] == c_out[i];
                        i += 1;
                    }
                    same
                }
                Err(e) => {
                    core::mem::forget(e);
                    false
                }
            };
            assert!(ok, "C/Rust divergence (bpchar_name)");
        }
    }

    oc_proof! {
        /// row 409 clip arm (LATIN1, concrete length 66 > NAMEDATALEN,
        /// symbolic bytes): pg_mbcliplen truncation to 63 + blank trim.
        /// Uses a 4B-header inline image (payload > short-form cap).
        /// Expected class: release-gate (unwind 70). UTF8 clip covered by
        /// the concrete spot inside the same harness (2-byte char
        /// straddling the 63-byte boundary).
        #[kani::unwind(70)]
        fn eq_bpchar_name_clip() {
            install_vc(PG_LATIN1);
            let payload: [u8; 66] = kani::any();
            let mut img = [0u8; 66 + 4];
            img[..4].copy_from_slice(&datum::varlena::set_varsize_4b(66 + 4));
            let mut i = 0;
            while i < 66 {
                img[4 + i] = payload[i];
                i += 1;
            }
            let mut c_out = [0u8; NAMEDATALEN];
            let c = unsafe { pgvc_bpchar_name(img[4..].as_ptr(), 66, c_out.as_mut_ptr()) };
            let cerr = unsafe { pgvc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_varchar::builtins::fc_bpchar_name,
                    [Datum::from_usize(img.as_ptr() as usize)],
                    COLL_C,
                    mcx,
                )
            });
            let ok = match r {
                Ok(d) => {
                    let name = unsafe {
                        core::slice::from_raw_parts(d.as_usize() as *const u8, NAMEDATALEN)
                    };
                    let mut same = cerr == 0 && c != PG_CERR;
                    let mut i = 0usize;
                    while i < NAMEDATALEN {
                        same = same && name[i] == c_out[i];
                        i += 1;
                    }
                    same
                }
                Err(e) => {
                    core::mem::forget(e);
                    false
                }
            };
            assert!(ok, "C/Rust divergence (bpchar_name clip arm)");
        }
    }

    oc_proof! {
        /// row 408: name -> text. First 8 bytes symbolic, byte 8 pinned to
        /// NUL (bounds the strlen/position walk; the tail is dead), byte 63
        /// NUL per the Name datatype invariant (module-doc fence). Image
        /// compare over the <= 8 copied bytes. fast.
        #[kani::unwind(11)]
        fn eq_name_bpchar() {
            install_vc(PG_LATIN1);
            let head: [u8; 8] = kani::any();
            let mut name = [0u8; NAMEDATALEN];
            let mut i = 0;
            while i < 8 {
                name[i] = head[i];
                i += 1;
            }
            // name[8..] stays 0 (incl name[63]: the Name invariant).
            let mut c_out = [0u8; NAMEDATALEN];
            let c = unsafe { pgvc_name_bpchar(name.as_ptr(), c_out.as_mut_ptr()) };
            let cerr = unsafe { pgvc_take_err() };
            let r = with_ctx(|mcx| {
                call(
                    adt_varchar::builtins::fc_name_bpchar,
                    [Datum::from_usize(name.as_ptr() as usize)],
                    COLL_C,
                    mcx,
                )
            });
            check_bytes(r, c, &c_out, cerr);
        }
    }

    // ---- typmod I/O (rows 2913/2915 check core; 2914/2916) ----

    oc_proof! {
        /// rows 2913/2915 CHECK CORE (n and per-value planes incl both
        /// 22023 error classes; ArrayGetIntegerTypmods out of theorem —
        /// see module doc; the "char"/"varchar" name feeds only stubbed
        /// message text, so one theorem covers both rows). fast.
        #[kani::unwind(4)]
        fn eq_anychar_typmodin() {
            install_vc(PG_LATIN1);
            let tl: [i32; 2] = kani::any();
            let n: usize = kani::any();
            kani::assume(n <= 2);
            let mut c_typmod: c_int = 0;
            let c = unsafe { pgvc_anychar_typmodin(tl.as_ptr(), n as c_int, &mut c_typmod) };
            let cerr = unsafe { pgvc_take_err() };
            let r = adt_varchar::anychar_typmodin(&tl[..n], "char");
            let ok = match r {
                Ok(t) => c == 0 && cerr == 0 && t == c_typmod,
                Err(e) => {
                    let cls = err_class(&e);
                    core::mem::forget(e);
                    c == PG_CERR && cls == cerr
                }
            };
            assert!(ok, "C/Rust divergence (anychar_typmodin)");
        }
    }

    fn body_typmodout(typmod: i32) {
        let mut c_out = [0u8; 64];
        let c = unsafe { pgvc_anychar_typmodout(typmod, c_out.as_mut_ptr()) };
        let cerr = unsafe { pgvc_take_err() };
        let mut r_out = [0u8; 16];
        let n = adt_varchar::anychar_typmodout(typmod, &mut r_out);
        let mut same = cerr == 0 && c >= 0 && n == c as usize;
        let mut i = 0usize;
        while i < n && i < c_out.len() {
            same = same && r_out[i] == c_out[i];
            i += 1;
        }
        assert!(same, "C/Rust divergence (anychar_typmodout)");
    }

    oc_proof! {
        /// rows 2914/2916 band: typmod in [-16, 10_000_003] (intout law:
        /// digit emission ~1e7-wide band; the empty-"()" plane typmod <=
        /// VARHDRSZ in-theorem). Compares through REL_18's own pg_ltoa
        /// (C-file shim 8). Expected class: ladder.
        /// unwind 12: pg_ultoa_n emits up to 7 digits (2/iter) and the
        /// byte-compare loop runs n <= 9 iterations (fleet fail:
        /// unwinding assertion at unwind(4)).
        #[kani::unwind(12)]
        fn eq_anychar_typmodout_band() {
            install_vc(PG_LATIN1);
            let typmod: i32 = kani::any();
            kani::assume((-16..=10_000_003).contains(&typmod));
            body_typmodout(typmod);
        }
    }

    oc_proof! {
        /// rows 2914/2916 wide-digit spots: MaxAttrSize+VARHDRSZ (the
        /// largest valid typmod), INT32_MAX, INT32_MIN (negative ->
        /// empty-string plane). fast.
        /// unwind 14: INT32_MAX emits 10 digits -> compare loop n <= 12
        /// (fleet fail: unwinding assertion at unwind(6)).
        #[kani::unwind(14)]
        fn spot_anychar_typmodout_wide() {
            install_vc(PG_LATIN1);
            body_typmodout(10 * 1024 * 1024 + 4);
            body_typmodout(i32::MAX);
            body_typmodout(i32::MIN);
        }
    }

    // =====================================================================
    // G. negative controls (MUST FAIL — non-vacuity gates; run with the
    //    DEFAULT solver per the kissat-failure trap)
    // =====================================================================

    oc_proof! {
        /// CONTROL (oracle rig): C sees a one-shorter trim set. MUST FAIL.
        #[kani::unwind(11)]
        fn control_dobyteatrim_short_set() {
            let s = concrete_varlena::<CAP>(&[7, 1, 2, 7]);
            let set = concrete_varlena::<SETCAP>(&[7, 1]);
            let mut c_start: c_int = 0;
            let mut c_out = [0u8; CAP];
            let c_len = unsafe {
                pgoc_dobyteatrim(
                    s.data(), s.clen(),
                    set.data(), set.clen() - 1, // skew: C's set loses the 1
                    1, 1,
                    &mut c_start, c_out.as_mut_ptr(),
                )
            };
            let cerr = unsafe { pgoc_take_err() };
            let w = adt_oracle_compat::dobyteatrim(s.payload(), set.payload(), true, true);
            let off = (w.as_ptr() as usize).wrapping_sub(s.payload().as_ptr() as usize);
            assert!(
                cerr == 0 && off == c_start as usize && w.len() == c_len as usize,
                "control: rig failed to detect a skewed set"
            );
        }
    }

    oc_proof! {
        /// CONTROL (varchar rig): C clips against atttypmod + 1. MUST FAIL.
        /// Input sits on the FITS-EXACTLY plane of the skewed typmod
        /// (len 4 = varchar(4)) so the skew FLIPS THE VERDICT (C accepts,
        /// Rust 22001s). The original b"abcdef" was VACUOUS: both sides
        /// errored with the same class (maxlen only appears in message
        /// text, out of proof), so the skew was invisible to the
        /// error-class-only compare — fleet 994f9977 caught the control
        /// passing (broken-gate class).
        #[kani::unwind(10)]
        fn control_varchar_clip_skew() {
            install_vc(PG_LATIN1);
            let s = b"abcd";
            let atttypmod: i32 = 4 + 3; // varchar(3)
            let mut c_len: usize = 0;
            let c = unsafe {
                pgvc_varchar_input(s.as_ptr(), s.len(), atttypmod + 1, &mut c_len)
            };
            let cerr = unsafe { pgvc_take_err() };
            let r = adt_varchar::varchar_clip(s, atttypmod, None);
            let ok = match r {
                Ok(Some(l)) => c == 0 && cerr == 0 && l == c_len,
                Ok(None) => false,
                Err(e) => {
                    let cls = err_class(&e);
                    core::mem::forget(e);
                    c == PG_CERR && cls == cerr
                }
            };
            assert!(ok, "control: rig failed to detect a skewed typmod");
        }
    }

    // =====================================================================
    // H. shared coverage witnesses (hoisted per the varbit/mbconv cover
    //    law: one dedicated harness per rig, cover-only, no asserts)
    // =====================================================================

    oc_proof! {
        /// Oracle-rig regime witnesses (vacuity insurance for the symbolic
        /// harnesses above): trim-some / trim-all / trim-none windows,
        /// pad truncation + pad arms, repeat ok arm.
        #[kani::unwind(11)]
        fn cover_oracle_regimes() {
            install_oc(PG_LATIN1);
            let s = sym_varlena::<CAP>();
            let set = sym_varlena::<SETCAP>();
            let ctx = MemoryContext::new_bump("proof");
            // ManuallyDrop keeps the Err(Box<PgError>) drop glue out of the
            // formula (TRIAGE error-drop trap).
            let w = core::mem::ManuallyDrop::new(adt_oracle_compat::dotrim_slice(
                ctx.mcx(),
                s.payload(),
                set.payload(),
                true,
                true,
            ));
            if let Ok(w) = &*w {
                kani::cover!(w.len() == s.len && s.len > 0, "trim-none window reachable");
                kani::cover!(w.is_empty() && s.len > 0, "trim-all window reachable");
                kani::cover!(!w.is_empty() && w.len() < s.len, "partial trim reachable");
            }
            core::mem::forget(ctx);

            let len: i32 = kani::any();
            kani::assume((0..=6).contains(&len));
            let s1 = sym_varlena::<4>();
            let s2 = sym_varlena::<3>();
            let ctx2 = MemoryContext::new_bump("proof");
            // ManuallyDrop: no drop glue, so the ctx2 borrow ends at last
            // use and the teardown stays out of the formula (mcx-stubs
            // recipe).
            let r = core::mem::ManuallyDrop::new(adt_oracle_compat::lpad(
                ctx2.mcx(),
                s1.payload(),
                len,
                s2.payload(),
            ));
            if r.is_ok() {
                kani::cover!(len as usize > s1.len && s2.len > 0, "pad arm reachable");
                kani::cover!((len as usize) < s1.len, "truncate arm reachable");
            }
            core::mem::forget(ctx2);
        }
    }

    oc_proof! {
        /// Varchar-rig regime witnesses: clip truncation / pad / error /
        /// invalid-typmod planes, coercion identity plane.
        #[kani::unwind(10)]
        fn cover_varchar_regimes() {
            install_vc(PG_LATIN1);
            let payload: [u8; CAP] = kani::any();
            let len: usize = kani::any();
            kani::assume(len <= CAP);
            let s = &payload[..len];
            let atttypmod: i32 = kani::any();
            let r = adt_varchar::bpchar_clip(s, atttypmod, None);
            match r {
                Ok(Some(clip)) => {
                    kani::cover!(clip.total > clip.copy, "blank-pad arm reachable");
                    kani::cover!(clip.copy < len, "truncation arm reachable");
                    kani::cover!(atttypmod < 4 && clip.total == len, "invalid-typmod identity reachable");
                }
                Ok(None) => {}
                Err(e) => {
                    kani::cover!(true, "22001 error arm reachable");
                    core::mem::forget(e);
                }
            }
            let ctx2 = MemoryContext::new_bump("proof");
            // ManuallyDrop: see cover_oracle_regimes.
            let r2 = core::mem::ManuallyDrop::new(adt_varchar::varchar(
                ctx2.mcx(),
                s,
                kani::any(),
                kani::any(),
            ));
            match &*r2 {
                Ok(None) => kani::cover!(true, "varchar identity plane reachable"),
                Ok(Some(_)) => kani::cover!(true, "varchar truncation image reachable"),
                Err(_) => {}
            }
            core::mem::forget(ctx2);
        }
    }
}
