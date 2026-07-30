//! Kani C≡Rust equivalence: "char" type octal escape codec.
//! Rust side: shipped crates/backend/utils/adt/char (charin/charout).
//! C side: vendored REL_18_STABLE char.c core logic (csrc/char_shim.c) —
//! charin/charout originally fetched from master 2026-07-28, verified
//! byte-identical to REL_18_STABLE (proofs/PROVENANCE-AUDIT.md).

#[cfg(kani)]
mod proofs {
    extern "C" {
        fn pgc_charin(ch: *const u8) -> i32;
        fn pgc_charout(ch: i32, result: *mut u8) -> i32;
    }

    /// charout over all 2^8 inputs: byte-identical output + length,
    /// plus round-trip charin(charout(x)) == x.
    #[kani::proof]
    #[kani::unwind(12)]
    fn charout_equiv_and_roundtrip() {
        let ch: i8 = kani::any();

        let mut rbuf = [0u8; 4];
        let rlen = adt_char::charout(ch, &mut rbuf);

        let mut cbuf = [0u8; 5];
        let clen = unsafe { pgc_charout(ch as i32, cbuf.as_mut_ptr()) };

        assert_eq!(rlen as i32, clen);
        for i in 0..rlen {
            assert_eq!(rbuf[i], cbuf[i]);
        }

        // round-trip through BOTH implementations of charin
        let rt_rust = adt_char::charin(&rbuf[..rlen]);
        assert_eq!(rt_rust, ch);
        let rt_c = unsafe { pgc_charin(cbuf.as_ptr()) }; // cbuf is NUL-terminated
        // 8-bit datum-value parity: the shim widens PG_RETURN_CHAR to int,
        // and that widening is platform-split (C `char` signedness — signed
        // on macOS/x86-64-Linux, unsigned on Linux-aarch64). Real Postgres
        // never exposes the widened value: the Datum round-trips through
        // DatumGetChar (8-bit truncation). Ground-truthed 2026-07-29:
        // charout/chartoi4/comparisons byte-identical on docker postgres:18
        // (Linux aarch64) vs macOS psql 18.4.
        assert_eq!(rt_c as u8, ch as u8);
    }

    /// charin over all NUL-terminated strings of length 0..=4
    /// (interior bytes constrained nonzero = the cstring domain C sees).
    #[kani::proof]
    #[kani::unwind(12)]
    fn charin_equiv() {
        let len: usize = kani::any();
        kani::assume(len <= 4);
        let mut buf = [0u8; 5];
        for i in 0..len {
            let b: u8 = kani::any();
            kani::assume(b != 0);
            buf[i] = b;
        }
        // buf[len] == 0 terminator; bytes past it stay 0.

        let rust = adt_char::charin(&buf[..len]);
        let c = unsafe { pgc_charin(buf.as_ptr()) };
        // 8-bit datum-value parity (shim int-widening is char-signedness
        // platform-split; see charout_equiv_and_roundtrip comment).
        assert_eq!(rust as u8, c as u8);
    }

    // ---- chartoi4 / i4tochar / text_char / char_text (77/78/944/946) ----
    // C side vendored from REL_18_STABLE (csrc/char_shim.c, shims there).

    use proof_support::{mcx_stubs, stubs};
    use types_error::{ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR};

    extern "C" {
        fn pgc_chartoi4(ch: i32) -> i32;
        fn pgc_i4tochar(arg1: i32, err: *mut i32) -> i32;
        fn pgc_text_char(ch: *const u8, len: u64) -> i32;
        fn pgc_char_text(ch: i32, out: *mut u8) -> i32;
    }

    /// chartoi4: full i8 domain, exact i32 value (sign extension).
    #[kani::proof]
    fn eq_chartoi4() {
        let ch: i8 = kani::any();
        let c = unsafe { pgc_chartoi4(ch as i32) };
        let r = adt_char::chartoi4(ch);
        assert!(r == c);
    }

    /// i4tochar: full i32 domain, value + verdict + sqlstate/level parity
    /// on the range-error arm (message text stubbed out of the proof —
    /// stub_pg_error_error is field-identical minus location/message).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_i4tochar() {
        let v: i32 = kani::any();
        let mut cerr: i32 = -1;
        let c = unsafe { pgc_i4tochar(v, &mut cerr) };
        match adt_char::i4tochar(v) {
            // 8-bit datum-value parity (shim int-widening is char-signedness
            // platform-split; see charout_equiv_and_roundtrip comment).
            Ok(x) => assert!(cerr == 0 && x as u8 == c as u8),
            Err(e) => {
                let ok =
                    cerr == 1 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE && e.level == ERROR;
                // Box<PgError> drop glue walls symex (varbit-rows trap).
                core::mem::forget(e);
                assert!(ok);
            }
        }
    }

    /// text_char: symbolic varlena payload (len<=8, contents fully
    /// symbolic; pre-detoasted caller contract, bytea-cmp pattern).
    /// Unlike charin there is NO cstring fence: embedded NUL bytes are in
    /// the domain, exactly the "handle empty-string honestly" C variant.
    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_text_char() {
        const N: usize = 8;
        let buf: [u8; N] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= N);
        // Both octal-escape and passthrough regimes reachable.
        kani::cover!(len == 4 && buf[0] == b'\\');
        kani::cover!(len == 0);
        let c = unsafe { pgc_text_char(buf.as_ptr(), len as u64) };
        let r = adt_char::text_char(&buf[..len]);
        // 8-bit datum-value parity (shim int-widening is char-signedness
        // platform-split; see charout_equiv_and_roundtrip comment).
        assert!(r as u8 == c as u8);
    }

    /// char_text VALUE CORE: the C char_text body (vendored) against the
    /// shipped charout core — the exact function fc_char_text feeds to
    /// cstring_to_text. Proves the "0x00 -> empty string" honesty comment
    /// and octal-escape parity over full i8; only the text packing
    /// (cstring_to_text) sits outside this theorem (see eq_char_text below).
    #[kani::proof]
    fn eq_char_text_value_core() {
        let ch: i8 = kani::any();

        let mut cbuf = [0u8; 4];
        let clen = unsafe { pgc_char_text(ch as i32, cbuf.as_mut_ptr()) };

        let mut rbuf = [0u8; 4];
        let rlen = adt_char::charout(ch, &mut rbuf);

        assert!(rlen as i32 == clen);
        // Both buffers start zeroed and len <= 3, so whole-buffer equality
        // covers the payload without a symbolic-bound loop.
        assert!(rbuf == cbuf);
    }

    /// char_text: full i8 domain, payload bytes + length parity, modulo
    /// the static-buffer allocator model (mcx-stubs recipe; allocation
    /// strategy and context teardown leave the proof).
    ///
    /// NOT A STANDING GATE (2026-07-28): walls >180s under load on both
    /// solvers — the cstring_to_text PgVec/context machinery, the hex-crate
    /// wall class. The VALUE core of char_text is charout, proved full-i8
    /// (+ roundtrip) above; only the text packing sits outside that proof.
    /// Kept as the ready-made harness should the mcx wall get cheaper.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn eq_char_text() {
        let ch: i8 = kani::any();

        let mut cbuf = [0u8; 4];
        let clen = unsafe { pgc_char_text(ch as i32, cbuf.as_mut_ptr()) };

        let ctx = mcx::MemoryContext::new_bump("kani-char-text");
        let r = adt_char::char_text(ctx.mcx(), ch).unwrap();
        let data = r.data();
        assert!(data.len() as i32 == clen);
        let n = data.len();
        for i in 0..n {
            assert!(data[i] == cbuf[i]);
        }
        // Teardown is not part of the claim and walls symex.
        core::mem::forget(r);
        core::mem::forget(ctx);
    }
}
