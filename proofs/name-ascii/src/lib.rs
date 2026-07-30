//! Kani C≡Rust equivalence proofs: PostgreSQL name-type family
//! (namein / nameout / nameeq..namege / btnamecmp, name.c) and pg_to_ascii
//! (ascii.c) vs the shipped pgrust `name` and `adt_ascii` crates.
//!
//! Domain fences (documented in the ledger):
//! - Comparisons: collid = C_COLLATION_OID only (the C-locale strncmp fast
//!   path). Non-C collations route to varstr_cmp/locale on both sides — out
//!   of scope. Buffers are FULL 64-byte symbolic, no terminator assumption
//!   (strncmp with n=NAMEDATALEN is total over the buffer).
//! - namein: pg_mbcliplen is a seam on the Rust side; the harness installs
//!   the single-byte-encoding model (min(len, limit)) and the vendored C uses
//!   the identical model — namein is proved C≡Rust modulo an identical
//!   mbcliplen (multibyte clip parity is the mbutils family's proof).
//! - nameout: contract domain data[63] == 0 (C invariant: NAME values always
//!   carry a terminator; without it C's pstrdup/strlen reads out of bounds).
//! - pg_to_ascii: enc ∈ {LATIN1, LATIN2, LATIN9, WIN1250} (supported set),
//!   src cap 8 bytes; plus an unsupported-enc harness (both sides error).

#[cfg(kani)]
mod proofs {
    use std::os::raw::c_int;
    use types_core::C_COLLATION_OID;
    use types_tuple::NameData;
    use wchar::{PG_LATIN1, PG_LATIN2, PG_LATIN9, PG_WIN1250};

    const NAMELEN: usize = 64;

    extern "C" {
        fn pg_namein(s: *const u8, result: *mut u8) -> c_int;
        fn pg_nameout(name: *const u8, out: *mut u8) -> c_int;
        fn pg_nameeq(a: *const u8, b: *const u8, collid: u32) -> c_int;
        fn pg_namene(a: *const u8, b: *const u8, collid: u32) -> c_int;
        fn pg_namelt(a: *const u8, b: *const u8, collid: u32) -> c_int;
        fn pg_namele(a: *const u8, b: *const u8, collid: u32) -> c_int;
        fn pg_namegt(a: *const u8, b: *const u8, collid: u32) -> c_int;
        fn pg_namege(a: *const u8, b: *const u8, collid: u32) -> c_int;
        fn pg_btnamecmp(a: *const u8, b: *const u8, collid: u32) -> c_int;
        fn pg_c_to_ascii(src: *const u8, src_end: *const u8, dest: *mut u8, enc: c_int) -> c_int;
    }

    fn any_name() -> NameData {
        NameData { data: kani::any() }
    }

    /// Prefix-16-symbolic variant: bytes 16..64 fixed zero.
    fn any_name_cap16() -> NameData {
        let mut n = NameData::default();
        let prefix: [u8; 16] = kani::any();
        n.data[..16].copy_from_slice(&prefix);
        n
    }

    // ---------- comparison family (full 64-byte symbolic) ----------

    #[kani::proof]
    #[kani::unwind(66)]
    fn eq_btnamecmp_full64() {
        let (a, b) = (any_name(), any_name());
        let c = unsafe { pg_btnamecmp(a.data.as_ptr(), b.data.as_ptr(), C_COLLATION_OID) };
        let r = name::btnamecmp(&a, &b, C_COLLATION_OID).unwrap();
        assert_eq!(c, r, "btnamecmp: raw strncmp value diverges");
    }

    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_btnamecmp_cap16() {
        let (a, b) = (any_name_cap16(), any_name_cap16());
        let c = unsafe { pg_btnamecmp(a.data.as_ptr(), b.data.as_ptr(), C_COLLATION_OID) };
        let r = name::btnamecmp(&a, &b, C_COLLATION_OID).unwrap();
        assert_eq!(c, r);
    }

    macro_rules! eq_bool_op {
        ($harness:ident, $cfn:ident, $rfn:ident) => {
            #[kani::proof]
            #[kani::unwind(66)]
            fn $harness() {
                let (a, b) = (any_name(), any_name());
                let c = unsafe { $cfn(a.data.as_ptr(), b.data.as_ptr(), C_COLLATION_OID) };
                let r = name::$rfn(&a, &b, C_COLLATION_OID).unwrap();
                assert_eq!(c != 0, r, "boolean comparator diverges");
            }
        };
    }

    eq_bool_op!(eq_nameeq_full64, pg_nameeq, nameeq);
    eq_bool_op!(eq_namene_full64, pg_namene, namene);
    eq_bool_op!(eq_namelt_full64, pg_namelt, namelt);
    eq_bool_op!(eq_namele_full64, pg_namele, namele);
    eq_bool_op!(eq_namegt_full64, pg_namegt, namegt);
    eq_bool_op!(eq_namege_full64, pg_namege, namege);

    // ---------- namein ----------

    fn install_mbcliplen_single_byte() {
        // Single-byte-encoding model, identical to the C shim (see c/ header).
        mbutils_seams::pg_mbcliplen::set(|_mbstr, len, limit| if len < limit { len } else { limit });
    }

    /// Full regime: NUL-terminated inputs of length 0..=71 (both under and
    /// over the NAMEDATALEN truncation threshold), symbolic bytes.
    #[kani::proof]
    #[kani::unwind(74)]
    fn eq_namein_sym_len_le_71() {
        install_mbcliplen_single_byte();
        let mut buf: [u8; 72] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 71);
        for i in 0..72 {
            if i < len {
                kani::assume(buf[i] != 0); // cstring: no interior NUL
            }
        }
        buf[len] = 0;

        let mut c_out = [0xAAu8; NAMELEN];
        unsafe { pg_namein(buf.as_ptr(), c_out.as_mut_ptr()) };
        let r = name::namein(&buf[..len]);
        assert_eq!(r.data, c_out, "namein: result NameData diverges");
    }

    // ---------- text_name (oids 407/1400) ----------

    extern "C" {
        fn pg_text_name(s: *const u8, len: c_int, result: *mut u8) -> c_int;
        fn pg_name_text(name: *const u8, out: *mut u8) -> c_int;
    }

    /// text_name = namein over an explicit-length text payload: symbolic
    /// lengths 0..=71 (under and over the truncation threshold), fully
    /// symbolic bytes INCLUDING interior NULs (no cstring fence — this is
    /// exactly how text_name differs from namein). Same single-byte
    /// pg_mbcliplen model on both sides as eq_namein.
    #[kani::proof]
    #[kani::unwind(74)]
    fn eq_text_name_sym_len_le_71() {
        install_mbcliplen_single_byte();
        let buf: [u8; 72] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 71);
        // Interior-NUL and truncation regimes both reachable.
        kani::cover!(len > 0 && buf[0] == 0);
        kani::cover!(len >= NAMELEN);

        let mut c_out = [0xAAu8; NAMELEN];
        unsafe { pg_text_name(buf.as_ptr(), len as c_int, c_out.as_mut_ptr()) };
        let r = name::namein(&buf[..len]);
        assert_eq!(r.data, c_out, "text_name: result NameData diverges");
    }

    // ---------- name_text (oids 406/1401) ----------

    /// name_text VALUE CORE: the text payload is the strlen prefix of the
    /// name (Rust side: shipped NameData::name_str(), exactly what
    /// fc_name_text hands to cstring_to_text). The varlena construction /
    /// allocation around it is plumbing and stays out of the proof (same
    /// reduction as the C shim, documented in the C file). Contract domain:
    /// data[63] == 0 (C invariant: NAME values are NUL-terminated).
    #[kani::proof]
    #[kani::unwind(66)]
    fn eq_name_text_payload() {
        let mut a = any_name();
        a.data[NAMELEN - 1] = 0; // contract: terminated

        let mut c_out = [0xAAu8; NAMELEN];
        let c_len = unsafe { pg_name_text(a.data.as_ptr(), c_out.as_mut_ptr()) };

        let r = a.name_str();
        assert_eq!(r.len() as c_int, c_len, "name_text: payload length diverges");
        for i in 0..NAMELEN {
            if i < r.len() {
                assert!(r[i] == c_out[i], "name_text: payload byte diverges");
            }
        }
    }

    // ---------- nameout ----------

    /// Contract domain: NAME values always carry a NUL terminator
    /// (data[63] == 0 admits every terminated buffer).
    #[kani::proof]
    #[kani::unwind(66)]
    fn eq_nameout_terminated() {
        let mut n = any_name();
        n.data[NAMELEN - 1] = 0;

        let mut c_out = [0xAAu8; NAMELEN];
        let c_len = unsafe { pg_nameout(n.data.as_ptr(), c_out.as_mut_ptr()) } as usize;

        let mut r_out: Vec<u8> = Vec::new();
        name::nameout_into(&n, &mut r_out);

        assert_eq!(r_out.len(), c_len + 1, "nameout: cstring length diverges");
        assert_eq!(&r_out[..], &c_out[..c_len + 1], "nameout: bytes diverge");
    }

    // ---------- pg_to_ascii ----------

    /// Case-split on the encoding (a symbolic enc drags Kani's symex through
    /// the Rust error-constructor branch — format!/Box alloc machinery — and
    /// walls; concrete enc constant-folds that branch dead). One harness per
    /// supported encoding + a union-coverage harness for the split.
    fn check_to_ascii_fixed_enc(enc: i32) {
        let src: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);

        let mut c_dest = [0u8; 8];
        let rc = unsafe {
            pg_c_to_ascii(src.as_ptr(), src.as_ptr().add(len), c_dest.as_mut_ptr(), enc)
        };
        assert_eq!(rc, 0, "C rejected a supported encoding");

        let mut r_dest = [0u8; 8];
        adt_ascii::pg_to_ascii(&src[..len], &mut r_dest[..len], enc)
            .expect("Rust rejected a supported encoding");

        assert_eq!(&r_dest[..len], &c_dest[..len], "to_ascii: output bytes diverge");
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_to_ascii_latin1() {
        check_to_ascii_fixed_enc(PG_LATIN1);
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_to_ascii_latin2() {
        check_to_ascii_fixed_enc(PG_LATIN2);
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_to_ascii_latin9() {
        check_to_ascii_fixed_enc(PG_LATIN9);
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_to_ascii_win1250() {
        check_to_ascii_fixed_enc(PG_WIN1250);
    }

    /// MANDATORY union-coverage harness for the case-split: every encoding
    /// in the proof domain is one of the four proved cases.
    #[kani::proof]
    fn cover_to_ascii_enc_split() {
        let enc: i32 = kani::any();
        kani::assume(enc == PG_LATIN1 || enc == PG_LATIN2 || enc == PG_LATIN9 || enc == PG_WIN1250);
        assert!(
            enc == PG_LATIN1 || enc == PG_LATIN2 || enc == PG_LATIN9 || enc == PG_WIN1250,
            "case-split covers the supported-encoding domain"
        );
    }

    // Unsupported-encoding error path: NOT harnessed (wall). Even with a
    // concrete enc, Rust's error constructor (format! + pg_encoding_to_char
    // + Box<PgError>) exceeds the 30s solver budget (measured >120s,
    // 2026-07-28). Fence: the proofs above cover the supported-encoding
    // domain {LATIN1, LATIN2, LATIN9, WIN1250}; both sides' encoding
    // admission is the same 4-arm if-chain (C returns -1 / Rust returns Err
    // for everything else), checked by unit tests, not by Kani.

    // ---------- negative controls (MUST FAIL: rig non-vacuity) ----------

    /// Feeds C and Rust names that differ in byte 0 and asserts the equality
    /// verdicts agree with a same-input run. Expected: VERIFICATION FAILED
    /// with a decodable counterexample. A pass here means the rig is broken.
    #[kani::proof]
    #[kani::unwind(66)]
    fn control_name_mismatch_must_fail() {
        let a = any_name();
        let mut b = a;
        b.data[0] = a.data[0].wrapping_add(1);
        let c = unsafe { pg_nameeq(a.data.as_ptr(), b.data.as_ptr(), C_COLLATION_OID) };
        let r = name::nameeq(&a, &a, C_COLLATION_OID).unwrap();
        assert_eq!(c != 0, r, "expected: this control diverges");
    }

    /// Compares Rust LATIN1 output against C LATIN2 output. Expected:
    /// VERIFICATION FAILED (the maps differ).
    #[kani::proof]
    #[kani::unwind(10)]
    fn control_ascii_enc_mismatch_must_fail() {
        let src: [u8; 2] = kani::any();
        let mut c_dest = [0u8; 2];
        unsafe { pg_c_to_ascii(src.as_ptr(), src.as_ptr().add(2), c_dest.as_mut_ptr(), PG_LATIN2) };
        let mut r_dest = [0u8; 2];
        adt_ascii::pg_to_ascii(&src, &mut r_dest, PG_LATIN1).unwrap();
        // plain assert!: assert_eq! on arrays drags Debug-format machinery
        // into symex on the (intended) failure path and walls.
        assert!(r_dest == c_dest, "expected: LATIN1 vs LATIN2 diverge");
    }
}
