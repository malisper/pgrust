//! Kani C≡Rust equivalence probe: jsonb BINARY-FORMAT operations (not the
//! parser) — header reads (jsonb_typeof / jsonb_array_length cores),
//! object-key binary search (-> / ->> lookup core), array indexing
//! (-> int core incl. negative subscripts), key existence (?), and the
//! btree comparator compareJsonbContainers (jsonb_cmp & the eq/ne/lt/le/
//! gt/ge family route every verdict through it).
//!
//! Rust side (SHIPPED code, path-dep — never copied):
//!   adt_jsonb::io::{container_type_name, extract_scalar}
//!   adt_jsonb::container::{array_length, get_key_value, get_ith_value,
//!     container_is_object/array/scalar}
//!   adt_jsonb::getfield::adjust_element_index (pub'd for this family)
//!   adt_jsonb::ops::{exists_key, compare_containers}
//! C side: c/pg_jsonb.c — verbatim REL_18_STABLE jsonb_util.c/jsonb.c cores
//! (provenance + every shim documented there).
//!
//! THE FENCE — jsonb binary-format invariant (jsonb.h "JEntry format"):
//! constraint-generation over raw symbolic bytes is replaced by a TRUSTED
//! BUILDER (harness infrastructure): symbolic counts, key/value bytes,
//! lengths and per-entry HAS_OFF choices are assembled into a
//! layout-valid container image —
//!   * header = count | JB_FOBJECT/JB_FARRAY (JB_FSCALAR on 1-elem raw
//!     scalar arrays only);
//!   * every JEntry is either length-form or HAS_OFF with the consistent
//!     cumulative end+1 offset (a strict SUPERSET of the writer's
//!     every-32nd-entry placement — readers must accept both, jsonb.h);
//!   * object keys strictly sorted+unique per lengthCompareJsonbString
//!     (the convertJsonbObject writer invariant the binary search needs);
//!   * scalar values fenced to null/bool/string — NUMERIC IMAGES ARE OUT
//!     OF FENCE (adt_numeric is its own proof family), depth 1 in this
//!     probe wave.
//! Both implementations read the SAME builder bytes; the varlena header /
//! detoast layer is out of scope (pre-detoasted payload fence, bytea-cmp
//! precedent), and result-image materialization (JsonbValueToJsonb /
//! item_to_jsonb_image) is out of scope — lookups are compared as
//! (found, type, payload window) tuples.
//!
//! Seam + scaffolding models (each restated in the ledger row):
//!   * varstr_cmp (string compare inside jsonb_cmp only): BOTH sides run
//!     the identical C-locale model (memcmp byte-difference + length
//!     tiebreak; C shim pg_seam_varstr_cmp, Rust -Z stubbing) — collation
//!     internals leave the proof (dt-minmax shared-seam precedent).
//!   * allocation: C palloc -> static bump pool; Rust Mcx::allocate/
//!     deallocate -> proof_support::mcx_stubs (allocation strategy out of
//!     scope, never the values).
//!
//! Negative control: control_object_field_short_key (C sees keylen-1) MUST
//! FAIL — run with the DEFAULT solver (kissat never terminates on failing
//! harnesses).
//!
//! MEASURED OUTCOME (2026-07-28, loaded shared box — see the final probe
//! report for the full table):
//!   - typeof / array_length / object_field / array_element /
//!     exists(arrays) / exists(object n<=1): ALL GREEN, 0 divergences.
//!   - exists(object n>=2): wall(memory>6GiB @45s) under the RSS protocol.
//!   - cmp family: WALL — the mcx-backed JsonbIterator is the blocker, not
//!     the compare logic. probe_ctx_iter_cost measures it: iterator init +
//!     one token on a CONCRETE empty array costs 47s with the token-ctx +
//!     proof-heap stubs and >240s with a real MemoryContext::new_bump;
//!     dual iterators + the compare loop exceed 240s/6GiB at every cell
//!     tried (0_0, 1_1, raw_raw; exact unwinds; cadical, kissat,
//!     --no-assertion-reach-checks). Fix path: a non-allocating compare
//!     core (fixed-array frame stack) in shipped code, then re-open.
//! ROUND 2 (2026-07-28/29, after the shipped non-allocating refactor): the
//! cmp family target is adt_jsonb::ops::compare_containers_fixed::<N> —
//! no-Mcx, no-heap; `None` is pure depth-abstention (production falls back
//! to the allocating walk), so THE THEOREM TARGET IS EXACTLY THE Some
//! REGION; harness cells use N=1 (flat, never abstains under the fence).
//! fc_jsonb_{cmp,eq,ne,lt,le,gt,ge} all route through this core
//! (builtins.rs cmp_args); rv == cv over i32 implies every boolean verdict
//! map, so one green cell covers all seven rows' logic at core level.
//!
//! ROUND-2 MEASURED OUTCOME (run-cmp.sh is the cmp recipe of record:
//! kissat + --no-assertion-reach-checks + --no-unwinding-checks +
//! per-loop EXACT --unwindset + Img<CMPCAP=32> images; soundness of the
//! disabled unwinding checks = every green cell's loop bounds are
//! STRUCTURAL ceilings independent of the symbolic bytes, so no input can
//! be truncated):
//!   GREEN (structural-order cells; 0.7-81s): bool_0_0, bool_1_2,
//!   raw_vs_arr, raw_vs_empty (the C no-else rawScalar/nElems quirk),
//!   obj_obj_0_0, obj_obj_1_2, obj_arr_1_1 (object>array root rank),
//!   nested_1i1_1i2 (depth-2, inner-n mismatch).
//!   WALL(memory>6GiB @6GiB cap, loaded shared box): every EQUAL-SHAPE
//!   full-element-walk cell (bool 1_1/2_2 incl per-side HAS_OFF-pinned
//!   sub-cells, raw_raw, string cells, crosstype, obj 1_1/2_2,
//!   nested equal shapes, abstention witness, seam-skew control) — the
//!   dual-iterator element-compare cross-product exceeds the cap at sound
//!   bounds; at tighter bounds the runs FAIL with TRUNCATION ARTIFACTS
//!   (adjudicated: native exhaustive ground-truth tables — bool 1_1 domain
//!   36/36 and string domain 7396/7396 IDENTICAL C-vs-Rust — so the
//!   kani FAILEDs are unwind artifacts, NOT divergences; scratchpad
//!   natenum/natstr).
//!   CONTROLS: control_cmp_swapped FAILED-as-required (default solver,
//!   52.8s) — the cmp rig is non-vacuous. control_cmp_seam_skew walls
//!   before reaching its counterexample (string cells wall) — recorded,
//!   not gate-bearing.
//!
//!   - RECIPE OF RECORD (run-all.sh is authoritative): DEFAULT solver
//!     (cadical), one pass per harness, 240s timeout — the configuration
//!     the standing green table was measured under. Historical probe note:
//!     during exploration cadical wedged on some low-constraint n0 cells
//!     (kissat was used ad hoc) and kissat re-solved per property batch on
//!     the big cells; the final suite runs entirely on the default solver.
//!     Every manual run goes through run-one.sh (one solver at a time,
//!     vm_stat gate, 6GiB RSS watchdog, own-family kills only).

#[cfg(kani)]
mod proofs {
    use adt_jsonb::container::{
        self, JsonbItem, JB_FARRAY, JB_FOBJECT, JB_FSCALAR, JENTRY_HAS_OFF, JENTRY_ISBOOL_FALSE,
        JENTRY_ISBOOL_TRUE, JENTRY_ISCONTAINER, JENTRY_ISNULL, JENTRY_ISNUMERIC, JENTRY_ISSTRING,
    };
    use proof_support::mcx_stubs;
    use std::os::raw::{c_char, c_int};
    use types_core::Oid;

    extern "C" {
        fn pgp_reset() -> c_int;
        fn pgp_take_abort() -> c_int;
        fn pgp_array_length(c: *const u8, err: *mut c_int) -> c_int;
        fn pgp_object_field(
            c: *const u8,
            key: *const u8,
            keylen: c_int,
            vtype: *mut c_int,
            vdata: *mut *const u8,
            vlen: *mut c_int,
            vbool: *mut c_int,
        ) -> c_int;
        fn pgp_array_element(
            c: *const u8,
            element: c_int,
            vtype: *mut c_int,
            vdata: *mut *const u8,
            vlen: *mut c_int,
            vbool: *mut c_int,
        ) -> c_int;
        fn pgp_exists(c: *const u8, key: *const u8, keylen: c_int) -> c_int;
        fn pgp_cmp(a: *const u8, b: *const u8) -> c_int;
        fn pgp_cmp_staged(a: *const u8, alen: c_int, b: *const u8, blen: c_int) -> c_int;
        fn pgp_iter_probe(a: *const u8, ncalls: c_int) -> c_int;
        fn pgp_typeof_name(c: *const u8) -> *const c_char;
    }

    /// Per-n case-split harness generator (escalation ladder step 3: a
    /// symbolic pair/elem count makes every builder address symbolic and
    /// walls the solver; the concrete-n family partitions the bound).
    macro_rules! per_n {
        ($($name:ident[$unwind:literal]: $check:ident($($arg:expr),*);)*) => {$(
            #[kani::proof]
            #[kani::unwind($unwind)]
            fn $name() { $check($($arg),*); }
        )*};
    }

    // ---- builder caps (the probe's cost knobs) ----
    const MAXN: usize = 3; // max elems / pairs per container
    const KL: usize = 3; // max key length
    const VL: usize = 3; // max string-value length
    const CAP: usize = 96; // container image cap

    /// 4-aligned image buffer (C reads the header/JEntrys as uint32).
    /// Default width CAP serves the lookup cells; the cmp cells use
    /// Img<CMPCAP> — every read of a symbolic offset is a byte-mux over
    /// the whole image object, so narrower cmp images shrink both formula
    /// size and the solver's memory peak (~3x, measured on this family).
    #[repr(align(8))]
    struct Img<const C: usize = CAP>([u8; C]);

    /// cmp-cell image width: covers every cmp builder shape (max 26 bytes
    /// for the n=2 object cells; nested = 24).
    const CMPCAP: usize = 32;

    fn put_u32<const C: usize>(buf: &mut Img<C>, off: usize, v: u32) {
        buf.0[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }

    /// One symbolic scalar leaf: 0 null, 1 false, 2 true, 3 string.
    #[derive(Clone, Copy)]
    struct Scalar {
        kind: u8,
        sbytes: [u8; VL],
        slen: usize,
    }

    fn any_scalar(allow_string: bool) -> Scalar {
        let kind: u8 = kani::any();
        kani::assume(kind <= if allow_string { 3 } else { 2 });
        let sbytes: [u8; VL] = kani::any();
        let slen: usize = kani::any();
        kani::assume(slen <= VL);
        Scalar { kind, sbytes, slen }
    }

    fn scalar_len(s: &Scalar) -> usize {
        if s.kind == 3 {
            s.slen
        } else {
            0
        }
    }

    fn scalar_jentry_type(s: &Scalar) -> u32 {
        match s.kind {
            0 => JENTRY_ISNULL,
            1 => JENTRY_ISBOOL_FALSE,
            2 => JENTRY_ISBOOL_TRUE,
            _ => JENTRY_ISSTRING,
        }
    }

    /// HAS_OFF pin for the cmp full-walk sub-cells (0 = symbolic choice,
    /// 1 = pinned length-form, 2 = pinned offset-form). The dual-iterator
    /// cmp formulas exceed the 6GiB cap with per-entry symbolic HAS_OFF, so
    /// those cells enumerate the choice as a case split instead: the
    /// {ff,ft,tf,tt} sub-harnesses partition the same space by construction
    /// (per-n split law, same coverage argument). Lookup cells keep the
    /// fully symbolic superset.
    static HOFF_PIN: core::sync::atomic::AtomicU8 = core::sync::atomic::AtomicU8::new(0);

    fn set_hoff_pin(v: u8) {
        HOFF_PIN.store(v, core::sync::atomic::Ordering::Relaxed);
    }

    /// JEntry: length-form, or (symbolic choice, unless pinned) HAS_OFF
    /// with the consistent cumulative end offset — the reader-side format
    /// superset.
    fn jentry(ty: u32, len: usize, end: u32) -> u32 {
        let hoff: bool = match HOFF_PIN.load(core::sync::atomic::Ordering::Relaxed) {
            0 => kani::any(),
            1 => false,
            _ => true,
        };
        ty | if hoff {
            end | JENTRY_HAS_OFF
        } else {
            len as u32
        }
    }

    /// Trusted builder: depth-1 array image [n scalars], optional raw-scalar.
    fn build_array<const C: usize>(elems: &[Scalar; MAXN], n: usize, raw: bool) -> Img<C> {
        let mut buf = Img::<C>([0u8; C]);
        let mut header = n as u32 | JB_FARRAY;
        if raw {
            header |= JB_FSCALAR;
        }
        put_u32(&mut buf, 0, header);
        let base = 4 + 4 * n;
        let mut end: u32 = 0;
        let mut pos = base;
        for i in 0..MAXN {
            if i < n {
                let l = scalar_len(&elems[i]);
                end += l as u32;
                let je = jentry(scalar_jentry_type(&elems[i]), l, end);
                put_u32(&mut buf, 4 + 4 * i, je);
                if elems[i].kind == 3 {
                    for j in 0..VL {
                        if j < elems[i].slen {
                            buf.0[pos + j] = elems[i].sbytes[j];
                        }
                    }
                    pos += elems[i].slen;
                }
            }
        }
        buf
    }

    /// One symbolic object key.
    #[derive(Clone, Copy)]
    struct Key {
        bytes: [u8; KL],
        len: usize,
    }

    fn any_key() -> Key {
        let bytes: [u8; KL] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= KL);
        Key { bytes, len }
    }

    /// Key with a PINNED length (per-length case split: symbolic stored-key
    /// lengths make every data offset symbolic; bytes stay fully symbolic).
    fn any_key_len(len: usize) -> Key {
        let bytes: [u8; KL] = kani::any();
        Key { bytes, len }
    }

    /// Scalar with pinned string length (kind stays symbolic).
    fn any_scalar_len(slen: usize) -> Scalar {
        let kind: u8 = kani::any();
        kani::assume(kind <= 3);
        let sbytes: [u8; VL] = kani::any();
        Scalar { kind, sbytes, slen }
    }

    /// Value-slot generator for search-lane object cells: `stringy` keeps
    /// the full null/bool/string kind space (value-window parity lane);
    /// otherwise null/false/true only (len-0 values, cheaper offsets — the
    /// search order is still fully exercised by the keys).
    fn any_val(stringy: bool, slen: usize) -> Scalar {
        if stringy {
            any_scalar_len(slen)
        } else {
            any_scalar(false)
        }
    }

    /// lengthCompareJsonbString strict-less (the object key sort order).
    fn lcjs_less(a: &Key, b: &Key) -> bool {
        if a.len != b.len {
            return a.len < b.len;
        }
        let mut i = 0;
        while i < a.len {
            if a.bytes[i] != b.bytes[i] {
                return a.bytes[i] < b.bytes[i];
            }
            i += 1;
        }
        false
    }

    /// Trusted builder: depth-1 object image {n sorted-key pairs}.
    fn build_object<const C: usize>(keys: &[Key; MAXN], vals: &[Scalar; MAXN], n: usize) -> Img<C> {
        // Writer invariant: keys strictly sorted (unique) per
        // lengthCompareJsonbString.
        for i in 0..MAXN - 1 {
            if i + 1 < n {
                kani::assume(lcjs_less(&keys[i], &keys[i + 1]));
            }
        }
        let mut buf = Img::<C>([0u8; C]);
        put_u32(&mut buf, 0, n as u32 | JB_FOBJECT);
        let base = 4 + 8 * n;
        let mut end: u32 = 0;
        let mut pos = base;
        // key JEntrys + key bytes
        for i in 0..MAXN {
            if i < n {
                end += keys[i].len as u32;
                let je = jentry(JENTRY_ISSTRING, keys[i].len, end);
                put_u32(&mut buf, 4 + 4 * i, je);
                for j in 0..KL {
                    if j < keys[i].len {
                        buf.0[pos + j] = keys[i].bytes[j];
                    }
                }
                pos += keys[i].len;
            }
        }
        // value JEntrys + value bytes (offsets continue across the region)
        for i in 0..MAXN {
            if i < n {
                let l = scalar_len(&vals[i]);
                end += l as u32;
                let je = jentry(scalar_jentry_type(&vals[i]), l, end);
                put_u32(&mut buf, 4 + 4 * (n + i), je);
                if vals[i].kind == 3 {
                    for j in 0..VL {
                        if j < vals[i].slen {
                            buf.0[pos + j] = vals[i].sbytes[j];
                        }
                    }
                    pos += vals[i].slen;
                }
            }
        }
        buf
    }

    /// Compare a Rust JsonbItem lookup result against C's flattened
    /// JsonbValue out-params (same input buffer, so window identity is
    /// pointer + length equality).
    fn assert_item_matches(
        c_found: c_int,
        vtype: c_int,
        vdata: *const u8,
        vlen: c_int,
        vbool: c_int,
        r: Option<JsonbItem<'_>>,
    ) {
        match r {
            None => assert!(c_found == 0),
            Some(item) => {
                assert!(c_found == 1);
                match item {
                    JsonbItem::Null => assert!(vtype == 0x0),
                    JsonbItem::String(s) => {
                        assert!(vtype == 0x1);
                        assert!(vdata == s.as_ptr());
                        assert!(vlen == s.len() as c_int);
                    }
                    JsonbItem::Numeric(s) => {
                        // out of fence; reaching it is a harness bug
                        assert!(vtype == 0x2);
                        assert!(vdata == s.as_ptr());
                    }
                    JsonbItem::Bool(b) => {
                        assert!(vtype == 0x3);
                        assert!(vbool == b as c_int);
                    }
                    JsonbItem::Binary(w) => {
                        assert!(vtype == 0x12);
                        assert!(vdata == w.as_ptr());
                        assert!(vlen == w.len() as c_int);
                    }
                    _ => panic!("composite item from a leaf lookup"),
                }
            }
        }
    }

    fn assert_no_abort() {
        assert!(unsafe { pgp_take_abort() } == 0);
    }

    /// Compare C's returned static name string against the Rust &str.
    fn assert_name_eq(cname: *const c_char, rname: &str) {
        let rb = rname.as_bytes();
        // longest name in play: "boolean" (7)
        for i in 0..8 {
            let cb = unsafe { *cname.add(i) } as u8;
            if i < rb.len() {
                assert!(cb == rb[i]);
            } else if i == rb.len() {
                assert!(cb == 0);
                break;
            }
        }
    }

    // =========== jsonb_typeof (JsonbContainerTypeName core) ===========
    //
    // Symbolic-n harnesses walled/overran (99-214s): the pair count feeds
    // every address, so the whole builder goes symbolic. Per-n case split
    // (escalation ladder step 3); the family partitions n = 0..=2, which
    // is the coverage argument (each harness pins its own cell).

    fn check_typeof_array(n: usize) {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img = build_array(&elems, n, false);
        let c = unsafe { pgp_typeof_name(img.0.as_ptr()) };
        let r = adt_jsonb::io::container_type_name(&img.0[..]);
        assert_no_abort();
        assert_name_eq(c, r);
    }

    fn check_typeof_object(n: usize) {
        unsafe { pgp_reset() };
        let keys: [Key; MAXN] = [any_key_len(1), any_key_len(2), any_key_len(2)];
        let vals: [Scalar; MAXN] = [any_scalar_len(1), any_scalar_len(2), any_scalar_len(1)];
        let img: Img = build_object(&keys, &vals, n);
        let c = unsafe { pgp_typeof_name(img.0.as_ptr()) };
        let r = adt_jsonb::io::container_type_name(&img.0[..]);
        assert_no_abort();
        assert_name_eq(c, r);
    }

    per_n! {
        eq_typeof_array_n0[9]: check_typeof_array(0);
        eq_typeof_array_n1[9]: check_typeof_array(1);
        eq_typeof_array_n2[9]: check_typeof_array(2);
        eq_typeof_object_n0[9]: check_typeof_object(0);
        eq_typeof_object_n1[9]: check_typeof_object(1);
        eq_typeof_object_n2[9]: check_typeof_object(2);
    }

    /// Raw-scalar root ("null"/"boolean"/"string" through the iterator on
    /// the C side vs extract_scalar on the Rust side).
    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_typeof_scalar() {
        unsafe { pgp_reset(); };
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img = build_array(&elems, 1, true);
        let c = unsafe { pgp_typeof_name(img.0.as_ptr()) };
        let r = adt_jsonb::io::container_type_name(&img.0[..]);
        assert_no_abort();
        assert_name_eq(c, r);
    }

    // =========== jsonb_array_length (value + error verdict) ===========

    fn check_array_length(img: &Img) {
        let mut err: c_int = 0;
        let c = unsafe { pgp_array_length(img.0.as_ptr(), &mut err) };
        let r = container::array_length(&img.0[..]);
        match r {
            Ok(v) => {
                assert!(err == 0);
                assert!(c == v);
            }
            // error verdict parity incl. WHICH ereport fired; discriminate
            // by message length ("..of a scalar" = 35, "..of a non-array"
            // = 38) — full string equality is a 35-byte memcmp that busts
            // the unwind bound for no proof value.
            Err(m) => {
                let want = if m.len() == 35 { 1 } else { 2 };
                assert!(err == want);
            }
        }
        assert_no_abort();
    }

    fn check_arraylen_array(n: usize, raw: bool) {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img = build_array(&elems, n, raw);
        check_array_length(&img);
    }

    fn check_arraylen_object(n: usize) {
        unsafe { pgp_reset() };
        let keys: [Key; MAXN] = [any_key_len(1), any_key_len(2), any_key_len(2)];
        let vals: [Scalar; MAXN] = [any_scalar_len(1), any_scalar_len(2), any_scalar_len(1)];
        let img: Img = build_object(&keys, &vals, n);
        check_array_length(&img);
    }

    per_n! {
        eq_arraylen_array_n0[5]: check_arraylen_array(0, false);
        eq_arraylen_array_n3[5]: check_arraylen_array(MAXN, false);
        eq_arraylen_scalar[5]: check_arraylen_array(1, true);
        eq_arraylen_object_n0[5]: check_arraylen_object(0);
        eq_arraylen_object_n2[5]: check_arraylen_object(2);
    }

    // ====== jsonb_object_field[_text] lookup core (-> / ->> key) ======
    //
    // Rust composition mirrors getfield::object_field minus the result
    // materialization: the is-object gate + get_key_value are the shipped
    // fns under proof.

    fn check_object_field(n: usize, kl: [usize; MAXN], vl: [usize; MAXN], vstr: bool) {
        unsafe { pgp_reset() };
        let keys: [Key; MAXN] = [any_key_len(kl[0]), any_key_len(kl[1]), any_key_len(kl[2])];
        let vals: [Scalar; MAXN] =
            [any_val(vstr, vl[0]), any_val(vstr, vl[1]), any_val(vstr, vl[2])];
        let img: Img = build_object(&keys, &vals, n);
        let probe = any_key();

        let (mut vtype, mut vlen, mut vbool): (c_int, c_int, c_int) = (0, 0, 0);
        let mut vdata: *const u8 = core::ptr::null();
        let c_found = unsafe {
            pgp_object_field(
                img.0.as_ptr(),
                probe.bytes.as_ptr(),
                probe.len as c_int,
                &mut vtype,
                &mut vdata,
                &mut vlen,
                &mut vbool,
            )
        };

        let cslice = &img.0[..];
        let r = if container::container_is_object(cslice) {
            container::get_key_value(cslice, &probe.bytes[..probe.len])
        } else {
            None
        };

        assert_no_abort();
        if n == MAXN {
            kani::cover!(r.is_some()); // binary search hit is reachable
            kani::cover!(r.is_none()); // and a real miss is too
        }
        assert_item_matches(c_found, vtype, vdata, vlen, vbool, r);
    }

    per_n! {
        eq_object_field_n0[5]: check_object_field(0, [1, 1, 1], [1, 1, 1], true);
        // n1 carries the value-window parity lane (string values included)
        eq_object_field_n1[5]: check_object_field(1, [2, 0, 0], [2, 0, 0], true);
        // equal-length keys: the memcmp tiebreak lane of the search order
        eq_object_field_n2_eqlen[5]: check_object_field(2, [2, 2, 0], [0, 0, 0], false);
        // mixed-length keys: the length-first lane
        eq_object_field_n2_mixlen[5]: check_object_field(2, [1, 2, 0], [0, 0, 0], false);
        eq_object_field_n3[6]: check_object_field(3, [2, 2, 3], [0, 0, 0], false);
    }

    // ====== jsonb_array_element lookup core (-> int) ======

    fn check_array_element(n: usize, raw: bool) {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img = build_array(&elems, n, raw);
        let element: i32 = kani::any(); // FULL i32 subscript domain

        let (mut vtype, mut vlen, mut vbool): (c_int, c_int, c_int) = (0, 0, 0);
        let mut vdata: *const u8 = core::ptr::null();
        let c_found = unsafe {
            pgp_array_element(
                img.0.as_ptr(),
                element,
                &mut vtype,
                &mut vdata,
                &mut vlen,
                &mut vbool,
            )
        };

        let cslice = &img.0[..];
        let r = if container::container_is_array(cslice) {
            match adt_jsonb::getfield::adjust_element_index(cslice, element) {
                Some(i) => container::get_ith_value(cslice, i),
                None => None,
            }
        } else {
            None
        };

        assert_no_abort();
        if n == MAXN {
            kani::cover!(element < 0 && r.is_some()); // negative-index hit
        }
        assert_item_matches(c_found, vtype, vdata, vlen, vbool, r);
    }

    per_n! {
        eq_array_element_n0[5]: check_array_element(0, false);
        eq_array_element_n1[5]: check_array_element(1, false);
        eq_array_element_n2[5]: check_array_element(2, false);
        eq_array_element_n3[5]: check_array_element(3, false);
        eq_array_element_raw[5]: check_array_element(1, true);
    }

    // =========== jsonb_exists (?) ===========

    fn check_exists_object(n: usize, kl: [usize; MAXN], vl: [usize; MAXN], vstr: bool) {
        unsafe { pgp_reset() };
        let keys: [Key; MAXN] = [any_key_len(kl[0]), any_key_len(kl[1]), any_key_len(kl[2])];
        let vals: [Scalar; MAXN] =
            [any_val(vstr, vl[0]), any_val(vstr, vl[1]), any_val(vstr, vl[2])];
        let img: Img = build_object(&keys, &vals, n);
        let probe = any_key();

        let c = unsafe { pgp_exists(img.0.as_ptr(), probe.bytes.as_ptr(), probe.len as c_int) };
        let r = adt_jsonb::ops::exists_key(&img.0[..], &probe.bytes[..probe.len]);
        assert_no_abort();
        if n == MAXN {
            kani::cover!(r);
        }
        assert!((c != 0) == r);
    }

    fn check_exists_array(n: usize, raw: bool) {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img = build_array(&elems, n, raw);
        let probe = any_key();

        let c = unsafe { pgp_exists(img.0.as_ptr(), probe.bytes.as_ptr(), probe.len as c_int) };
        let r = adt_jsonb::ops::exists_key(&img.0[..], &probe.bytes[..probe.len]);
        assert_no_abort();
        if n == MAXN {
            kani::cover!(r); // string-element hit reachable
        }
        assert!((c != 0) == r);
    }

    per_n! {
        eq_exists_object_n0[5]: check_exists_object(0, [1, 1, 1], [1, 1, 1], true);
        eq_exists_object_n1[5]: check_exists_object(1, [2, 0, 0], [2, 0, 0], true);
        eq_exists_object_n2_eqlen[5]: check_exists_object(2, [2, 2, 0], [0, 0, 0], false);
        eq_exists_object_n2_mixlen[5]: check_exists_object(2, [1, 2, 0], [0, 0, 0], false);
        eq_exists_object_n3[6]: check_exists_object(3, [2, 2, 3], [0, 0, 0], false);
        eq_exists_array_n0[5]: check_exists_array(0, false);
        eq_exists_array_n1[5]: check_exists_array(1, false);
        eq_exists_array_n2[5]: check_exists_array(2, false);
        eq_exists_array_n3[5]: check_exists_array(3, false);
        eq_exists_array_raw[5]: check_exists_array(1, true);
    }

    // =========== jsonb_cmp / eq/ne/lt/le/gt/ge (compareJsonbContainers) ===
    //
    // The shipped fc_jsonb_{cmp,eq,..} wrappers are all trivial verdict maps
    // over ops::compare_containers — the core under proof here.

    /// Rust half of the shared varstr_cmp seam model — LITERALLY the same
    /// function as C's pg_seam_varstr_cmp (memcmp byte-difference + length
    /// tiebreak). Installed via -Z stubbing; collation internals leave the
    /// proof on both sides identically.
    fn stub_varstr_cmp(a: &[u8], b: &[u8], _collid: Oid) -> types_error::PgResult<i32> {
        let n = a.len().min(b.len());
        let mut i = 0;
        while i < n {
            if a[i] != b[i] {
                return Ok(a[i] as i32 - b[i] as i32);
            }
            i += 1;
        }
        Ok(if a.len() < b.len() {
            -1
        } else if a.len() > b.len() {
            1
        } else {
            0
        })
    }

    /// Token Mcx handle for stub-backed harnesses. SOUNDNESS: with
    /// Mcx::allocate/deallocate and mcx::vec_with_capacity_in all stubbed
    /// to the static proof heap, the handle is never dereferenced by any
    /// path under proof — constructing a REAL MemoryContext is pure
    /// scaffolding cost, and a measured symex wall (probe_ctx_iter_cost
    /// with new_bump: >240s on a concrete empty array; with this token:
    /// see suite times). The zeroed image is never read.
    fn token_ctx() -> &'static mcx::MemoryContext {
        static CTX: [u8; 256] = [0u8; 256];
        assert!(core::mem::size_of::<mcx::MemoryContext>() <= 256);
        unsafe { &*(CTX.as_ptr() as *const mcx::MemoryContext) }
    }

    /// Round-2 cmp driver: the SHIPPED non-allocating core vs verbatim C.
    /// Every builder image nests <= 2 frames, so with N >= 2 abstention
    /// (None) is out of the fence — reaching it is a harness bug and the
    /// panic makes the run fail. Err is unreachable under the fence (the
    /// seam stub is total); mem::forget avoids Err drop-glue (varbit
    /// lesson) on that dead arm.
    fn run_cmp_fixed<const N: usize>(a: &Img<CMPCAP>, b: &Img<CMPCAP>) -> (i32, i32) {
        let r = adt_jsonb::ops::compare_containers_fixed::<N>(&a.0[..], &b.0[..]);
        let rv = match r {
            None => panic!("depth abstention inside the builder fence"),
            Some(Ok(v)) => v,
            Some(Err(e)) => {
                core::mem::forget(e); // avoid Err drop-glue (varbit lesson)
                panic!("compare_containers_fixed errored");
            }
        };
        let cv = unsafe {
            pgp_cmp_staged(a.0.as_ptr(), CMPCAP as c_int, b.0.as_ptr(), CMPCAP as c_int)
        };
        (rv, cv)
    }

    /// Full-walk bool cell with per-side HAS_OFF pins (1 = length-form,
    /// 2 = offset-form); see HOFF_PIN.
    fn check_cmp_arrays_bool_hoff(na: usize, nb: usize, pina: u8, pinb: u8, want_covers: bool) {
        unsafe { pgp_reset() };
        let ea: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let eb: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        set_hoff_pin(pina);
        let a: Img<CMPCAP> = build_array(&ea, na, false);
        set_hoff_pin(pinb);
        let b: Img<CMPCAP> = build_array(&eb, nb, false);
        set_hoff_pin(0);
        let (rv, cv) = run_cmp_fixed::<1>(&a, &b);
        assert_no_abort();
        if want_covers {
            kani::cover!(rv < 0);
            kani::cover!(rv == 0);
            kani::cover!(rv > 0);
            // fc_jsonb_{eq,ne,lt,le,gt,ge} verdict maps over the same core
            assert!((rv == 0) == (cv == 0));
            assert!((rv != 0) == (cv != 0));
            assert!((rv < 0) == (cv < 0));
            assert!((rv <= 0) == (cv <= 0));
            assert!((rv > 0) == (cv > 0));
            assert!((rv >= 0) == (cv >= 0));
        }
        assert!(rv == cv);
    }

    /// Structural core: arrays of null/bool (no seam at all), incl.
    /// raw-scalar vs array and length-difference order. Pinned (na, nb)
    /// cells (same case-split rationale as per_n).
    fn check_cmp_arrays_bool(na: usize, rawa: bool, nb: usize, rawb: bool, want_covers: bool) {
        unsafe { pgp_reset() };
        let ea: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let eb: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let a: Img<CMPCAP> = build_array(&ea, na, rawa);
        let b: Img<CMPCAP> = build_array(&eb, nb, rawb);
        let (rv, cv) = run_cmp_fixed::<1>(&a, &b);
        assert_no_abort();
        if want_covers {
            kani::cover!(rv < 0);
            kani::cover!(rv == 0);
            kani::cover!(rv > 0);
            // cross-type rank rule reachable in-theorem: null(jbv 0) sorts
            // below bool(jbv 3) by the type-defined order branch
            kani::cover!(ea[0].kind == 0 && eb[0].kind != 0 && rv < 0);
            kani::cover!(ea[0].kind != 0 && eb[0].kind == 0 && rv > 0);
            // fc_jsonb_{eq,ne,lt,le,gt,ge} are these verdict maps over the
            // same core result (builtins.rs cmp_args)
            assert!((rv == 0) == (cv == 0));
            assert!((rv != 0) == (cv != 0));
            assert!((rv < 0) == (cv < 0));
            assert!((rv <= 0) == (cv <= 0));
            assert!((rv > 0) == (cv > 0));
            assert!((rv >= 0) == (cv >= 0));
        }
        assert!(rv == cv);
    }

    /// String scalars in arrays: exercises the shared varstr_cmp seam model
    /// on both sides (magnitude included — jsonb_cmp's int4 is SQL-visible).
    fn check_cmp_arrays_string(na: usize, nb: usize) {
        unsafe { pgp_reset() };
        let ea: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let eb: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let a: Img<CMPCAP> = build_array(&ea, na, false);
        let b: Img<CMPCAP> = build_array(&eb, nb, false);
        let (rv, cv) = run_cmp_fixed::<1>(&a, &b);
        assert_no_abort();
        kani::cover!(rv == 0);
        assert!(rv == cv);
    }

    /// Raw-scalar roots over the FULL fenced kind space (null/bool/string):
    /// the compareJsonbContainers cross-type rank ordering
    /// (jbvNull=0 < jbvString=1 < jbvBool=3) witnessed per direction.
    fn check_cmp_raw_scalar_crosstype() {
        unsafe { pgp_reset() };
        let ea: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let eb: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let a: Img<CMPCAP> = build_array(&ea, 1, true);
        let b: Img<CMPCAP> = build_array(&eb, 1, true);
        let (rv, cv) = run_cmp_fixed::<1>(&a, &b);
        assert_no_abort();
        // harness kinds: 0 null, 1 false, 2 true, 3 string
        kani::cover!(ea[0].kind == 0 && eb[0].kind == 3 && rv < 0); // null < string
        kani::cover!(ea[0].kind == 3 && (eb[0].kind == 1 || eb[0].kind == 2) && rv < 0); // string < bool
        kani::cover!((ea[0].kind == 1 || ea[0].kind == 2) && eb[0].kind == 0 && rv > 0); // bool > null
        kani::cover!(rv == 0);
        assert!(rv == cv);
    }

    /// Object-vs-object (keys through the seam model; nPairs ordering) and
    /// object-vs-array roots (type-defined order).
    fn check_cmp_object_vs(na: usize, nb: usize, b_is_obj: bool) {
        unsafe { pgp_reset() };
        let ka: [Key; MAXN] = [any_key_len(1), any_key_len(2), any_key_len(2)];
        let va: [Scalar; MAXN] = [any_scalar_len(1), any_scalar_len(2), any_scalar_len(1)];
        let a: Img<CMPCAP> = build_object(&ka, &va, na);
        let kb: [Key; MAXN] = [any_key_len(1), any_key_len(2), any_key_len(2)];
        let vb: [Scalar; MAXN] = [any_scalar_len(1), any_scalar_len(2), any_scalar_len(1)];
        let b: Img<CMPCAP> = if b_is_obj {
            build_object(&kb, &vb, nb)
        } else {
            build_array(&vb, nb, false)
        };
        let (rv, cv) = run_cmp_fixed::<1>(&a, &b);
        assert_no_abort();
        if b_is_obj && na == nb {
            kani::cover!(rv == 0);
        }
        if !b_is_obj {
            // root rank rule: jbvObject(0x11) > jbvArray(0x10), always
            kani::cover!(rv > 0);
        }
        assert!(rv == cv);
    }

    // ---- depth-2 nesting (slice 3): nested container elements ----

    /// Trusted builder: depth-2 array image. Element 0 is a
    /// JENTRY_ISCONTAINER child (a depth-1 array of `n_inner` null/bool
    /// scalars, optionally raw-scalar), elements 1..n are null/bool
    /// scalars. Child data offset 0 is already 4-aligned (base = 4 + 4n),
    /// so INTALIGN padding is zero and the JEntry length equals the child
    /// image size — the same layout convertJsonbArray emits for a first
    /// container element.
    fn build_array_nested<const C: usize>(n: usize, n_inner: usize, inner_raw: bool) -> Img<C> {
        let ie: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let child = build_array::<C>(&ie, n_inner, inner_raw);
        let clen = 4 + 4 * n_inner; // null/bool elems carry no data bytes

        let mut buf = Img::<C>([0u8; C]);
        put_u32(&mut buf, 0, n as u32 | JB_FARRAY);
        let base = 4 + 4 * n;
        let end: u32 = clen as u32;
        put_u32(&mut buf, 4, jentry(JENTRY_ISCONTAINER, clen, end));
        for j in 0..clen {
            buf.0[base + j] = child.0[j];
        }
        for i in 1..MAXN {
            if i < n {
                let s = any_scalar(false);
                // len-0 scalars: the cumulative end offset stays at clen
                let je = jentry(scalar_jentry_type(&s), 0, end);
                put_u32(&mut buf, 4 + 4 * i, je);
            }
        }
        buf
    }

    /// Depth-2 vs depth-2: both roots are [ <container>, bools.. ].
    fn check_cmp_nested(na: usize, ia: usize, nb: usize, ib: usize) {
        unsafe { pgp_reset() };
        let a: Img<CMPCAP> = build_array_nested(na, ia, false);
        let b: Img<CMPCAP> = build_array_nested(nb, ib, false);
        let (rv, cv) = run_cmp_fixed::<2>(&a, &b);
        assert_no_abort();
        if na == nb && ia == ib {
            kani::cover!(rv == 0);
            kani::cover!(rv != 0);
        }
        assert!(rv == cv);
    }

    /// Depth-2 vs depth-1: element 0 container meets element 0 scalar —
    /// the iterator recurses on one side only (ra != rb arm, Array > scalar
    /// by the rank rule).
    fn check_cmp_nested_vs_flat() {
        unsafe { pgp_reset() };
        let a: Img<CMPCAP> = build_array_nested(2, 1, false);
        let eb: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let b: Img<CMPCAP> = build_array(&eb, 2, false);
        let (rv, cv) = run_cmp_fixed::<2>(&a, &b);
        assert_no_abort();
        kani::cover!(rv > 0);
        assert!(rv == cv);
    }

    /// The depth-abstention contract itself (not a parity cell): on a
    /// depth-2 input, fixed::<1> abstains (None, pure — production falls
    /// back to the allocating walk) while fixed::<2> on the SAME input is
    /// Some and equals C.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(varlena::varstr_cmp, stub_varstr_cmp)]
    #[kani::stub(adt_numeric::cmp_numerics, stub_cmp_numerics)]
    fn eq_cmp_abstention_witness() {
        unsafe { pgp_reset() };
        let a: Img<CMPCAP> = build_array_nested(1, 1, false);
        let b: Img<CMPCAP> = build_array_nested(1, 1, false);
        assert!(adt_jsonb::ops::compare_containers_fixed::<1>(&a.0[..], &b.0[..]).is_none());
        let (rv, cv) = run_cmp_fixed::<2>(&a, &b);
        assert_no_abort();
        assert!(rv == cv);
    }

    /// REACHABILITY CUT (out-of-fence arm): compare_scalar's jbvNumeric arm
    /// is builder-dead (scalar kinds are null/bool/string; JENTRY_ISNUMERIC
    /// is never built), but symbolic execution cannot prove that and drags
    /// adt_numeric's digit-loop machinery into every cmp formula (measured:
    /// cmp_abs_common unwinding in a CONCRETE bool-only cell). The stub
    /// removes it from the model; parity on the arm is NOT claimed — the C
    /// side's numeric arm sets the abort sentinel, so any input that really
    /// reached it would fail assert_no_abort.
    fn stub_cmp_numerics(
        _a: adt_numeric::Num<'_>,
        _b: adt_numeric::Num<'_>,
    ) -> i32 {
        0
    }

    /// cmp harness cells: every cell stubs the two out-of-fence scalar arms
    /// (numeric = reachability cut above; string = the shared varstr_cmp
    /// seam model, semantic for string cells and a reachability cut for
    /// bool-only cells whose builder never emits strings — the REAL
    /// varstr_cmp reaches collation/locale machinery that walls symex).
    macro_rules! cmp_case {
        ($($name:ident[$unwind:literal]: $check:ident($($arg:expr),*);)*) => {$(
            #[kani::proof]
            #[kani::unwind($unwind)]
            #[kani::stub(varlena::varstr_cmp, stub_varstr_cmp)]
            #[kani::stub(adt_numeric::cmp_numerics, stub_cmp_numerics)]
            fn $name() { $check($($arg),*); }
        )*};
    }

    /// Alias kept for the harness table: string cells (same stub set; the
    /// seam model is load-bearing here, see control_cmp_seam_skew).
    macro_rules! cmp_case_str {
        ($($name:ident[$unwind:literal]: $check:ident($($arg:expr),*);)*) => {
            cmp_case! { $($name[$unwind]: $check($($arg),*);)* }
        };
    }

    cmp_case! {
        eq_cmp_bool_0_0[4]: check_cmp_arrays_bool(0, false, 0, false, false);
        eq_cmp_bool_1_1[5]: check_cmp_arrays_bool(1, false, 1, false, false);
        eq_cmp_bool_2_2[8]: check_cmp_arrays_bool(2, false, 2, false, true);
        eq_cmp_bool_1_2[8]: check_cmp_arrays_bool(1, false, 2, false, false);
        eq_cmp_bool_raw_vs_arr[7]: check_cmp_arrays_bool(1, true, 1, false, false);
        eq_cmp_bool_raw_raw[7]: check_cmp_arrays_bool(1, true, 1, true, false);
        // C quirk cell: empty top-level array vs raw scalar (the no-else
        // rawScalar/nElems override in compareJsonbContainers)
        eq_cmp_bool_raw_vs_empty[7]: check_cmp_arrays_bool(1, true, 0, false, false);
        // full-walk 1_1 partitioned over per-side HAS_OFF (see HOFF_PIN)
        eq_cmp_bool_1_1_ff[5]: check_cmp_arrays_bool_hoff(1, 1, 1, 1, true);
        eq_cmp_bool_1_1_ft[5]: check_cmp_arrays_bool_hoff(1, 1, 1, 2, false);
        eq_cmp_bool_1_1_tf[5]: check_cmp_arrays_bool_hoff(1, 1, 2, 1, false);
        eq_cmp_bool_1_1_tt[5]: check_cmp_arrays_bool_hoff(1, 1, 2, 2, false);
        eq_cmp_bool_2_2_ff[6]: check_cmp_arrays_bool_hoff(2, 2, 1, 1, true);
        eq_cmp_bool_2_2_ft[6]: check_cmp_arrays_bool_hoff(2, 2, 1, 2, false);
        eq_cmp_bool_2_2_tf[6]: check_cmp_arrays_bool_hoff(2, 2, 2, 1, false);
        eq_cmp_bool_2_2_tt[6]: check_cmp_arrays_bool_hoff(2, 2, 2, 2, false);
        // depth-2 cells (slice 3, n<=2 outer and inner)
        eq_cmp_nested_1i1_1i1[10]: check_cmp_nested(1, 1, 1, 1);
        eq_cmp_nested_1i1_1i2[10]: check_cmp_nested(1, 1, 1, 2);
        eq_cmp_nested_2i2_2i2[12]: check_cmp_nested(2, 2, 2, 2);
        eq_cmp_nested_vs_flat[10]: check_cmp_nested_vs_flat();
    }

    cmp_case_str! {
        eq_cmp_string_1_1[7]: check_cmp_arrays_string(1, 1);
        eq_cmp_string_2_2[8]: check_cmp_arrays_string(2, 2);
        eq_cmp_raw_scalar_crosstype[7]: check_cmp_raw_scalar_crosstype();
        eq_cmp_obj_obj_0_0[6]: check_cmp_object_vs(0, 0, true);
        eq_cmp_obj_obj_1_1[8]: check_cmp_object_vs(1, 1, true);
        eq_cmp_obj_obj_2_2[9]: check_cmp_object_vs(2, 2, true);
        eq_cmp_obj_obj_1_2[9]: check_cmp_object_vs(1, 2, true);
        eq_cmp_obj_arr_1_1[8]: check_cmp_object_vs(1, 1, false);
    }

    // ---- cmp controls (DEFAULT solver, MUST FAIL) ----

    /// Rig negative control: Rust compares (b, a) while C compares (a, b)
    /// on length-mismatched bool arrays — any nonzero result is a
    /// counterexample. Proves the round-2 cmp rig is non-vacuous.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(varlena::varstr_cmp, stub_varstr_cmp)]
    #[kani::stub(adt_numeric::cmp_numerics, stub_cmp_numerics)]
    fn control_cmp_swapped() {
        unsafe { pgp_reset() };
        let ea: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let eb: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let a: Img<CMPCAP> = build_array(&ea, 1, false);
        let b: Img<CMPCAP> = build_array(&eb, 2, false);
        let rv = match adt_jsonb::ops::compare_containers_fixed::<1>(&b.0[..], &a.0[..]) {
            Some(Ok(v)) => v,
            _ => panic!("unreachable under the fence"),
        };
        let cv = unsafe {
            pgp_cmp_staged(a.0.as_ptr(), CMPCAP as c_int, b.0.as_ptr(), CMPCAP as c_int)
        };
        assert!(rv == cv);
    }

    /// Seam-skew control: the Rust side runs a SKEWED varstr_cmp model
    /// (length tiebreak inverted) against C's straight seam model — the
    /// harness must find a string pair exposing it. Proves the shared seam
    /// model is load-bearing, not decorative.
    fn stub_varstr_cmp_skew(a: &[u8], b: &[u8], _collid: Oid) -> types_error::PgResult<i32> {
        let n = a.len().min(b.len());
        let mut i = 0;
        while i < n {
            if a[i] != b[i] {
                return Ok(a[i] as i32 - b[i] as i32);
            }
            i += 1;
        }
        Ok(if a.len() < b.len() {
            1 // SKEW: shorter sorts HIGHER
        } else if a.len() > b.len() {
            -1
        } else {
            0
        })
    }

    #[kani::proof]
    #[kani::unwind(7)]
    #[kani::stub(varlena::varstr_cmp, stub_varstr_cmp_skew)]
    #[kani::stub(adt_numeric::cmp_numerics, stub_cmp_numerics)]
    fn control_cmp_seam_skew() {
        check_cmp_arrays_string(1, 1);
    }

    /// Scaffolding cost probe (not a parity claim): context + iterator
    /// init + one token on a CONCRETE empty array. Isolates whether the cmp
    /// walls are mcx/iterator machinery or the compare loop itself.
    #[kani::proof]
    #[kani::unwind(4)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
    fn probe_ctx_iter_cost() {
        let mut buf = Img([0u8; CAP]);
        put_u32(&mut buf, 0, JB_FARRAY); // empty array, all concrete
        let ctx: &'static mcx::MemoryContext = token_ctx();
        let mut it = adt_jsonb::iter::JsonbIterator::init(ctx.mcx(), &buf.0[..]).unwrap();
        let (r, _) = it.next(false);
        assert!(r == adt_jsonb::iter::WjbToken::BeginArray);
    }

    /// Cost probes (not parity claims): bisect round-2 symex cost.
    /// probe_fixed_cost_core: fixed core alone, concrete empty arrays.
    #[kani::proof]
    #[kani::unwind(4)]
    #[kani::stub(varlena::varstr_cmp, stub_varstr_cmp)]
    #[kani::stub(adt_numeric::cmp_numerics, stub_cmp_numerics)]
    fn probe_fixed_cost_core() {
        let mut a = Img::<CMPCAP>([0u8; CMPCAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let mut b = Img::<CMPCAP>([0u8; CMPCAP]);
        put_u32(&mut b, 0, JB_FARRAY);
        let r = adt_jsonb::ops::compare_containers_fixed::<1>(&a.0[..], &b.0[..]);
        assert!(matches!(r, Some(Ok(0))));
    }

    /// probe_fixed_cost_c: C side alone, concrete empty arrays.
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_fixed_cost_c() {
        unsafe { pgp_reset() };
        let mut a = Img::<CMPCAP>([0u8; CMPCAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let mut b = Img::<CMPCAP>([0u8; CMPCAP]);
        put_u32(&mut b, 0, JB_FARRAY);
        let cv = unsafe {
            pgp_cmp_staged(a.0.as_ptr(), CMPCAP as c_int, b.0.as_ptr(), CMPCAP as c_int)
        };
        assert_no_abort();
        assert!(cv == 0);
    }

    /// probe_fixed_cost_builder: builder(n=0) + fixed core, no C.
    #[kani::proof]
    #[kani::unwind(4)]
    #[kani::stub(varlena::varstr_cmp, stub_varstr_cmp)]
    #[kani::stub(adt_numeric::cmp_numerics, stub_cmp_numerics)]
    fn probe_fixed_cost_builder() {
        let ea: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let eb: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let a: Img<CMPCAP> = build_array(&ea, 0, false);
        let b: Img<CMPCAP> = build_array(&eb, 0, false);
        let r = adt_jsonb::ops::compare_containers_fixed::<2>(&a.0[..], &b.0[..]);
        assert!(matches!(r, Some(Ok(0))));
    }

    /// probe_r1: fixed iterator init + ONE token, concrete empty array.
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_r1() {
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let mut it = adt_jsonb::iter::FixedJsonbIterator::<1>::init(&a.0[..]);
        let r = it.next(false);
        assert!(matches!(r, Some((adt_jsonb::iter::WjbToken::BeginArray, _))));
    }

    /// probe_r2: full token drain, concrete empty array, no C.
    #[kani::proof]
    #[kani::unwind(5)]
    fn probe_r2() {
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let mut it = adt_jsonb::iter::FixedJsonbIterator::<1>::init(&a.0[..]);
        let mut n = 0u32;
        loop {
            match it.next(false) {
                Some((adt_jsonb::iter::WjbToken::Done, _)) => break,
                Some(_) => n += 1,
                None => panic!("abstained"),
            }
        }
        assert!(n == 2);
    }

    /// probe_r3: THREE straight-line next() calls, no harness loop.
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_r3() {
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let mut it = adt_jsonb::iter::FixedJsonbIterator::<1>::init(&a.0[..]);
        let r1 = it.next(false);
        assert!(matches!(r1, Some((adt_jsonb::iter::WjbToken::BeginArray, _))));
        let r2 = it.next(false);
        assert!(matches!(r2, Some((adt_jsonb::iter::WjbToken::EndArray, _))));
        let r3 = it.next(false);
        assert!(matches!(r3, Some((adt_jsonb::iter::WjbToken::Done, _))));
    }

    /// probe_w1: copy_from_slice write, header read back.
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_w1() {
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        assert!(container::container_header(&a.0[..]) == JB_FARRAY);
    }

    /// probe_w2: per-byte writes, header read back.
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_w2() {
        let mut a = Img([0u8; CAP]);
        let v = JB_FARRAY.to_ne_bytes();
        a.0[0] = v[0];
        a.0[1] = v[1];
        a.0[2] = v[2];
        a.0[3] = v[3];
        assert!(container::container_header(&a.0[..]) == JB_FARRAY);
    }

    /// probe_w3: per-byte writes + three straight-line nexts (r3 variant).
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_w3() {
        let mut a = Img([0u8; CAP]);
        let v = JB_FARRAY.to_ne_bytes();
        a.0[0] = v[0];
        a.0[1] = v[1];
        a.0[2] = v[2];
        a.0[3] = v[3];
        let mut it = adt_jsonb::iter::FixedJsonbIterator::<1>::init(&a.0[..]);
        let r1 = it.next(false);
        assert!(matches!(r1, Some((adt_jsonb::iter::WjbToken::BeginArray, _))));
        let r2 = it.next(false);
        assert!(matches!(r2, Some((adt_jsonb::iter::WjbToken::EndArray, _))));
        let r3 = it.next(false);
        assert!(matches!(r3, Some((adt_jsonb::iter::WjbToken::Done, _))));
    }

    /// probe_w4: TWO nexts, results inspected.
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_w4() {
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let mut it = adt_jsonb::iter::FixedJsonbIterator::<1>::init(&a.0[..]);
        let r1 = it.next(false);
        assert!(matches!(r1, Some((adt_jsonb::iter::WjbToken::BeginArray, _))));
        let r2 = it.next(false);
        assert!(matches!(r2, Some((adt_jsonb::iter::WjbToken::EndArray, _))));
    }

    /// probe_w5: TWO nexts, results dropped unexamined.
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_w5() {
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let mut it = adt_jsonb::iter::FixedJsonbIterator::<1>::init(&a.0[..]);
        let _ = it.next(false);
        let r2 = it.next(false);
        assert!(r2.is_some());
    }

    /// C iterator per-call scaling probes (concrete empty array).
    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_c1() {
        unsafe { pgp_reset() };
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let acc = unsafe { pgp_iter_probe(a.0.as_ptr(), 1) };
        assert!(acc == 4); // WJB_BEGIN_ARRAY
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn probe_c3() {
        unsafe { pgp_reset() };
        let mut a = Img([0u8; CAP]);
        put_u32(&mut a, 0, JB_FARRAY);
        let acc = unsafe { pgp_iter_probe(a.0.as_ptr(), 3) };
        assert!(acc == 450); // BEGIN(4), END(5), DONE(0)
    }

    // =========== negative control (DEFAULT solver, MUST FAIL) ===========

    /// C sees a one-byte-shorter probe key than Rust: any object whose
    /// lookup verdicts then differ is a counterexample. Proves the rig can
    /// fail (non-vacuous gate).
    #[kani::proof]
    #[kani::unwind(5)]
    fn control_object_field_short_key() {
        unsafe { pgp_reset() };
        let n: usize = 1; // pinned: one pair suffices for the mismatch
        let keys: [Key; MAXN] = [any_key_len(2), any_key_len(1), any_key_len(1)];
        let vals: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let img: Img = build_object(&keys, &vals, n);
        let mut probe = any_key_len(2);
        probe.len = 2;

        let (mut vtype, mut vlen, mut vbool): (c_int, c_int, c_int) = (0, 0, 0);
        let mut vdata: *const u8 = core::ptr::null();
        let c_found = unsafe {
            pgp_object_field(
                img.0.as_ptr(),
                probe.bytes.as_ptr(),
                (probe.len - 1) as c_int, // deliberate mismatch
                &mut vtype,
                &mut vdata,
                &mut vlen,
                &mut vbool,
            )
        };
        let r = container::get_key_value(&img.0[..], &probe.bytes[..probe.len]);
        assert!((c_found != 0) == r.is_some());
    }

    // ======================================================================
    // WAVE 2 (2026-07-30): gin_compare_jsonb (3480) + the scalar-cast rows
    // 3449 jsonb_numeric / 3450-3452 jsonb_int2/4/8 / 2580+3453
    // jsonb_float8/float4.  C side: c/pg_gin_cmp.c and c/pg_jsonb_casts.c
    // (run with `--c-lib c/pg_jsonb_casts.c --c-lib c/pg_gin_cmp.c` added;
    // the casts TU links against pg_jsonb.c's pg_JsonbExtractScalar).
    //
    // Cast-rig fence: raw-scalar jsonb images whose sole element is a
    // NUMERIC leaf built by build_raw_numeric (4-byte varlena header +
    // short/long packed numeric header + digits < NBASE, the writer
    // invariant), or the existing null/bool/string/array/object builders
    // for the error-class cells.  Result materialization (byref_result /
    // Datum packing) is out of scope; claims are value-space + error-CLASS
    // (sqlstate) parity, message text out of proof (stub_pg_error_error /
    // stub_format).  DigitBuf: inline-capacity allocator model (heap arm
    // panics if reached — nd fences keep it unreachable), pool return
    // forgotten (numeric-probe N4 recipe).
    // ======================================================================

    use proof_support::stubs;

    extern "C" {
        fn pg_gin_compare_jsonb(a: *const u8, la: c_int, b: *const u8, lb: c_int) -> c_int;
        fn pgp_casts_reset() -> c_int;
        fn pgp_casts_take_abort() -> c_int;
        fn pgp_jsonb_numeric(
            c: *const u8,
            errclass: *mut c_int,
            errtype: *mut c_int,
            vdata: *mut *const u8,
            vlen: *mut c_int,
        ) -> c_int;
        fn pgp_jsonb_int8(
            c: *const u8,
            errclass: *mut c_int,
            errtype: *mut c_int,
            out: *mut i64,
        ) -> c_int;
        fn pgp_jsonb_int4(
            c: *const u8,
            errclass: *mut c_int,
            errtype: *mut c_int,
            out: *mut i32,
        ) -> c_int;
        fn pgp_jsonb_int2(
            c: *const u8,
            errclass: *mut c_int,
            errtype: *mut c_int,
            out: *mut i16,
        ) -> c_int;
        fn pgp_jsonb_float8_special(
            c: *const u8,
            errclass: *mut c_int,
            errtype: *mut c_int,
            bits: *mut u64,
        ) -> c_int;
        fn pgp_jsonb_float4_special(
            c: *const u8,
            errclass: *mut c_int,
            errtype: *mut c_int,
            bits: *mut u32,
        ) -> c_int;
    }

    // ---- 3480 gin_compare_jsonb ----
    //
    // Core-level (ptr,len) pairs per the varlena-comparator pattern (fmgr
    // text unwrap stays in the tested tier, bytea-cmp precedent).  EXACT
    // int32 value asserted: CBMC's memcmp model returns the first
    // mismatching byte difference — the glibc convention the shipped
    // varstrfastcmp_c mirrors (text-cmp eq_bttextcmp precedent).

    const GINCAP: usize = 8;

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_gin_compare_jsonb() {
        let a: [u8; GINCAP] = kani::any();
        let b: [u8; GINCAP] = kani::any();
        let la: usize = kani::any();
        let lb: usize = kani::any();
        kani::assume(la <= GINCAP);
        kani::assume(lb <= GINCAP);
        let c = unsafe {
            pg_gin_compare_jsonb(a.as_ptr(), la as c_int, b.as_ptr(), lb as c_int)
        };
        let r = adt_jsonb::gin::gin_compare_jsonb(&a[..la], &b[..lb]);
        kani::cover!(r < 0);
        kani::cover!(r == 0);
        kani::cover!(r > 0);
        kani::cover!(r != 0 && la == lb); // memcmp mismatch arm
        kani::cover!(r != 0 && la != lb); // length tiebreak arm
        assert!(c == r);
    }

    // ---- cast rig ----

    /// numeric-leaf image cap (max cell: w4n5 short = 8 + 6 + 10 = 24).
    const CASTCAP: usize = 32;

    const NBASE: i16 = 10000;
    const NUMERIC_POS: u16 = 0x0000;
    const NUMERIC_NEG: u16 = 0x4000;
    const NUMERIC_SHORT: u16 = 0x8000;
    const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
    const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
    const NUMERIC_SHORT_DSCALE_MAX: u16 = 0x1F80 >> 7;
    const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
    const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
    const NUM_NAN: u16 = 0xC000;
    const NUM_PINF: u16 = 0xD000;
    const NUM_NINF: u16 = 0xF000;

    const NDMAX: usize = 5;

    fn put_u16<const C: usize>(buf: &mut Img<C>, off: usize, v: u16) {
        buf.0[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    }

    /// Trusted builder: raw-scalar jsonb image whose sole element is a
    /// numeric leaf. `hdr_words`: 1 (short form) or 2 (long form: sign|dscale
    /// then weight). Digits are the caller's (fenced 0..NBASE there).
    /// Element data offset is 0 (base = 8), so INTALIGN padding is zero and
    /// the JEntry length equals the numeric image size — the writer layout
    /// for a raw-scalar numeric root.
    fn build_raw_numeric<const C: usize>(
        w0: u16,
        w1: i16,
        long_form: bool,
        digits: &[i16; NDMAX],
        nd: usize,
    ) -> Img<C> {
        let numlen = 4 + 2 + if long_form { 2 } else { 0 } + 2 * nd;
        let mut buf = Img::<C>([0u8; C]);
        put_u32(&mut buf, 0, 1u32 | JB_FARRAY | JB_FSCALAR);
        put_u32(
            &mut buf,
            4,
            jentry(JENTRY_ISNUMERIC, numlen, numlen as u32),
        );
        // 4-byte varlena header (varattrib_4b LE word, len<<2)
        put_u32(&mut buf, 8, (numlen as u32) << 2);
        put_u16(&mut buf, 12, w0);
        let mut pos = 14;
        if long_form {
            put_u16(&mut buf, 14, w1 as u16);
            pos = 16;
        }
        for i in 0..NDMAX {
            if i < nd {
                put_u16(&mut buf, pos + 2 * i, digits[i] as u16);
            }
        }
        buf
    }

    /// Symbolic short-form numeric cell at LITERAL weight/nd: sign and
    /// dscale symbolic, digits symbolic fenced to the on-disk invariant
    /// 0 <= d < NBASE.
    fn any_short_numeric(weight: i32, nd: usize) -> Img<CASTCAP> {
        set_hoff_pin(1); // length-form JEntry cells (offset-form reader lane
                         // covered by the lookup family + window_n1_of)
        let neg: bool = kani::any();
        let dscale: u16 = kani::any();
        kani::assume(dscale <= NUMERIC_SHORT_DSCALE_MAX);
        let mut digits = [0i16; NDMAX];
        for d in digits.iter_mut().take(nd) {
            let v: i16 = kani::any();
            kani::assume(v >= 0 && v < NBASE);
            *d = v;
        }
        let wbits = ((weight as u16) & NUMERIC_SHORT_WEIGHT_MASK)
            | if weight < 0 { NUMERIC_SHORT_WEIGHT_SIGN_MASK } else { 0 };
        let hdr = NUMERIC_SHORT
            | if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 }
            | (dscale << NUMERIC_SHORT_DSCALE_SHIFT)
            | wbits;
        let img = build_raw_numeric(hdr, 0, false, &digits, nd);
        set_hoff_pin(0);
        img
    }

    /// Long-form cell (verbatim long header: sign|dscale word + weight word).
    fn any_long_numeric(weight: i16, nd: usize) -> Img<CASTCAP> {
        let neg: bool = kani::any();
        let dscale: u16 = kani::any();
        kani::assume(dscale <= 0x3FFF && dscale <= 100);
        let mut digits = [0i16; NDMAX];
        for d in digits.iter_mut().take(nd) {
            let v: i16 = kani::any();
            kani::assume(v >= 0 && v < NBASE);
            *d = v;
        }
        let hdr = if neg { NUMERIC_NEG } else { NUMERIC_POS } | dscale;
        set_hoff_pin(1);
        let img = build_raw_numeric(hdr, weight, true, &digits, nd);
        set_hoff_pin(0);
        img
    }

    /// Special (NaN/+Inf/-Inf) numeric cell at a LITERAL header (a symbolic
    /// selector keeps the finite numeric_out+float8in arm structurally in
    /// the formula — assume never folds; the literal prunes it.  The three
    /// per-header cells partition the special lattice by construction).
    fn special_numeric(hdr: u16) -> Img<CASTCAP> {
        set_hoff_pin(1); // literal JEntry (symbolic HAS_OFF keeps the numeric
                         // window symbolic and the finite arm live)
        let img = build_raw_numeric(hdr, 0, false, &[0i16; NDMAX], 0);
        set_hoff_pin(0);
        img
    }

    /// Rust-side value-space cast composition (the jsonb_numeric_cast macro
    /// body minus fcinfo/mcx result packing, which stays in the tested
    /// tier): shipped cast_numeric_image + the shipped numeric conversion.
    enum CastR<T> {
        Null,
        Val(T),
        Err(types_error::SqlState),
    }

    fn take_err<T>(e: Box<types_error::PgError>) -> CastR<T> {
        let s = e.sqlstate;
        core::mem::forget(e); // avoid Box<PgError> drop glue (varbit lesson)
        CastR::Err(s)
    }

    fn rust_cast<T>(
        payload: &[u8],
        sqltype: &'static str,
        conv: impl Fn(adt_numeric::Num<'_>) -> types_error::PgResult<T>,
    ) -> CastR<T> {
        match adt_jsonb::builtins::cast_numeric_image(payload, sqltype) {
            Ok(None) => CastR::Null,
            Ok(Some(img)) => match conv(adt_numeric::Num::from_payload(&img[4..])) {
                Ok(v) => CastR::Val(v),
                Err(e) => take_err(e),
            },
            Err(e) => take_err(e),
        }
    }

    /// sqlstate expected for each C error class (SHIM C2 mapping).
    fn class_sqlstate(errclass: c_int) -> types_error::SqlState {
        match errclass {
            1 => types_error::ERRCODE_INVALID_PARAMETER_VALUE,
            2 => types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            _ => types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        }
    }

    fn casts_reset() {
        unsafe {
            pgp_reset();
            pgp_casts_reset();
        }
    }

    fn assert_no_abort_casts() {
        assert!(unsafe { pgp_take_abort() } == 0);
        assert!(unsafe { pgp_casts_take_abort() } == 0);
    }

    macro_rules! int_cast_check {
        ($check:ident, $pgp:ident, $sqltype:literal, $conv:path, $cty:ty) => {
            fn $check(img: &Img<CASTCAP>, want_overflow_covers: bool) {
                let (mut ec, mut et): (c_int, c_int) = (0, 0);
                let mut out: $cty = 0;
                let c = unsafe { $pgp(img.0.as_ptr(), &mut ec, &mut et, &mut out) };
                let r = rust_cast(&img.0[..], $sqltype, |n| $conv(n));
                assert_no_abort_casts();
                if want_overflow_covers {
                    kani::cover!(matches!(r, CastR::Val(_)));
                    kani::cover!(matches!(r, CastR::Err(_)));
                }
                match r {
                    CastR::Null => assert!(c == 0),
                    CastR::Val(v) => {
                        assert!(c == 1);
                        assert!(out == v);
                    }
                    CastR::Err(s) => {
                        assert!(c == 2);
                        assert!(s == class_sqlstate(ec));
                    }
                }
            }
        };
    }

    int_cast_check!(check_jsonb_int8, pgp_jsonb_int8, "bigint", adt_numeric::numeric_int8, i64);
    int_cast_check!(check_jsonb_int4, pgp_jsonb_int4, "integer", adt_numeric::numeric_int4, i32);
    int_cast_check!(
        check_jsonb_int2,
        pgp_jsonb_int2,
        "smallint",
        adt_numeric::numeric_int2,
        i16
    );

    /// Kani stub for `adt_numeric::var::digit_buf_heap_realloc` (N4 recipe):
    /// the plane fences ndigits within INLINE_DIGITS, so reaching the heap
    /// arm is a harness defect — panic loudly, never a silent fence.
    fn stub_digit_buf_heap_realloc(_heap: &mut Vec<i16>, _n: usize) {
        panic!("DigitBuf heap arm reached under the inline nd fence");
    }

    /// Kani stub for `adt_numeric::var::digit_buf_put`: drop-time pool
    /// return out of proof.
    fn stub_digit_buf_put(v: Vec<i16>) {
        core::mem::forget(v);
    }

    /// Cast-cell harness generator: per-(weight, nd) LITERAL cells (the
    /// case-split law: assumes never fold; each cell pins its plane and the
    /// family partitions the short-form weight/nd grid — per_n coverage
    /// argument).  All cast stubs: message machinery (stub_pg_error_error /
    /// stub_format — sqlstate stays shipped and IS asserted) + the DigitBuf
    /// inline-capacity model.
    macro_rules! cast_case {
        ($($name:ident[$unwind:literal]: $check:ident($($arg:expr),*);)*) => {$(
            #[kani::proof]
            #[kani::unwind($unwind)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(adt_numeric::var::digit_buf_heap_realloc, stub_digit_buf_heap_realloc)]
            #[kani::stub(adt_numeric::var::digit_buf_put, stub_digit_buf_put)]
            fn $name() { $check($($arg),*); }
        )*};
    }

    // int cells: literal (weight, nd) planes.  w0n0 = zero; wm1n1 = pure
    // fraction (rounding to 0/±1); w0n2/w1n2 = fractional-digit rounding
    // lanes; w4n5/w4n1 = the "treat stripped digits as real" lane at big
    // weights (int2/int4 range-error arms live from w1n2 up; int8's
    // overflow arm needs w4+ which its own cells carry).
    fn cell_int8(weight: i32, nd: usize, covers: bool) {
        casts_reset();
        let img = any_short_numeric(weight, nd);
        check_jsonb_int8(&img, covers);
    }
    fn cell_int4(weight: i32, nd: usize, covers: bool) {
        casts_reset();
        let img = any_short_numeric(weight, nd);
        check_jsonb_int4(&img, covers);
    }
    fn cell_int2(weight: i32, nd: usize, covers: bool) {
        casts_reset();
        let img = any_short_numeric(weight, nd);
        check_jsonb_int2(&img, covers);
    }
    fn cell_int8_long(weight: i16, nd: usize) {
        casts_reset();
        let img = any_long_numeric(weight, nd);
        check_jsonb_int8(&img, false);
    }

    cast_case! {
        eq_jsonb_int8_w0n0[6]: cell_int8(0, 0, false);
        eq_jsonb_int8_w0n1[12]: cell_int8(0, 1, false);
        eq_jsonb_int8_w0n2[12]: cell_int8(0, 2, false);
        eq_jsonb_int8_wm1n1[12]: cell_int8(-1, 1, false);
        eq_jsonb_int8_w1n2[12]: cell_int8(1, 2, false);
        eq_jsonb_int8_w4n5[14]: cell_int8(4, 5, true);
        eq_jsonb_int8_w5n1[14]: cell_int8(5, 1, true);
        eq_jsonb_int8_long_w0n1[12]: cell_int8_long(0, 1);
        eq_jsonb_int4_w0n1[12]: cell_int4(0, 1, false);
        eq_jsonb_int4_w0n2[12]: cell_int4(0, 2, false);
        eq_jsonb_int4_wm1n1[12]: cell_int4(-1, 1, false);
        eq_jsonb_int4_w2n3[13]: cell_int4(2, 3, true);
        eq_jsonb_int2_w0n1[12]: cell_int2(0, 1, false);
        eq_jsonb_int2_wm1n1[12]: cell_int2(-1, 1, false);
        eq_jsonb_int2_w1n2[12]: cell_int2(1, 2, true);
    }

    // ---- special-value lattice (int class-2 errors; float NaN/±Inf) ----

    fn cell_int8_special(hdr: u16) {
        casts_reset();
        let img = special_numeric(hdr);
        check_jsonb_int8(&img, false);
    }
    fn cell_int4_special(hdr: u16) {
        casts_reset();
        let img = special_numeric(hdr);
        check_jsonb_int4(&img, false);
    }
    fn cell_int2_special(hdr: u16) {
        casts_reset();
        let img = special_numeric(hdr);
        check_jsonb_int2(&img, false);
    }

    /// 2580/3453 fenced plane: the specials lattice (NaN/+Inf/-Inf) with
    /// bit-exact results, plus the shared null/error classes in the kinds
    /// cells below.  The finite arm (numeric_out + strtod cascade) is out
    /// of fence on both sides (C sets the abort sentinel; the ledger row
    /// records the wall).
    fn cell_float8_special(hdr: u16) {
        casts_reset();
        let img = special_numeric(hdr);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut bits: u64 = 0;
        let c = unsafe { pgp_jsonb_float8_special(img.0.as_ptr(), &mut ec, &mut et, &mut bits) };
        let r = rust_cast(&img.0[..], "double precision", adt_numeric::numeric_float8);
        assert_no_abort_casts();
        match r {
            CastR::Val(v) => {
                assert!(c == 1);
                assert!(v.to_bits() == bits);
            }
            _ => panic!("special lattice cell left the value arm"),
        }
    }

    fn cell_float4_special(hdr: u16) {
        casts_reset();
        let img = special_numeric(hdr);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut bits: u32 = 0;
        let c = unsafe { pgp_jsonb_float4_special(img.0.as_ptr(), &mut ec, &mut et, &mut bits) };
        let r = rust_cast(&img.0[..], "real", adt_numeric::numeric_float4);
        assert_no_abort_casts();
        match r {
            CastR::Val(v) => {
                assert!(c == 1);
                assert!(v.to_bits() == bits);
            }
            _ => panic!("special lattice cell left the value arm"),
        }
    }

    cast_case! {
        eq_jsonb_int8_special_nan[6]: cell_int8_special(NUM_NAN);
        eq_jsonb_int8_special_pinf[6]: cell_int8_special(NUM_PINF);
        eq_jsonb_int8_special_ninf[6]: cell_int8_special(NUM_NINF);
        eq_jsonb_int4_special_nan[6]: cell_int4_special(NUM_NAN);
        eq_jsonb_int2_special_nan[6]: cell_int2_special(NUM_NAN);
        eq_jsonb_float8_special_nan[6]: cell_float8_special(NUM_NAN);
        eq_jsonb_float8_special_pinf[6]: cell_float8_special(NUM_PINF);
        eq_jsonb_float8_special_ninf[6]: cell_float8_special(NUM_NINF);
        eq_jsonb_float4_special_nan[6]: cell_float4_special(NUM_NAN);
        eq_jsonb_float4_special_pinf[6]: cell_float4_special(NUM_PINF);
        eq_jsonb_float4_special_ninf[6]: cell_float4_special(NUM_NINF);
    }

    // ---- 3449 jsonb_numeric: image-window identity + shared error class --

    /// Window identity: same input buffer, so slice identity is pointer +
    /// length equality (materialization/copy out of scope, SHIM C3).
    fn cell_numeric_window(nd: usize, hoff: u8) {
        casts_reset();
        set_hoff_pin(hoff);
        let neg: bool = kani::any();
        let dscale: u16 = kani::any();
        kani::assume(dscale <= NUMERIC_SHORT_DSCALE_MAX);
        let mut digits = [0i16; NDMAX];
        for d in digits.iter_mut().take(nd) {
            let v: i16 = kani::any();
            kani::assume(v >= 0 && v < NBASE);
            *d = v;
        }
        let hdr = NUMERIC_SHORT | if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 }
            | (dscale << NUMERIC_SHORT_DSCALE_SHIFT);
        let img: Img<CASTCAP> = build_raw_numeric(hdr, 0, false, &digits, nd);
        set_hoff_pin(0);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut vlen: c_int = -1;
        let mut vdata: *const u8 = core::ptr::null();
        let c = unsafe {
            pgp_jsonb_numeric(img.0.as_ptr(), &mut ec, &mut et, &mut vdata, &mut vlen)
        };
        // shipped cast_numeric_image directly: the claim is the image
        // window (ptr + len) itself — same input buffer on both sides.
        let r = adt_jsonb::builtins::cast_numeric_image(&img.0[..], "numeric");
        assert_no_abort_casts();
        match r {
            Ok(Some(s)) => {
                assert!(c == 1);
                assert!(vdata == s.as_ptr());
                assert!(vlen == s.len() as c_int);
            }
            Ok(None) => panic!("numeric window cell left the value arm"),
            Err(e) => {
                core::mem::forget(e);
                panic!("numeric window cell errored");
            }
        }
    }

    /// Shared error/null classes for ALL cast rows: raw-scalar root over the
    /// full null/bool/string kind space through the jsonb_numeric flow (the
    /// same shipped cast_numeric_image + verbatim C flow every cast row
    /// shares; per-row sqltype only changes message text, which is out of
    /// proof).  null -> SQL NULL both sides; bool/string -> class-1 error
    /// with C's jbvType matching the builder kind.
    fn cell_cast_scalar_kinds() {
        casts_reset();
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img<CASTCAP> = build_array(&elems, 1, true);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut vlen: c_int = -1;
        let mut vdata: *const u8 = core::ptr::null();
        let c = unsafe {
            pgp_jsonb_numeric(img.0.as_ptr(), &mut ec, &mut et, &mut vdata, &mut vlen)
        };
        let r = rust_cast(&img.0[..], "numeric", |_| Ok(()));
        assert_no_abort_casts();
        kani::cover!(matches!(r, CastR::Null));
        kani::cover!(matches!(r, CastR::Err(_)));
        match r {
            CastR::Null => {
                assert!(c == 0);
                assert!(elems[0].kind == 0);
            }
            CastR::Err(s) => {
                assert!(c == 2);
                assert!(s == class_sqlstate(ec));
                // C jbvType parity with the builder kind (string=1, bool=3)
                assert!(et == if elems[0].kind == 3 { 1 } else { 3 });
            }
            CastR::Val(_) => panic!("no numeric leaf in this cell"),
        }
    }

    /// Not-a-scalar error class: array root (n symbolic would wall the
    /// builder; n literal per the per_n law — n=2 exercises the false-arm
    /// type report jbvArray).
    fn cell_cast_err_array(n: usize) {
        casts_reset();
        let elems: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let img: Img<CASTCAP> = build_array(&elems, n, false);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut vlen: c_int = -1;
        let mut vdata: *const u8 = core::ptr::null();
        let c = unsafe {
            pgp_jsonb_numeric(img.0.as_ptr(), &mut ec, &mut et, &mut vdata, &mut vlen)
        };
        let r = rust_cast(&img.0[..], "numeric", |_| Ok(()));
        assert_no_abort_casts();
        match r {
            CastR::Err(s) => {
                assert!(c == 2);
                assert!(s == class_sqlstate(ec));
                assert!(et == 0x10); // jbvArray
            }
            _ => panic!("array root must be a cast error"),
        }
    }

    fn cell_cast_err_object(n: usize) {
        casts_reset();
        let keys: [Key; MAXN] = [any_key_len(1), any_key_len(2), any_key_len(2)];
        let vals: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let img: Img<CASTCAP> = build_object(&keys, &vals, n);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut vlen: c_int = -1;
        let mut vdata: *const u8 = core::ptr::null();
        let c = unsafe {
            pgp_jsonb_numeric(img.0.as_ptr(), &mut ec, &mut et, &mut vdata, &mut vlen)
        };
        let r = rust_cast(&img.0[..], "numeric", |_| Ok(()));
        assert_no_abort_casts();
        match r {
            CastR::Err(s) => {
                assert!(c == 2);
                assert!(s == class_sqlstate(ec));
                assert!(et == 0x11); // jbvObject
            }
            _ => panic!("object root must be a cast error"),
        }
    }

    /// int-row representative of the shared error flow (macro-identical
    /// composition across int2/4/float rows; sqltype only feeds message
    /// text).
    fn cell_int8_scalar_kinds() {
        casts_reset();
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img<CASTCAP> = build_array(&elems, 1, true);
        check_jsonb_int8(&img, false);
    }

    cast_case! {
        eq_jsonb_numeric_window_n0[6]: cell_numeric_window(0, 1);
        eq_jsonb_numeric_window_n1[7]: cell_numeric_window(1, 1);
        eq_jsonb_numeric_window_n1_of[7]: cell_numeric_window(1, 2);
        eq_jsonb_numeric_window_n2[8]: cell_numeric_window(2, 1);
        eq_jsonb_cast_scalar_kinds[7]: cell_cast_scalar_kinds();
        eq_jsonb_cast_err_array_n2[7]: cell_cast_err_array(2);
        eq_jsonb_cast_err_object_n1[7]: cell_cast_err_object(1);
        eq_jsonb_int8_scalar_kinds[7]: cell_int8_scalar_kinds();
    }

    // ---- 3217 jsonb_extract_path (#>): path-resolution verdict ----
    //
    // Rust target: shipped getfield::resolve_path (the jsonb_get_element
    // walk factored out; materialization + the fc-level null-element
    // pre-screen of get_jsonb_path_all stay in the tested tier).  C:
    // c/pg_jsonb_path.c pgp_get_element (verbatim walk; strtol -> total
    // C-locale model, SHIM P3).  Path-element fence: no NUL bytes (SQL text
    // cannot contain them; the shipped parse stops at the first NUL exactly
    // as TextDatumGetCString does, and the C model is handed a NUL-free
    // buffer).  0x0B is IN the symbolic alphabet: the walk now parses
    // subscripts with pg_string::strtoint10_strict, so the VT case that once
    // diverged is covered by these EQ cells and pinned by the standing
    // witness witness_path_vt_subscript below.

    extern "C" {
        fn pgp_path_take_abort() -> c_int;
        fn pgp_get_element(
            c: *const u8,
            paths: *const *const u8,
            plens: *const c_int,
            npath: c_int,
            vtype: *mut c_int,
            vdata: *mut *const u8,
            vlen: *mut c_int,
            vbool: *mut c_int,
        ) -> c_int;
    }

    fn run_get_element(img: &[u8], path: &[&[u8]]) -> (c_int, c_int, *const u8, c_int, c_int) {
        let mut ptrs = [core::ptr::null::<u8>(); 2];
        let mut lens = [0 as c_int; 2];
        for (i, p) in path.iter().enumerate() {
            ptrs[i] = p.as_ptr();
            lens[i] = p.len() as c_int;
        }
        let (mut vtype, mut vlen, mut vbool): (c_int, c_int, c_int) = (0, 0, 0);
        let mut vdata: *const u8 = core::ptr::null();
        let c = unsafe {
            pgp_get_element(
                img.as_ptr(),
                ptrs.as_ptr(),
                lens.as_ptr(),
                path.len() as c_int,
                &mut vtype,
                &mut vdata,
                &mut vlen,
                &mut vbool,
            )
        };
        assert!(unsafe { pgp_path_take_abort() } == 0);
        (c, vtype, vdata, vlen, vbool)
    }

    fn check_get_element(img: &[u8], path: &[&[u8]]) {
        let (c, vtype, vdata, vlen, vbool) = run_get_element(img, path);
        match adt_jsonb::getfield::resolve_path(img, path) {
            adt_jsonb::getfield::PathVerdict::Null => assert!(c == 0),
            adt_jsonb::getfield::PathVerdict::Input => assert!(c == 2),
            adt_jsonb::getfield::PathVerdict::Item(it) => {
                assert!(c == 1);
                assert_item_matches(1, vtype, vdata, vlen, vbool, Some(it));
            }
        }
    }

    /// Symbolic path-element text, fenced: no NUL (SQL text cannot carry
    /// one).  Every other byte, VT (0x0B) included, is in the alphabet.
    fn any_path_elem(bytes: &mut [u8; 8], cap: usize) -> usize {
        let len: usize = kani::any();
        kani::assume(len <= cap);
        for b in bytes.iter_mut().take(cap) {
            let v: u8 = kani::any();
            kani::assume(v != 0);
            *b = v;
        }
        len
    }

    fn cell_path_obj_n2_len1() {
        unsafe { pgp_reset() };
        let keys: [Key; MAXN] = [any_key_len(1), any_key_len(2), any_key_len(2)];
        let vals: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let img: Img<CMPCAP> = build_object(&keys, &vals, 2);
        let probe = any_key();
        check_get_element(&img.0[..], &[&probe.bytes[..probe.len]]);
    }

    /// Per-length case split (assumes never fold; literal len prunes the
    /// parse circuit).  hoff_pin: 0 symbolic, 1/2 pinned length/offset form.
    fn cell_path_arr_sub_len(sl: usize, hoff_pin: u8) {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        set_hoff_pin(hoff_pin);
        let img: Img<CMPCAP> = build_array(&elems, 2, false);
        set_hoff_pin(0);
        let mut sub = [0u8; 8];
        for b in sub.iter_mut().take(sl) {
            let v: u8 = kani::any();
            kani::assume(v != 0);
            *b = v;
        }
        check_get_element(&img.0[..], &[&sub[..sl]]);
    }

    /// Subscript-parse regime witnesses for the array cell (hoisted).
    fn cover_path_arr_subscr() {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let img: Img<CMPCAP> = build_array(&elems, 2, false);
        let mut sub = [0u8; 8];
        let sl = any_path_elem(&mut sub, 4);
        let (c, _, _, _, _) = run_get_element(&img.0[..], &[&sub[..sl]]);
        kani::cover!(c == 1 && sub[0] == b'-'); // negative subscript hit
        kani::cover!(c == 1 && sub[0] == b' '); // leading-whitespace parse
        kani::cover!(c == 1 && sub[0] == 0x0b); // VT is C-locale whitespace
        kani::cover!(c == 1 && sub[0] == b'+'); // explicit plus sign
        kani::cover!(c == 0 && sl == 2 && sub[0] == b'1'); // trailing junk null
        kani::cover!(c == 0 && sl == 0); // empty string null
    }

    /// i32-overflow subscript lane (11 symbolic digits > INT_MAX: C strtoint
    /// flags ERANGE, Rust flags the i32 range check — both null).
    fn cell_path_arr_bigsub() {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let img: Img<CMPCAP> = build_array(&elems, 2, false);
        let mut sub = [0u8; 12];
        let first: u8 = kani::any();
        kani::assume(first >= b'1' && first <= b'9');
        sub[0] = first;
        for b in sub.iter_mut().skip(1) {
            let v: u8 = kani::any();
            kani::assume(v.is_ascii_digit());
            *b = v;
        }
        let (c, _, _, _, _) = run_get_element(&img.0[..], &[&sub[..]]);
        assert!(c == 0);
        match adt_jsonb::getfield::resolve_path(&img.0[..], &[&sub[..]]) {
            adt_jsonb::getfield::PathVerdict::Null => {}
            _ => panic!("11-digit subscript must be null on both sides"),
        }
    }

    fn cell_path_scalar_root_len1() {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img<CMPCAP> = build_array(&elems, 1, true);
        let mut sub = [0u8; 8];
        let sl = any_path_elem(&mut sub, 2);
        check_get_element(&img.0[..], &[&sub[..sl]]);
    }

    fn cell_path_empty_arr() {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(false), any_scalar(false), any_scalar(false)];
        let img: Img<CMPCAP> = build_array(&elems, 1, false);
        check_get_element(&img.0[..], &[]);
    }

    fn cell_path_empty_scalar() {
        unsafe { pgp_reset() };
        let elems: [Scalar; MAXN] = [any_scalar(true), any_scalar(true), any_scalar(true)];
        let img: Img<CMPCAP> = build_array(&elems, 1, true);
        check_get_element(&img.0[..], &[]);
    }

    /// Depth-2 walk: outer array [ <child array>, bool ], path len 2 —
    /// first subscript must resolve the container element, second recurses.
    fn cell_path_nested_len2() {
        unsafe { pgp_reset() };
        let img: Img<CMPCAP> = build_array_nested(2, 2, false);
        let mut s1 = [0u8; 8];
        let l1 = any_path_elem(&mut s1, 2);
        let mut s2 = [0u8; 8];
        let l2 = any_path_elem(&mut s2, 2);
        check_get_element(&img.0[..], &[&s1[..l1], &s2[..l2]]);
    }

    per_n! {
        eq_path_obj_n2_len1[6]: cell_path_obj_n2_len1();
        eq_path_arr_sub_len1_lf[8]: cell_path_arr_sub_len(1, 1);
        eq_path_arr_sub_len1_of[8]: cell_path_arr_sub_len(1, 2);
        eq_path_arr_sub_len2_lf[8]: cell_path_arr_sub_len(2, 1);
        eq_path_arr_sub_len2_of[8]: cell_path_arr_sub_len(2, 2);
        eq_path_arr_sub_len3_lf[8]: cell_path_arr_sub_len(3, 1);
        eq_path_arr_sub_len0[8]: cell_path_arr_sub_len(0, 1);
        cover_path_arr_subscr_regimes[8]: cover_path_arr_subscr();
        eq_path_arr_bigsub[14]: cell_path_arr_bigsub();
        eq_path_scalar_root_len1[6]: cell_path_scalar_root_len1();
        eq_path_empty_arr[6]: cell_path_empty_arr();
        eq_path_empty_scalar[7]: cell_path_empty_scalar();
        eq_path_nested_len2[16]: cell_path_nested_len2();
    }

    // ---- 3272/3274 jsonb_build_array_noargs / jsonb_build_object_noargs --
    //
    // Constant empty-container images through the SHIPPED build pipeline
    // (JsonbPush + convert_to_jsonb) vs the verbatim C writer subset
    // (c/pg_jsonb_build.c).  Full varlena image byte-compared.  Rust
    // allocation via token ctx + mcx stubs (allocation strategy out of
    // scope); fcinfo/variadic-extraction stays in the tested tier.

    extern "C" {
        fn pgp_build_take_abort() -> c_int;
        fn pgp_build_array_noargs(out: *mut u8, outcap: c_int) -> c_int;
        fn pgp_build_object_noargs(out: *mut u8, outcap: c_int) -> c_int;
    }

    fn check_build_noargs(object: bool) {
        let mut out = [0u8; 16];
        let clen = unsafe {
            if object {
                pgp_build_object_noargs(out.as_mut_ptr(), 16)
            } else {
                pgp_build_array_noargs(out.as_mut_ptr(), 16)
            }
        };
        assert!(unsafe { pgp_build_take_abort() } == 0);
        let ctx: &'static mcx::MemoryContext = token_ctx();
        let r = if object {
            adt_jsonb::tojsonb::jsonb_build_object_worker(ctx.mcx(), &[], &[], &[], false, false)
        } else {
            adt_jsonb::tojsonb::jsonb_build_array_worker(ctx.mcx(), &[], &[], &[], false)
        };
        let v = match r {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("noargs builder errored");
            }
        };
        assert!(clen >= 0);
        assert!(clen as usize == v.len());
        let vs: &[u8] = &v;
        for i in 0..16 {
            if i < vs.len() {
                assert!(out[i] == vs[i]);
            }
        }
        core::mem::forget(v);
    }

    macro_rules! build_case {
        ($($name:ident[$unwind:literal]: $check:ident($($arg:expr),*);)*) => {$(
            #[kani::proof]
            #[kani::unwind($unwind)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() { $check($($arg),*); }
        )*};
    }

    build_case! {
        eq_build_array_noargs[8]: check_build_noargs(false);
        eq_build_object_noargs[8]: check_build_noargs(true);
    }

    /// STANDING REGRESSION WITNESS (must PASS — it asserts the two sides
    /// AGREE): subscript text "\x0b1" over a 2-element array.  C-locale
    /// strtol skips '\v' as whitespace and resolves index 1; the shipped
    /// walk must resolve the SAME element.
    ///
    /// History: this harness originally asserted the DIVERGENCE — the walk
    /// parsed subscripts with `trim_ascii_start()`, whose whitespace set
    /// omits VT (0x0B), so a VT-prefixed subscript that C accepts returned
    /// SQL NULL.  Ground-truthed against PostgreSQL 18.4 (glibc) and FIXED
    /// by routing the parse through `pg_string::strtoint10_strict`, an exact
    /// model of C's strtoint.  Flipped to assert equivalence and kept as a
    /// permanent witness: reintroducing ANY Rust-whitespace-set
    /// approximation at the subscript parse drives the PathVerdict::Null arm
    /// below against a C side that found the element, and this harness FAILS.
    #[kani::proof]
    #[kani::unwind(8)]
    fn witness_path_vt_subscript() {
        unsafe { pgp_reset() };
        // fully concrete instance (a witness needs ONE case): false,false
        // elements, length-form JEntrys.
        let e = Scalar { kind: 1, sbytes: [0u8; VL], slen: 0 };
        let elems: [Scalar; MAXN] = [e, e, e];
        set_hoff_pin(1);
        let img: Img<CMPCAP> = build_array(&elems, 2, false);
        set_hoff_pin(0);
        let sub: [u8; 2] = [0x0b, b'1'];
        let (c, vtype, vdata, vlen, vbool) = run_get_element(&img.0[..], &[&sub[..]]);
        // The C side must FIND the element.  Anti-vacuity: if the C model
        // ever stopped resolving VT subscripts the equivalence claim below
        // would be satisfiable by both sides returning NULL, which is
        // exactly the bug this witness exists to catch.
        assert!(c == 1); // C: '\v' skipped, index 1 found
        match adt_jsonb::getfield::resolve_path(&img.0[..], &[&sub[..]]) {
            adt_jsonb::getfield::PathVerdict::Item(it) => {
                assert_item_matches(1, vtype, vdata, vlen, vbool, Some(it));
            }
            adt_jsonb::getfield::PathVerdict::Null => {
                panic!("VT-prefixed subscript regressed to SQL NULL; C resolves it")
            }
            adt_jsonb::getfield::PathVerdict::Input => {
                panic!("non-empty path cannot yield the input datum")
            }
        }
    }

    /// Concrete diagnostic probe (w0n1 counterexample instance: short-form
    /// negative, dscale 0, digit 1 => value -1).  Separates the sides.
    #[kani::proof]
    #[kani::unwind(7)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(adt_numeric::var::digit_buf_heap_realloc, stub_digit_buf_heap_realloc)]
    #[kani::stub(adt_numeric::var::digit_buf_put, stub_digit_buf_put)]
    fn probe_int8_concrete_neg1() {
        casts_reset();
        set_hoff_pin(1);
        let digits = [8192i16, 0, 0, 0, 0];
        // hdr = SHORT | NEG-sign | dscale=63<<7 (the second counterexample)
        let img: Img<CASTCAP> = build_raw_numeric(0xBF80, 0, false, &digits, 1);
        set_hoff_pin(0);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut out: i64 = 0;
        let c = unsafe { pgp_jsonb_int8(img.0.as_ptr(), &mut ec, &mut et, &mut out) };
        assert_no_abort_casts();
        assert!(c == 1);
        assert!(out == -8192); // C side
        let r = rust_cast(&img.0[..], "bigint", adt_numeric::numeric_int8);
        match r {
            CastR::Val(v) => assert!(v == -8192), // Rust side
            _ => panic!("rust side left value arm"),
        }
    }

    /// Symbolic diagnostic for the w0n1 fabrication: both sides asserted
    /// against the analytic spec (value = ±digits[0] at weight 0, nd 1).
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(adt_numeric::var::digit_buf_heap_realloc, stub_digit_buf_heap_realloc)]
    #[kani::stub(adt_numeric::var::digit_buf_put, stub_digit_buf_put)]
    fn probe_int8_w0n1_spec() {
        casts_reset();
        set_hoff_pin(1);
        let neg: bool = kani::any();
        let dscale: u16 = kani::any();
        kani::assume(dscale <= NUMERIC_SHORT_DSCALE_MAX);
        let d0: i16 = kani::any();
        kani::assume(d0 >= 0 && d0 < NBASE);
        let digits = [d0, 0, 0, 0, 0];
        let hdr = NUMERIC_SHORT
            | if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 }
            | (dscale << NUMERIC_SHORT_DSCALE_SHIFT);
        let img: Img<CASTCAP> = build_raw_numeric(hdr, 0, false, &digits, 1);
        set_hoff_pin(0);
        let expected: i64 = if neg { -(d0 as i64) } else { d0 as i64 };
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut out: i64 = 0;
        let c = unsafe { pgp_jsonb_int8(img.0.as_ptr(), &mut ec, &mut et, &mut out) };
        assert_no_abort_casts();
        assert!(c == 1);
        assert!(out == expected); // C vs spec
        let r = rust_cast(&img.0[..], "bigint", adt_numeric::numeric_int8);
        match r {
            CastR::Val(v) => assert!(v == expected), // Rust vs spec
            _ => panic!("rust side left value arm"),
        }
    }

    extern "C" {
        fn pgp_probe_numeric_decode(
            c: *const u8,
            sign: *mut c_int,
            weight: *mut c_int,
            dsc: *mut c_int,
            nd: *mut c_int,
            d0: *mut c_int,
        ) -> c_int;
    }

    /// C-side decode diagnostic: the C view of the symbolic embedded
    /// numeric must equal the builder's fields.
    #[kani::proof]
    #[kani::unwind(12)]
    fn probe_int8_w0n1_cdecode() {
        casts_reset();
        set_hoff_pin(1);
        let neg: bool = kani::any();
        let dscale: u16 = kani::any();
        kani::assume(dscale <= NUMERIC_SHORT_DSCALE_MAX);
        let d0: i16 = kani::any();
        kani::assume(d0 >= 0 && d0 < NBASE);
        let digits = [d0, 0, 0, 0, 0];
        let hdr = NUMERIC_SHORT
            | if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 }
            | (dscale << NUMERIC_SHORT_DSCALE_SHIFT);
        let img: Img<CASTCAP> = build_raw_numeric(hdr, 0, false, &digits, 1);
        set_hoff_pin(0);
        let (mut sign, mut weight, mut dsc, mut nd, mut cd0): (c_int, c_int, c_int, c_int, c_int) =
            (0, 0, 0, 0, 0);
        let ok = unsafe {
            pgp_probe_numeric_decode(
                img.0.as_ptr(),
                &mut sign,
                &mut weight,
                &mut dsc,
                &mut nd,
                &mut cd0,
            )
        };
        assert!(ok == 1);
        assert!(sign == if neg { 0x4000 } else { 0 });
        assert!(weight == 0);
        assert!(dsc == dscale as c_int);
        assert!(nd == 1);
        assert!(cd0 == d0 as c_int);
    }

    /// Negative control (DEFAULT solver, MUST FAIL): C reads a
    /// weight-skewed image (weight+1 in the header the C side sees) on a
    /// plane where the value parity is otherwise provable — the int8 cell
    /// must find a counterexample.  Proves the cast rig is non-vacuous.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(adt_numeric::var::digit_buf_heap_realloc, stub_digit_buf_heap_realloc)]
    #[kani::stub(adt_numeric::var::digit_buf_put, stub_digit_buf_put)]
    fn control_cast_weight_skew() {
        casts_reset();
        // Rust reads weight 0, C reads weight 1 (skewed header bit assembly)
        let neg: bool = kani::any();
        let mut digits = [0i16; NDMAX];
        let v: i16 = kani::any();
        kani::assume(v >= 1 && v < NBASE);
        digits[0] = v;
        let hdr_r = NUMERIC_SHORT | if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 };
        let hdr_c = hdr_r | 1; // weight bits skewed: C sees weight 1
        let img_r = build_raw_numeric::<CASTCAP>(hdr_r, 0, false, &digits, 1);
        let img_c = build_raw_numeric::<CASTCAP>(hdr_c, 0, false, &digits, 1);
        let (mut ec, mut et): (c_int, c_int) = (0, 0);
        let mut out: i64 = 0;
        let c = unsafe { pgp_jsonb_int8(img_c.0.as_ptr(), &mut ec, &mut et, &mut out) };
        let r = rust_cast(&img_r.0[..], "bigint", adt_numeric::numeric_int8);
        match r {
            CastR::Val(v) => {
                assert!(c == 1);
                assert!(out == v);
            }
            _ => panic!("plane stays in the value arm"),
        }
    }
}
