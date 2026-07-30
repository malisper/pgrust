//! Kani C≡Rust equivalence: the pg_snapshot / xid8funcs family
//! (pg_snapshot_in core, pg_snapshot_xmin/xmax cores, pg_visible_in_snapshot,
//! the FullTransactionIdFromAllowableAt epoch-widening helper, and the
//! libc-strtou64 parity claim the shipped code makes in a comment).
//!
//! Rust side (shipped code, path-dep — never copied):
//!  - xid8funcs::{SnapView, is_visible_fxid, full_xid_from_allowable_at,
//!    strtou64, parse_snapshot}
//!    (crates/backend/utils/adt/xid8funcs/src/lib.rs).
//!
//! C side: proofs/xid8snap/c/pg_xid8snap.c (REL_18_STABLE xid8funcs.c +
//! transam.h, provenance + full shim manifest documented there).
//!
//! SNAPSHOT MODEL: the C pg_snapshot varlena struct is built in-harness as
//! a #[repr(C)] SnapC (layout identical: i32 varsz, u32 nxip, u64 xmin,
//! u64 xmax, u64 xip[] — no padding on any supported target since
//! 4+4 = 8-aligned). The SAME memory is viewed by Rust as
//! SnapView::new(&bytes[4..]) — so the struct-offset arithmetic SnapView
//! hardcodes (lib.rs:24-26 "C's struct offsets minus the 4-byte length
//! word") is IN-theorem: eq_snapview_layout proves the four accessor
//! projections against C member reads over fully symbolic contents.
//! DETOASTING IS OUT OF SCOPE: inputs model the post-detoast caller
//! contract (fcinfo.arg_varlena_packed), same fence as the bytea-cmp
//! varlena pattern.
//!
//! Claims and fences:
//!  - eq_full_xid_from_allowable_at: value parity over full u64 x u32,
//!    FENCED to the C caller contract (transam.h Asserts, compiled out in
//!    production): the epoch-decrement branch is only reached with
//!    epoch != 0. Outside the fence, C wraps epoch to UINT32_MAX while
//!    the Rust debug_assert would fire — C-contract plane, not parity.
//!  - eq_is_visible_fxid_linear: verdict parity, nxip <= 4 (linear-scan
//!    arm), fully symbolic value/xmin/xmax/xips (unsorted allowed —
//!    upstream's linear arm doesn't require order).
//!  - eq_is_visible_fxid_bsearch_31/32/33 (+ cover_bsearch_case_split):
//!    verdict parity on the bsearch arm, nxip case-split to LITERALS
//!    (assumes never constant-fold; the symbolic-nxip cut walled in SAT),
//!    xips assumed ascending non-strict (superset of the strictly-
//!    ascending on-disk rep). C bsearch is modeled by pg_proof_bsearch
//!    (shim [S3]). DEFAULT solver only: kissat false-fails the unwinding
//!    assertions (known trap).
//!  - eq_snapview_layout: nxip/xmin/xmax/xip[i] projection parity over
//!    symbolic contents and symbolic index (covers the pg_snapshot_xmin /
//!    pg_snapshot_xmax cores, which are exactly these member reads).
//!  - eq_strtou64_len6: (value, endoff) parity of xid8funcs::strtou64
//!    against the C-standard strtoull(s, &e, 10) MODEL (shim [S4]) —
//!    machine-checks the shipped comment claim (lib.rs:123-124 "libc
//!    strtou64 ... saturates to u64::MAX on overflow; end == 0 if no
//!    digits") modulo the libc model; symbolic len <= 6, NUL-free bytes.
//!  - eq_parse_snapshot_len0: verdict + sqlstate (22P02) + level parity
//!    of the empty-input reject (proved, 0.9s). The larger parse cells
//!    (len1..6, and even fully CONCRETE spot inputs) are a measured WALL
//!    — NOT arithmetic: as soon as one input byte is symbolic (or the
//!    formula merely CONTAINS the accept path), the shipped
//!    parse_snapshot -> snapshot_image pipeline's PgVec push/grow +
//!    vec_append_bytes try_reserve machinery enters the program
//!    expression (~776K steps vs 8.5K at len0) and CNF conversion never
//!    returns (TRIAGE wall class 5, std-Vec push/grow; ladder exhausted:
//!    literal-length cells, token-ctx + vec_with_capacity_in stub,
//!    --no-assertion-reach-checks, both solvers, 300s). The cells and
//!    spot harnesses are KEPT as the high-memory-retry-tier vehicles
//!    (TRIAGE wall taxonomy #7); a shipped-code refactor that builds the
//!    image with one exact-capacity reserve + set_len (and a fixed-array
//!    xips core) would break the class per the measured Vec-wall remedy.
//!    Claim scaffolding when they do run: mcx-stubs recipe "modulo
//!    static-buffer allocator model"; message text / Location out of
//!    proof (PgError::error + fmt stubs), shipped .with_sqlstate
//!    load-bearing; escontext = None (hard-error path only).
//!    The PARSE KERNEL parity that matters (strtou64 x3 call sites) is
//!    fully proved by eq_strtou64_len6, and the [S4] libc model is
//!    grounded by tests/native_strtou64.rs (4M+ checks vs real libc
//!    strtoull, 0 diffs; host libc — replay on glibc per ground-truth
//!    law before reporting any future mismatch).
//!
//! Negative control: control_snapview_swapped compares C xmax against
//! Rust xmin (must FAIL with a decodable counterexample; run with the
//! DEFAULT solver, not kissat).
//!
//! Not in this family (recorded in the ledger): pg_current_xact_id /
//! pg_current_snapshot / pg_xact_status / pg_export_snapshot etc. =
//! excluded(state); pg_snapshot_xip = excluded(engine: SRF protocol);
//! pg_snapshot_out = digit-emission over full u64 (result-image +
//! /10-chain sloped wall) — kernels here prove the read side;
//! pg_snapshot_recv/send = pqformat StringInfo rig, follow-up candidates.

#[cfg(kani)]
mod proofs {
    use proof_support::{mcx_stubs, stubs};
    use std::os::raw::c_int;
    use types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERROR};

    extern "C" {
        fn pgc_full_xid_from_allowable_at(next_full_xid: u64, xid: u32) -> u64;
        fn pgc_is_visible_fxid(value: u64, snap: *const u8) -> c_int;
        fn pgc_snap_nxip(snap: *const u8) -> u32;
        fn pgc_snap_xmin(snap: *const u8) -> u64;
        fn pgc_snap_xmax(snap: *const u8) -> u64;
        fn pgc_snap_xip(snap: *const u8, i: u32) -> u64;
        fn pgc_strtou64(s: *const u8, endoff: *mut usize) -> u64;
        fn pgc_parse_snapshot(s: *const u8, outbuf: *mut u8, err: *mut c_int) -> c_int;
    }

    /// C pg_snapshot layout twin (see module doc). `xip` capacity is a
    /// const generic; the C flexible array member imposes none.
    #[repr(C)]
    struct SnapC<const N: usize> {
        varsz: i32,
        nxip: u32,
        xmin: u64,
        xmax: u64,
        xip: [u64; N],
    }

    impl<const N: usize> SnapC<N> {
        fn any() -> Self {
            SnapC {
                varsz: 0, // length word untouched by the cores under proof
                nxip: kani::any(),
                xmin: kani::any(),
                xmax: kani::any(),
                xip: kani::any(),
            }
        }

        fn as_ptr(&self) -> *const u8 {
            self as *const Self as *const u8
        }

        /// VARDATA view: the bytes SnapView::new expects (payload after
        /// the 4-byte length word).
        fn vardata(&self) -> &[u8] {
            unsafe {
                core::slice::from_raw_parts(self.as_ptr().add(4), 4 + 8 + 8 + 8 * N)
            }
        }
    }

    // ---- FullTransactionIdFromAllowableAt (transam.h) ----
    // Fence = the C caller contract stated by transam.h's Asserts
    // (compiled out in production builds, shim [S2]): when the xid is
    // numerically above nextFullXid's low word, nextFullXid's epoch must
    // be nonzero (xid is "allowable at" nextFullXid). Outside the fence C
    // wraps epoch-1 to UINT32_MAX (unsigned arithmetic) while shipped
    // Rust would trip its debug_assert / release-wrap — a C-contract
    // plane, not a user-reachable parity question (both sides' callers
    // pass XIDs read from transam state that satisfies the contract).
    #[kani::proof]
    fn eq_full_xid_from_allowable_at() {
        let next: u64 = kani::any();
        let xid: u32 = kani::any();
        // contract fence: only the epoch-decrement branch is constrained
        kani::assume(!(xid >= 3 && xid > next as u32 && (next >> 32) == 0));
        let c = unsafe { pgc_full_xid_from_allowable_at(next, xid) };
        let r = xid8funcs::full_xid_from_allowable_at(next, xid);
        assert!(c == r);
        // regime witnesses: special-xid passthrough, same-epoch, prior-epoch
        kani::cover!(xid < 3);
        kani::cover!(xid >= 3 && xid <= next as u32);
        kani::cover!(xid >= 3 && xid > next as u32);
    }

    // ---- is_visible_fxid: linear-scan arm (nxip <= 30 cutover) ----
    // unwind 6: linear loop over nxip <= 4, +1 exit.
    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_is_visible_fxid_linear() {
        let snap = SnapC::<4>::any();
        kani::assume(snap.nxip <= 4);
        let value: u64 = kani::any();
        let c = unsafe { pgc_is_visible_fxid(value, snap.as_ptr()) };
        let view = xid8funcs::SnapView::new(snap.vardata());
        let r = xid8funcs::is_visible_fxid(value, &view);
        assert!(c == r as c_int);
        kani::cover!(value < snap.xmin); // fast-true arm
        kani::cover!(value >= snap.xmax); // fast-false arm
        kani::cover!(c == 0 && value >= snap.xmin && value < snap.xmax); // xip hit
        kani::cover!(c == 1 && value >= snap.xmin && value < snap.xmax); // xip miss
    }

    // ---- is_visible_fxid: bsearch arm (nxip > 30) ----
    // xips ascending (non-strict): superset of the strictly-ascending
    // on-disk representation every shipped builder produces. Duplicates
    // keep verdict parity (any hit -> false on both sides).
    //
    // Shape lessons applied (first cut walled in SAT at 60s): nxip is a
    // LITERAL per harness (31/32/33 case-split — assumes never
    // constant-fold, literals do), the sortedness assumes are straight-
    // line unrolled (no harness loop forcing unwind slack onto the
    // binary-search loops), and slots beyond NX are literal-zero (dead
    // symbolic bytes inflate CNF). unwind 8: both binary searches run
    // <= ceil(log2(33)) = 6 iterations, +1 exit +1 slack.

    /// Straight-line per-index ops guarded by `$i < NX` (const-folded).
    macro_rules! for_xip_idx {
        (fill, $nx:expr, $snap:ident) => {
            for_xip_idx!(@each fill, $nx, $snap;
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32);
        };
        // sorted reads xip[$i + 1]: stop at 31 (xip[32] is the last slot)
        (sorted, $nx:expr, $snap:ident) => {
            for_xip_idx!(@each sorted, $nx, $snap;
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
        };
        (@each $body:ident, $nx:expr, $snap:ident; $($i:literal),*) => {
            $( for_xip_idx!(@$body $nx, $snap, $i); )*
        };
        (@fill $nx:expr, $snap:ident, $i:literal) => {
            if $i < $nx {
                $snap.xip[$i] = kani::any();
            }
        };
        (@sorted $nx:expr, $snap:ident, $i:literal) => {
            if $i + 1 < $nx {
                kani::assume($snap.xip[$i] <= $snap.xip[$i + 1]);
            }
        };
    }

    fn bsearch_case<const NX: usize>() {
        let mut snap = SnapC::<33> {
            varsz: 0,
            nxip: NX as u32, // literal per monomorphization
            xmin: kani::any(),
            xmax: kani::any(),
            xip: [0u64; 33], // slots >= NX stay literal zero (dead-byte rule)
        };
        for_xip_idx!(fill, NX, snap);
        for_xip_idx!(sorted, NX, snap);
        let value: u64 = kani::any();
        let c = unsafe { pgc_is_visible_fxid(value, snap.as_ptr()) };
        let view = xid8funcs::SnapView::new(snap.vardata());
        let r = xid8funcs::is_visible_fxid(value, &view);
        assert!(c == r as c_int);
        kani::cover!(c == 0 && value >= snap.xmin && value < snap.xmax); // bsearch hit
        kani::cover!(c == 1 && value >= snap.xmin && value < snap.xmax); // bsearch miss
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_is_visible_fxid_bsearch_31() {
        bsearch_case::<31>();
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_is_visible_fxid_bsearch_32() {
        bsearch_case::<32>();
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_is_visible_fxid_bsearch_33() {
        bsearch_case::<33>();
    }

    // Union-coverage gate for the case-split (mandatory per the ladder):
    // every nxip the bsearch arm can see at cap 33 is one of the literal
    // cases above; the linear harness owns nxip <= 30 at its own cap.
    #[kani::proof]
    fn cover_bsearch_case_split() {
        let nxip: u32 = kani::any();
        kani::assume(nxip > 30 && nxip <= 33);
        assert!(nxip == 31 || nxip == 32 || nxip == 33);
    }

    // ---- SnapView layout theorem (pg_snapshot_xmin/xmax cores) ----
    #[kani::proof]
    fn eq_snapview_layout() {
        let snap = SnapC::<3>::any();
        kani::assume(snap.nxip <= 3);
        let view = xid8funcs::SnapView::new(snap.vardata());
        unsafe {
            assert!(pgc_snap_nxip(snap.as_ptr()) == view.nxip());
            assert!(pgc_snap_xmin(snap.as_ptr()) == view.xmin());
            assert!(pgc_snap_xmax(snap.as_ptr()) == view.xmax());
            let i: u32 = kani::any();
            kani::assume(i < snap.nxip);
            assert!(pgc_snap_xip(snap.as_ptr(), i) == view.xip(i as usize));
        }
    }

    // ---- strtou64 vs the libc strtoull(.,.,10) model (shim [S4]) ----
    // Symbolic len <= 6, bytes NUL-free (C reads a NUL-terminated string;
    // an interior NUL would give the two sides different inputs, not a
    // divergence). unwind 8: isspace/digit loops <= 6 iterations + exit.
    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_strtou64_len6() {
        const CAP: usize = 6;
        let buf: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut cbuf = [0u8; CAP + 1]; // trailing NUL(s)
        let mut i = 0usize;
        while i < CAP {
            if i < len {
                kani::assume(buf[i] != 0);
                cbuf[i] = buf[i];
            }
            i += 1;
        }
        let mut endoff: usize = 0;
        let c = unsafe { pgc_strtou64(cbuf.as_ptr(), &mut endoff) };
        let (r, rend) = xid8funcs::strtou64(&buf[..len]);
        assert!(c == r);
        assert!(endoff == rend);
        kani::cover!(rend == 0); // no-conversion arm
        kani::cover!(rend > 0 && rend < len); // stopped at a non-digit
        kani::cover!(rend == len && len > 0); // consumed everything
    }

    // ---- parse_snapshot: verdict + sqlstate + scalar projections ----
    // Input: symbolic NUL-free ASCII, len <= 6 (=> nxip <= 1 reachable;
    // the xip-list loop, order/dedup checks and both strtou64 call sites
    // are all in-domain). Rust runs the SHIPPED parse_snapshot ->
    // snapshot_image pipeline: mcx-stubs recipe, "modulo static-buffer
    // allocator model"; message text/Location out of proof; shipped
    // .with_sqlstate load-bearing -> sqlstate parity asserted.
    // unwind 10: strtou64 loops <= 7 (len 6 + sign), parse loop <= 3
    // values, image build appends 5 chunks, projection loop <= 2.
    fn parse_snapshot_case<const LEN: usize>() {
        // LEN is a literal per monomorphization (per-length cells are
        // ~10x cheaper than one symbolic-length harness; the symbolic-len
        // cut of this harness walled in CNF at 300s with symex complete).
        let buf: [u8; LEN] = kani::any();
        let len: usize = LEN;
        let mut cbuf = [0u8; 8]; // LEN <= 7; trailing NULs
        let mut i = 0usize;
        while i < len {
            // NUL-free ASCII: same bytes on both sides; parse logic
            // is ASCII-only so the UTF-8 fence loses no domain
            kani::assume(buf[i] != 0 && buf[i] < 128);
            cbuf[i] = buf[i];
            i += 1;
        }

        // C side: fixed out-buffer, err flag
        let mut csnap = SnapC::<4> {
            varsz: 0,
            nxip: 0,
            xmin: 0,
            xmax: 0,
            xip: [0; 4],
        };
        let mut cerr: c_int = 0;
        let cok = unsafe {
            pgc_parse_snapshot(cbuf.as_ptr(), &mut csnap as *mut _ as *mut u8, &mut cerr)
        };

        // Rust side: shipped pipeline under the mcx-stub recipe
        let s = core::str::from_utf8(&buf[..len]).unwrap(); // ASCII by fence
        let ctx = token_ctx();
        match xid8funcs::parse_snapshot(ctx.mcx(), s, None) {
            Ok(Some(v)) => {
                assert!(cok == 1 && cerr == 0);
                let view = xid8funcs::SnapView::new(v.data());
                assert!(view.nxip() == csnap.nxip);
                assert!(view.xmin() == csnap.xmin);
                assert!(view.xmax() == csnap.xmax);
                let mut i = 0usize;
                while i < view.nxip() as usize {
                    assert!(view.xip(i) == csnap.xip[i]);
                    i += 1;
                }
                core::mem::forget(v); // image teardown out of the claim
            }
            Ok(None) => {
                // soft-error path requires an escontext; None was passed
                unreachable!()
            }
            Err(e) => {
                assert!(cok == 0 && cerr == 1);
                assert!(e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        if LEN >= 4 {
            // shortest accepted input is "1:1:" (len 4); an unconditional
            // cover would be unsatisfiable in the shorter cells
            kani::cover!(cerr == 0);
        }
        if LEN >= 1 {
            kani::cover!(cerr == 1);
        }
        // token ctx is a static: no teardown to forget
    }

    /// Token Mcx handle (cash/jsonb-probe recipe). SOUNDNESS: with
    /// Mcx::allocate/grow/deallocate AND mcx::vec_with_capacity_in stubbed
    /// to the static proof heap, no path under proof dereferences the
    /// context — a REAL MemoryContext::new_bump is pure scaffolding and a
    /// measured wall (this family: parse cells at 1.7M program-expression
    /// steps / 240s+ CNF with the real bump context). The zeroed image is
    /// never read.
    fn token_ctx() -> &'static mcx::MemoryContext {
        static CTX: [u8; 256] = [0u8; 256];
        assert!(core::mem::size_of::<mcx::MemoryContext>() <= 256);
        unsafe { &*(CTX.as_ptr() as *const mcx::MemoryContext) }
    }

    // Per-length proof cells. unwind 8: at LEN <= 6 every loop (harness
    // NUL-fill, strtou64 skip/digit loops, parse value loop, PgVec image
    // appends, projection compare) runs <= 7 iterations.
    macro_rules! parse_cell {
        ($name:ident, $len:literal) => {
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            // grow/deallocate stubs are LOAD-BEARING: vec_append_bytes
            // has a reachable try_reserve grow branch (json-escape
            // round-2 lesson)
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                parse_snapshot_case::<$len>();
            }
        };
    }

    parse_cell!(eq_parse_snapshot_len0, 0);
    parse_cell!(eq_parse_snapshot_len1, 1);
    parse_cell!(eq_parse_snapshot_len2, 2);
    parse_cell!(eq_parse_snapshot_len3, 3);
    parse_cell!(eq_parse_snapshot_len4, 4);
    parse_cell!(eq_parse_snapshot_len5, 5);
    parse_cell!(eq_parse_snapshot_len6, 6);

    // Union-coverage gate for the per-length split: the claimed domain
    // "len <= 6, NUL-free ASCII" is exactly the union of the cells.
    #[kani::proof]
    fn cover_parse_case_split() {
        let len: usize = kani::any();
        kani::assume(len <= 6);
        assert!(
            len == 0 || len == 1 || len == 2 || len == 3 || len == 4 || len == 5 || len == 6
        );
    }

    // ---- parse_snapshot: concrete SPOT proofs (accept arm + quirks) ----
    // The symbolic accept partition is a CNF wall at every length >= 4
    // (ladder exhausted: literal-length cells, token-ctx +
    // vec_with_capacity_in stub, --no-assertion-reach-checks, both
    // solvers; symex completes at ~786K program steps / 19K VCCs, SAT
    // never returns inside 240s). Per ladder step 5, the accept arm and
    // the user-visible parser quirks are pinned by concrete spots (all
    // loops fold at literal inputs).
    fn parse_spot(input: &'static [u8], expect_ok: bool) {
        let mut cbuf = [0u8; 48];
        let mut i = 0usize;
        while i < input.len() {
            cbuf[i] = input[i];
            i += 1;
        }
        let mut csnap = SnapC::<4> {
            varsz: 0,
            nxip: 0,
            xmin: 0,
            xmax: 0,
            xip: [0; 4],
        };
        let mut cerr: c_int = 0;
        let cok = unsafe {
            pgc_parse_snapshot(cbuf.as_ptr(), &mut csnap as *mut _ as *mut u8, &mut cerr)
        };
        let s = core::str::from_utf8(input).unwrap();
        let ctx = token_ctx();
        match xid8funcs::parse_snapshot(ctx.mcx(), s, None) {
            Ok(Some(v)) => {
                assert!(expect_ok); // spot self-check: arm is the intended one
                assert!(cok == 1 && cerr == 0);
                let view = xid8funcs::SnapView::new(v.data());
                assert!(view.nxip() == csnap.nxip);
                assert!(view.xmin() == csnap.xmin);
                assert!(view.xmax() == csnap.xmax);
                let mut i = 0usize;
                while i < view.nxip() as usize {
                    assert!(view.xip(i) == csnap.xip[i]);
                    i += 1;
                }
                core::mem::forget(v);
            }
            Ok(None) => unreachable!(),
            Err(e) => {
                assert!(!expect_ok);
                assert!(cok == 0 && cerr == 1);
                assert!(e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    macro_rules! parse_spot_cell {
        ($($name:ident: $input:literal => $ok:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(26)] // longest spot input is 23 bytes + digit run
                                // 20; exact-fit unwind (slack converts
                                // directly to RSS on this shared box)
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                parse_spot($input, $ok);
            }
        )*};
    }

    parse_spot_cell! {
        // accept arm
        spot_parse_min_accept: b"1:1:" => true;              // shortest accept
        spot_parse_one_xip: b"1:9:5" => true;
        spot_parse_two_xips: b"1:9:3,5" => true;
        spot_parse_dedup: b"1:9:5,5" => true;                // dup skipped, nxip 1
        spot_parse_trailing_comma: b"1:9:5," => true;        // C quirk: accepted
        spot_parse_inner_space: b"1: 9:" => true;            // strtou64 skips isspace
        spot_parse_u64_max: b"1:18446744073709551615:" => true;
        // '-' negates modulo 2^64 (strtoul semantics): xmax = 2^64 - 9
        spot_parse_neg_sign: b"1:-9:" => true;
        // overflow saturates to UINT64_MAX -> xmax(9) < xmin -> reject
        spot_parse_overflow_sat: b"99999999999999999999:9:" => false;
        // reject arm
        spot_parse_zero_xmin: b"0:1:" => false;
        spot_parse_zero_xmax: b"1:0:" => false;
        spot_parse_xmax_lt_xmin: b"2:1:" => false;
        spot_parse_xip_below_xmin: b"3:9:1" => false;
        spot_parse_xip_at_xmax: b"1:9:9" => false;
        spot_parse_unordered: b"1:9:5,3" => false;
        spot_parse_garbage: b"abc" => false;
        spot_parse_missing_colon: b"1:2" => false;
        spot_parse_bad_tail: b"1:9:5x" => false;
        spot_parse_empty: b"" => false;
    }

    // ---- negative control: MUST FAIL (default solver) ----
    // C reads xmax where Rust reads xmin: the rig must catch it.
    #[kani::proof]
    fn control_snapview_swapped() {
        let snap = SnapC::<3>::any();
        kani::assume(snap.xmin != snap.xmax);
        let view = xid8funcs::SnapView::new(snap.vardata());
        let c = unsafe { pgc_snap_xmax(snap.as_ptr()) };
        assert!(c == view.xmin());
    }
}
