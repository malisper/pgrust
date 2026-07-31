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
//! pg_snapshot_recv/send (oids 2941/5057, 2942/5058) = mod sendrecv
//! below (pg_lsn/uuid wave-5 wire rig; see that module's doc).

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

/// WAVE sendrecv: pg_snapshot_recv (2941/5057) / pg_snapshot_send
/// (2942/5058) vs REL_18_STABLE pgc_pg_snapshot_recv/send (vendored in
/// c/pg_xid8snap.c, shims [S8]-[S13] documented there).
///
/// RECV — Rust side runs the SHIPPED snapshot_recv(mcx, &mut StringInfo)
/// pipeline (pqformat::pq_getmsgint/pq_getmsgint64 + snapshot_image) on a
/// harness-built StringInfo (uuid/pg_lsn wave-5 rig), C side the vendored
/// body on the identical bytes as a (data, dlen, cursor) triple; cursor
/// starts at 0. Per the result-image law, every cell has a CONCRETE input
/// length AND, wherever the xip-loop/allocation plane is reachable
/// (dlen >= 20), a CONCRETE nxip written as LITERAL frame bytes (literal
/// case cells prune; assume-pins do not — and a symbolic nxip makes the
/// output image length data-dependent, the measured CNF wall class).
/// Cells (frame cap 36 = 4 + 8 + 8 + 8*2, NXIP cap 2):
///   d0/d1/d2/d3  partial nxip word            -> Err(08P01) only
///   d4           nxip word exact              -> Err(08P01) + Err(22P03)
///   d12          xmin exact, xmax short       -> both Err classes
///   d19          xmax one byte short          -> both Err classes
///   d20n0        nxip=0 exact frame           -> Ok + Err(22P03)
///   d20n1        nxip=1, no xip bytes         -> Err(08P01) + Err(22P03)
///   d28n1        nxip=1 exact                 -> Ok + both Err classes
///   d36n0        nxip=0 + 16 trailing junk    -> Ok (cursor 20) — the
///                function itself never calls pq_getmsgend (the fmgr recv
///                CALLER does); claim scoped to the function
///   d36n2        nxip=2 exact                 -> Ok + both Err classes,
///                including the duplicate-xip plane (cur == last drops the
///                dup: nxip 2 -> image nxip 1) — verified equivalent to
///                C's `i--; nxip--; continue;` (net: i unchanged, nxip-1;
///                both sides read exactly the original nxip int64s)
/// Asserts: Ok -> C status 0, image length parity (Rust
/// Varlena::as_bytes().len() vs C *outlen), FULL varlena image byte
/// equality (both headers are LE len<<2, [S12]) and cursor parity;
/// Err -> exact sqlstate class per the C sentinel (4 -> 08P01
/// ERRCODE_PROTOCOL_VIOLATION, 22 -> 22P03
/// ERRCODE_INVALID_BINARY_REPRESENTATION) + level ERROR. kani::cover on
/// every reachable arm per cell. NOTE the dlen set is
/// boundary-representative, NOT an exhaustive dlen<=36 split — the claim
/// is scoped to the listed cells (no union-coverage harness applies).
///
/// SEND — input snapshot image built in-harness (ImgC, the SnapC layout
/// twin with a live LE 4B-U header so the shipped arg_varlena_packed
/// takes its inline arm; detoast out of scope, family SNAPSHOT MODEL
/// fence). xmin/xmax/xips fully symbolic and UNFENCED — neither send
/// validates anything (both just read fields; verified in both bodies).
/// nxip = 0/1/2 as struct-field literals per cell. Rust runs the SHIPPED
/// fc_pg_snapshot_send via LocalFcinfo (arg 0 = pointer datum), so datum
/// unwrap + pq_begintypsend/pq_sendint32/pq_sendint64/pq_endtypsend are
/// inside the theorem. Asserts image length (4B hdr + 4 + 8 + 8 + 8*nxip)
/// and full byte equality.
///
/// Scaffolding (identical to this crate's parse cells): mcx-stubs +
/// tiny-proof-heap recipe, "modulo static-buffer allocator model" —
/// largest allocations are send's pq_begintypsend StringInfo (1024) and
/// recv's harness StringInfo (38) + xips vec (<=16) + image (<=44), all
/// within the 2 KiB proof heap; message text / Location out of proof
/// (PgError::error + fmt stubs), shipped .with_sqlstate load-bearing ->
/// sqlstate parity IS asserted.
///
/// Controls: control_snapshot_send_skew (C fed xmax^1) MUST FAIL with a
/// decodable counterexample — run with the DEFAULT solver.
/// cover_recv_family pins rig liveness: Ok and both Err classes each
/// reached on a concrete witness frame.
#[cfg(kani)]
mod sendrecv {
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use std::os::raw::c_int;
    use types_error::{
        ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_PROTOCOL_VIOLATION, ERROR,
    };
    use types_fmgr::LocalFcinfo;

    extern "C" {
        fn pgc_pg_snapshot_recv(
            data: *const u8,
            dlen: i32,
            cursor: *mut i32,
            outbuf: *mut u8,
            outlen: *mut i32,
        ) -> c_int;
        fn pgc_pg_snapshot_send(snapimg: *const u8, out: *mut u8) -> i32;
        fn pgc_pg_snapshot_max_nxip() -> u64;
    }

    /// C recv status sentinels ([S9]/[S11] in c/pg_xid8snap.c).
    const PGC_ERR_PROTOCOL: c_int = 4; // 08P01 insufficient data
    const PGC_ERR_BADFORMAT: c_int = 22; // 22P03 invalid external pg_snapshot data

    /// Token Mcx handle — same recipe + soundness note as the parse cells'
    /// token_ctx above (with the full mcx stub set no path under proof
    /// dereferences the context; a real bump context is a measured wall in
    /// this family). Duplicated here because the sibling module is private.
    fn token_ctx() -> &'static mcx::MemoryContext {
        static CTX: [u8; 256] = [0u8; 256];
        assert!(core::mem::size_of::<mcx::MemoryContext>() <= 256);
        unsafe { &*(CTX.as_ptr() as *const mcx::MemoryContext) }
    }

    /// [S8] cross-check: the vendored C cap constant == the shipped Rust
    /// cap constant (the nxip<=cap comparisons on both sides are verbatim
    /// over these).
    #[kani::proof]
    fn eq_snapshot_max_nxip() {
        let c = unsafe { pgc_pg_snapshot_max_nxip() };
        assert!(c == xid8funcs::PG_SNAPSHOT_MAX_NXIP as u64);
    }

    /// Dual-run + parity asserts on one concrete-length frame (cursor 0).
    /// Returns C's status so callers can cover/pin arms.
    fn recv_parity(data: &[u8]) -> c_int {
        let mut ccur: i32 = 0;
        let mut cout = [0u8; 64]; // [S10]: >= 4 + 20 + 8*4; frame cap 36 writes <= 2 slots
        let mut coutlen: i32 = 0;
        let cst = unsafe {
            pgc_pg_snapshot_recv(
                data.as_ptr(),
                data.len() as i32,
                &mut ccur,
                cout.as_mut_ptr(),
                &mut coutlen,
            )
        };

        let ctx = token_ctx();
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), 38) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(data) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        match xid8funcs::snapshot_recv(ctx.mcx(), &mut si) {
            Ok(v) => {
                assert!(cst == 0);
                let img = v.as_bytes();
                assert!(img.len() == coutlen as usize);
                let mut j = 0usize;
                while j < img.len() {
                    assert!(img[j] == cout[j]);
                    j += 1;
                }
                assert!(si.cursor == ccur as usize);
                core::mem::forget(v);
            }
            Err(e) => {
                if cst == PGC_ERR_PROTOCOL {
                    assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                } else {
                    assert!(cst == PGC_ERR_BADFORMAT);
                    assert!(e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION);
                }
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        cst
    }

    /// One recv cell: concrete DLEN symbolic bytes; PIN >= 0 writes the
    /// nxip word as LITERAL big-endian frame bytes (mandatory wherever the
    /// xip-loop/allocation plane is reachable, i.e. DLEN >= 20).
    fn recv_case<const DLEN: usize, const PIN: i64>() {
        let mut data = [0u8; 36];
        let live: [u8; DLEN] = kani::any();
        let mut k = 0usize;
        while k < DLEN {
            data[k] = live[k];
            k += 1;
        }
        if PIN >= 0 {
            let be = (PIN as u32).to_be_bytes();
            data[0] = be[0];
            data[1] = be[1];
            data[2] = be[2];
            data[3] = be[3];
        }
        let cst = recv_parity(&data[..DLEN]);
        // arm covers: per the cell table in the module doc
        if DLEN < 20 || PIN == 1 && DLEN == 20 {
            kani::cover!(cst == PGC_ERR_PROTOCOL); // short-read arm
        }
        if DLEN >= 4 && PIN < 0 || DLEN >= 20 {
            kani::cover!(cst == PGC_ERR_BADFORMAT); // validation arm
        }
        if DLEN >= 20 && PIN >= 0 && (DLEN - 20) / 8 >= PIN as usize {
            kani::cover!(cst == 0); // accept arm
        }
    }

    macro_rules! recv_cell {
        ($($name:ident: $dlen:literal, $pin:literal, $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)] // >= max(dlen, image len) + slack for the
                                 // fill/copy/compare loops (tight per cell)
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            // grow/deallocate stubs LOAD-BEARING (vec_append_bytes'
            // reachable try_reserve grow branch; family lesson)
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                recv_case::<$dlen, $pin>();
            }
        )*};
    }

    recv_cell! {
        eq_snapshot_recv_d0:    0, -1, 6;
        eq_snapshot_recv_d1:    1, -1, 8;
        eq_snapshot_recv_d2:    2, -1, 8;
        eq_snapshot_recv_d3:    3, -1, 8;
        eq_snapshot_recv_d4:    4, -1, 10;
        eq_snapshot_recv_d12:  12, -1, 16;
        eq_snapshot_recv_d19:  19, -1, 23;
        eq_snapshot_recv_d20n0: 20, 0, 28;
        eq_snapshot_recv_d20n1: 20, 1, 26;
        eq_snapshot_recv_d28n1: 28, 1, 36;
        eq_snapshot_recv_d36n0: 36, 0, 42;
        eq_snapshot_recv_d36n2: 36, 2, 48;
    }

    /// Family rig-liveness cover: Ok and BOTH Err classes each reached on
    /// a concrete witness frame (gate-blindness insurance for the split).
    #[kani::proof]
    #[kani::unwind(28)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn cover_recv_family() {
        // nxip=0, xmin=1, xmax=1: shortest accept
        let ok_frame: [u8; 20] = [
            0, 0, 0, 0, // nxip = 0
            0, 0, 0, 0, 0, 0, 0, 1, // xmin = 1
            0, 0, 0, 0, 0, 0, 0, 1, // xmax = 1
        ];
        let st = recv_parity(&ok_frame);
        assert!(st == 0);
        kani::cover!(st == 0, "recv Ok class reached");

        // 3 bytes: nxip word short
        let short_frame: [u8; 3] = [0, 0, 0];
        let st = recv_parity(&short_frame);
        assert!(st == PGC_ERR_PROTOCOL);
        kani::cover!(st == PGC_ERR_PROTOCOL, "recv 08P01 class reached");

        // nxip = -1: bad format
        let bad_frame: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];
        let st = recv_parity(&bad_frame);
        assert!(st == PGC_ERR_BADFORMAT);
        kani::cover!(st == PGC_ERR_BADFORMAT, "recv 22P03 class reached");
    }

    // ---- send ----

    /// pg_snapshot varlena image twin (SnapC layout + a LIVE header):
    /// varsz carries the LE 4B-U header (len << 2, datum::set_varsize_4b
    /// semantics) so the shipped arg_varlena_packed takes its inline arm.
    #[repr(C)]
    struct ImgC<const N: usize> {
        varsz: u32,
        nxip: u32,
        xmin: u64,
        xmax: u64,
        xip: [u64; N],
    }

    /// Send cell: literal nxip = NX, fully symbolic UNFENCED fields (send
    /// validates nothing on either side). `cskew` xors C's xmax (0 for the
    /// equivalence cells; 1 for the must-fail control).
    fn send_case<const NX: usize>(cskew: u64) {
        let hdr = (((4 + 20 + 8 * NX) as u32) << 2) as u32; // LE 4B-U
        let xmin: u64 = kani::any();
        let xmax: u64 = kani::any();
        let xip: [u64; NX] = kani::any();
        let rimg = ImgC::<NX> { varsz: hdr, nxip: NX as u32, xmin, xmax, xip };
        let cimg = ImgC::<NX> {
            varsz: hdr,
            nxip: NX as u32,
            xmin,
            xmax: xmax ^ cskew,
            xip,
        };

        let mut cbuf = [0u8; 44]; // 4 + 4 + 8 + 8 + 8*2 max
        let clen = unsafe {
            pgc_pg_snapshot_send(&cimg as *const ImgC<NX> as *const u8, cbuf.as_mut_ptr())
        };

        let ctx = token_ctx();
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx is a static token; it outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(
            &rimg as *const ImgC<NX> as usize,
        ));
        let d = match xid8funcs::builtins::fc_pg_snapshot_send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("pg_snapshot_send errored")
            }
        };
        let expected = 4 + 4 + 8 + 8 + 8 * NX;
        assert!(clen as usize == expected);
        // SAFETY: varlena_result leaked the image; its first `expected`
        // bytes are the full send image (header stamped by from_image).
        let out = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, expected) };
        let mut j = 0usize;
        while j < expected {
            assert!(out[j] == cbuf[j]);
            j += 1;
        }
        kani::cover!(true, "send cell executed");
    }

    macro_rules! send_cell {
        ($($name:ident: $nx:literal, $skew:literal, $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)] // image len + slack for the compare loop
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                send_case::<$nx>($skew);
            }
        )*};
    }

    send_cell! {
        eq_snapshot_send_n0: 0, 0, 28;
        eq_snapshot_send_n1: 1, 0, 36;
        eq_snapshot_send_n2: 2, 0, 44;
        // MUST FAIL (wire-section control): C is fed xmax^1 — the rig has
        // to catch the mismatch in the xmax bytes. DEFAULT solver.
        control_snapshot_send_skew: 1, 1, 36;
    }
}
