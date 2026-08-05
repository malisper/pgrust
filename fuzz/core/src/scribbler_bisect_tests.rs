//! SCRIBBLER attribution + regression pins (task #112).
//!
//! ATTRIBUTED 2026-08-03. The writer is the shim `pstrdup` in
//! csrc/pg_float_io.c under the verbatim `float8in_internal` /
//! `float4in_internal` bodies:
//!
//! ```text
//! char *errnumber = pstrdup(num);
//! errnumber[endptr - num] = '\0';        /* index runs to strlen(num) */
//! ```
//!
//! PG's `pstrdup` returns `strlen(s)+1` bytes so that store is in-bounds; the
//! shim returned a fixed 256-byte static and silently truncated, so for a
//! longer `num` the store wrote one NUL byte arbitrarily far past the buffer
//! and into the next static in .bss. See
//! docs/conformance/scribbler-investigation-2026-08-02.md §8 for the measured
//! address arithmetic.
//!
//! These are the regression pins. `scribbler_single_seed_su6e1600` is the
//! minimal repro: seed `Su6E-1600` is the jsonb->float8 cast of `6E-1600`,
//! whose `numeric_out` is a 1602-char string, so the truncating shim wrote
//! 1346 bytes past its buffer, landing on byte index 2 of `datecache[4]`.
//! `scribbler_bisect_jsonbio_seeds` scans the whole committed jsonbio corpus
//! for any surviving instance of the class.
//!
//! Why the presentation looked like a race, and was not: the write is
//! single-threaded and deterministic (both tests reproduce it with
//! `--test-threads=1`, one test in the process). It only became VISIBLE under
//! whole-lib parallelism because a sibling datetime test had to populate
//! `datecache[]` first — zeroing byte 2 of a still-NULL slot leaves it NULL,
//! which no predicate can see. The coupling is process-global .bss shared
//! across oracle TUs, not an interleaving.

use crate::c_oracle_serial;

extern "C" {
    fn pg_tsdiff_timestamp_in(
        s: *const std::ffi::c_char,
        typmod: i32,
        style: i32,
        order: i32,
        tz: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_cache_check() -> i32;
    fn pg_tsdiff_cache_peek(which: i32, idx: i32) -> usize;
    fn pg_tsdiff_cache_table_base(which: i32) -> usize;
    fn pg_tsdiff_cache_table_nel(which: i32) -> i32;
    fn pg_tsdiff_cache_addr(which: i32) -> usize;
    // H6 detector (csrc/pg_float_io.c): guard band over the shim message
    // buffer that the attributed writer overran.
    fn pg_diff_msgbuf_check() -> i32;
    fn pg_diff_msgbuf_slack() -> i32;
    fn pg_diff_msgbuf_poison_for_test(off: i32) -> i32;
    // A real float8 error path, to arm the message buffer.
    fn pg_diff_float8in(num: *const std::ffi::c_char) -> f64;
}

/// Drive the float8 out-of-range error path so the shim message buffer is
/// allocated and its guard band armed.
fn arm_msgbuf() {
    let cs = std::ffi::CString::new("1e999999").unwrap();
    unsafe { pg_diff_float8in(cs.as_ptr()) };
    assert_eq!(
        unsafe { pg_diff_msgbuf_check() },
        0,
        "message buffer is not intact after a legal error path"
    );
}

/// H6 clean path: a legal over-range parse arms the band and does not fire.
#[test]
fn h6_msgbuf_guard_clean_path_does_not_fire() {
    let guard = c_oracle_serial();
    arm_msgbuf();
    drop(guard); // Drop runs H6; a panic here fails the test.
}

/// H6 must-fail control: clobber one guard byte exactly the way an over-index
/// would and assert the guard's Drop names it. A detector nobody has seen fire
/// is not a detector.
#[test]
fn h6_msgbuf_guard_fires_on_overrun() {
    let guard = c_oracle_serial();
    arm_msgbuf();
    let off = unsafe { pg_diff_msgbuf_poison_for_test(7) };
    assert_eq!(off, 7, "poison planter found no armed buffer");

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(guard)));
    let err = r.expect_err("H6 detector failed to fire on a clobbered guard band");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("SCRIBBLER H6") && msg.contains("guard byte +7"),
        "unexpected detector panic message: {msg:?}"
    );
    // And it self-heals: the next oracle exit is clean again.
    let guard = c_oracle_serial();
    drop(guard);
}

/// Parse a datetime whose 5th whitespace field is a NON-timezone datetktbl
/// token, so `DecodeSpecial(4, ...)` populates `datecache[4]` with a legal
/// in-table pointer. Timezone abbrevs (gmt/utc) are intercepted by the
/// `session_timezone` arm of `DecodeTimezoneAbbrev` (which fills
/// `tzabbrevcache` instead); a meridian is not. Fields: "3"(0) "jan"(1)
/// "2001"(2) "4:05"(3) "pm"(4).
///
/// Populating is what makes the corruption OBSERVABLE — see the module note.
fn populate_datecache_slot4() -> usize {
    let cs = std::ffi::CString::new("3 jan 2001 4:05 pm").unwrap();
    let mut t = 0i64;
    let rc = unsafe { pg_tsdiff_timestamp_in(cs.as_ptr(), -1, 2, 0, 0, &mut t) };
    assert_eq!(rc, 0, "populate parse failed rc={rc}");
    let v = unsafe { pg_tsdiff_cache_peek(0, 4) };
    assert_ne!(v, 0, "populate did not fill datecache[4]");
    let base = unsafe { pg_tsdiff_cache_table_base(0) };
    let nel = unsafe { pg_tsdiff_cache_table_nel(0) } as usize;
    assert!(
        v >= base && v < base + nel * 32,
        "populated datecache[4] {v:#x} is not plausibly inside datetktbl {base:#x}"
    );
    v
}

/// Minimal deterministic repro / regression pin for the attributed writer.
#[test]
fn scribbler_single_seed_su6e1600() {
    let _g = c_oracle_serial();

    let healthy = populate_datecache_slot4();
    let addr = unsafe { pg_tsdiff_cache_addr(0) };

    // sel = b'S' % 5 = 3 (cast arm), which = b'u' % 7 = 5 (jsonb_float8),
    // JSON text "6E-1600" -> numeric_out is 1602 chars -> float8 underflow
    // error path -> pstrdup(num) + errnumber[endptr - num] = '\0'.
    crate::jsonbio_diff(b"Su6E-1600");

    let v = unsafe { pg_tsdiff_cache_peek(0, 4) };
    let code = unsafe { pg_tsdiff_cache_check() };
    assert!(
        code == 0 && v == healthy,
        "SCRIBBLER REGRESSION: code {code}, datecache[4] {v:#018x} \
         (healthy {healthy:#018x}, xor {:#018x}); datecache @ {addr:#x}. \
         The float-in shim pstrdup is truncating again — see \
         csrc/pg_float_io.c and this module's header.",
        v ^ healthy
    );
}

/// Whole-corpus scan for any surviving instance of the class: replay the
/// committed jsonbio corpus one seed at a time with `datecache[4]` legally
/// populated, and name the first seed after which the slot changed.
#[test]
fn scribbler_bisect_jsonbio_seeds() {
    let _g = c_oracle_serial();

    let mut healthy = populate_datecache_slot4();

    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/jsonbio_diff");
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .expect("corpus/jsonbio_diff missing")
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .collect();
    entries.sort();
    assert!(entries.len() >= 30, "expected >=30 seeds, found {}", entries.len());

    let mut hits = 0u32;
    for p in &entries {
        let bytes = std::fs::read(p).unwrap();
        crate::jsonbio_diff(&bytes);

        let v = unsafe { pg_tsdiff_cache_peek(0, 4) };
        let code = unsafe { pg_tsdiff_cache_check() };
        if code != 0 || v != healthy {
            hits += 1;
            eprintln!(
                "SCRIBBLER HIT after seed {}: code {code}, datecache[4] {v:#018x} \
                 (healthy {healthy:#018x}, xor {:#018x}) input {} bytes: {:?}",
                p.file_name().unwrap().to_string_lossy(),
                v ^ healthy,
                bytes.len(),
                &bytes[..bytes.len().min(64)],
            );
            // check() clears both caches on poison; repopulate for the rest
            healthy = populate_datecache_slot4();
        }
    }
    assert_eq!(hits, 0, "scribbler localized: {hits} poisoning seed(s) named above");
}

/// EXACT-SIZING CONTROL. The first cut of this shim sized the buffer up only
/// ("grow, never shrink"), which is memory-SAFE but detection-BLIND: after one
/// long call the buffer keeps the larger capacity, so a later short call leaves
/// hundreds of bytes of slack and an input-derived store past `strlen` lands
/// inside the allocation instead of in the guard band — invisible to H6 and to
/// the allocator both. Real `mcxt.c` hands out a fresh chunk of exactly
/// strlen+1 per call, and this family's doctrine is that the allocation SIZE is
/// the load-bearing half of the contract.
///
/// So: assert capacity tracks the CURRENT string, having first driven a much
/// longer one. Under grow-never-shrink this fails (cap stays at the long
/// string's size); under exact sizing it passes.
#[test]
fn h6_msgbuf_capacity_is_exact_after_a_longer_call() {
    let _guard = c_oracle_serial();

    // A long over-range literal first, so any grow-only policy inflates cap.
    let long = format!("1e{}", "9".repeat(600));
    let cs = std::ffi::CString::new(long.clone()).unwrap();
    unsafe { pg_diff_float8in(cs.as_ptr()) };
    let long_slack = unsafe { pg_diff_msgbuf_slack() };
    assert_eq!(long_slack, 0, "the long call itself must be exactly sized");

    // Now a short one. Exact sizing must SHRINK the allocation back.
    arm_msgbuf();
    let short_slack = unsafe { pg_diff_msgbuf_slack() };
    assert_eq!(
        short_slack, 0,
        "GROW-NEVER-SHRINK REGRESSION: {short_slack} bytes of slack after a \
         short message following a ~600-byte one. Slack past the guard band \
         hides exactly the overrun this shim exists to expose."
    );
    assert_eq!(unsafe { pg_diff_msgbuf_check() }, 0, "band not intact");
}
