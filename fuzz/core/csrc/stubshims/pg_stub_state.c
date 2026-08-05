/*
 * pg_stub_state.c — the C half of the shared stub-pin facility
 * (fuzz/core/src/stubs.rs is the Rust half; fuzz/STUBS.md documents how a
 * target declares its pins).
 *
 * THIS FILE IS NOT POSTGRESQL SOURCE. It is driver-side shim state only: a
 * canonical set of thread-local "pinned session state" globals (the GUC /
 * clock / prng / work_mem values a differential exec runs under) plus the
 * setters the Rust driver calls. Vendored PostgreSQL C is never edited to
 * read these; instead
 *
 *   - NEW oracle TUs that want pinned state extern these globals (or take
 *     the value as an argument the driver reads from pg_stub_get_*) instead
 *     of defining their own per-TU copies, and
 *   - the pg_stub_*_guc consumer wrappers below route the pinned globals
 *     into EXISTING verbatim vendored consumers by extern call
 *     (pg_float_io.c float8out_internal_efd, pg_timestamp_io.c
 *     EncodeDateTime/EncodeInterval via pg_tsdiff_*, pg_pg_prng_io.c
 *     verbatim xoroshiro128** engine, pg_libfam_io.c verbatim
 *     bloom_create). These wrappers are what the must-fail control tests
 *     and the demo wiring drive: they prove the pinned global is ALIVE all
 *     the way into vendored code, not just stored.
 *
 * BOTH-SIDES DISCIPLINE: a pinned value is part of the compared input. The
 * Rust driver derives it ONCE from fuzz bytes (bounded to the GUC's legal
 * range in stubs.rs — one derivation, so both sides clamp identically by
 * construction) and writes it to the Rust-side thread-local AND here. Never
 * let one side default.
 *
 * _Thread_local, not plain globals: this models per-backend session state,
 * pgrust is thread-per-backend, and the multi-threaded `cargo test` rails
 * otherwise manufacture cross-test "divergences" (pg_timestamp_io.c
 * DateStyle precedent, lane merge 2026-07-31). libFuzzer campaigns are one
 * thread per process and unaffected.
 *
 * Defaults mirror PG boot values (guc_tables: extra_float_digits=1,
 * DateStyle=ISO/MDY, IntervalStyle=postgres, standard_conforming_strings=on,
 * work_mem=4096kB, maintenance_work_mem=65536kB). The clock default is 0
 * (PG epoch 2000-01-01); the prng state default is the all-zero xoroshiro
 * fixed point — a target using clock/prng pins MUST pin before use, and the
 * controls verify pinning is alive.
 */
#include <stdint.h>
#include "../pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ---- stub:guc — pinned GUC scalars -------------------------------------- */

_Thread_local int pg_stub_extra_float_digits = 1;
_Thread_local int pg_stub_DateStyle = 1;	/* USE_ISO_DATES */
_Thread_local int pg_stub_DateOrder = 2;	/* DATEORDER_MDY */
_Thread_local int pg_stub_IntervalStyle = 0;	/* INTSTYLE_POSTGRES */
_Thread_local int pg_stub_standard_conforming_strings = 1;

void
pg_stub_set_extra_float_digits(int v)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_extra_float_digits = v;
}

int
pg_stub_get_extra_float_digits(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_stub_extra_float_digits;
}

void
pg_stub_set_datestyle(int style, int order)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_DateStyle = style;
	pg_stub_DateOrder = order;
}

int
pg_stub_get_datestyle(void)
{
	return pg_stub_DateStyle;
}

int
pg_stub_get_dateorder(void)
{
	return pg_stub_DateOrder;
}

void
pg_stub_set_intervalstyle(int istyle)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_IntervalStyle = istyle;
}

int
pg_stub_get_intervalstyle(void)
{
	return pg_stub_IntervalStyle;
}

void
pg_stub_set_standard_conforming_strings(int on)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_standard_conforming_strings = on;
}

int
pg_stub_get_standard_conforming_strings(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_stub_standard_conforming_strings;
}

/*
 * md5_password_warnings (bool GUC, crypt.c; boot true) — the cryptbe family
 * oracle #defines the vendored global name onto the getter. Rust side = the
 * crypt crate's session cell via the installed GUC accessor.
 */
_Thread_local int pg_stub_md5_password_warnings = 1;

void
pg_stub_set_md5_password_warnings(int on)
{
	pg_stub_md5_password_warnings = on;
}

int
pg_stub_get_md5_password_warnings(void)
{
	return pg_stub_md5_password_warnings;
}

/*
 * scram_iterations (int GUC, auth-scram.c scram_sha_256_iterations; boot
 * 4096, legal range [1, INT_MAX]) — same #define-onto-getter consumption.
 */
_Thread_local int pg_stub_scram_iterations = 4096;

void
pg_stub_set_scram_iterations(int iters)
{
	pg_stub_scram_iterations = iters;
}

int
pg_stub_get_scram_iterations(void)
{
	return pg_stub_scram_iterations;
}

/* ---- stub:prng scram-salt channel ----------------------------------------- */

/*
 * The pg_strong_random-shaped entropy read inside pg_be_scram_build_secret
 * (SCRAM_DEFAULT_SALT_LEN = 16 bytes). The oracle TU's pg_strong_random shim
 * copies from here; the Rust side pins the SAME bytes through the shipped
 * crate's PGRUST_SCRAM_FIXED_SALT_B64 determinism hook (the real seam the
 * shipped pg_be_scram_build_secret reads). Default zero salt — a target
 * using the channel MUST pin per exec; the must-fail control proves the pin
 * is alive on both sides.
 */
_Thread_local uint8_t pg_stub_scram_salt[16];

void
pg_stub_set_scram_salt(const uint8_t *salt16)
{
	for (int i = 0; i < 16; i++)
		pg_stub_scram_salt[i] = salt16[i];
}

void
pg_stub_get_scram_salt(uint8_t *out16)
{
	for (int i = 0; i < 16; i++)
		out16[i] = pg_stub_scram_salt[i];
}

/* ---- stub:clock — pinned GetCurrentTimestamp ----------------------------- */

_Thread_local int64_t pg_stub_now_usecs = 0;

void
pg_stub_set_current_timestamp(int64_t usecs)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_now_usecs = usecs;
}

/*
 * The GetCurrentTimestamp analog a stub-aware oracle TU calls (or externs
 * pg_stub_now_usecs directly) wherever vendored code reads the clock.
 */
int64_t
pg_stub_get_current_timestamp(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_stub_now_usecs;
}

/*
 * stub:clock monotonic half — the INSTR_TIME_SET_CURRENT / clock_gettime(
 * CLOCK_MONOTONIC) analog. An oracle TU #defines its INSTR_TIME_SET_CURRENT
 * (or equivalent monotonic read) to pg_stub_get_mono_ns(); the Rust half is
 * pg_clock's fuzz_mono_pin feature. Default 0 like the timestamp channel:
 * a target using it MUST pin before use.
 */

_Thread_local uint64_t pg_stub_mono_ns_val = 0;

void
pg_stub_set_mono_ns(uint64_t ns)
{
	pg_stub_mono_ns_val = ns;
}

uint64_t
pg_stub_get_mono_ns(void)
{
	return pg_stub_mono_ns_val;
}

/* ---- stub:prng — pinned pg_global_prng_state analog ---------------------- */

/*
 * The engine is the VERBATIM vendored pg_prng.c compiled in
 * csrc/pg_pg_prng_io.c; this TU only holds the "global state" the way
 * pg_global_prng_state does, reached through that TU's explicit-state
 * entry points.
 */
extern void pg_diff_prng_seed(uint64_t seed, uint64_t *out_s0, uint64_t *out_s1);
extern uint64_t pg_diff_prng_u64(uint64_t s0, uint64_t s1, uint64_t *out_s0, uint64_t *out_s1);
extern double pg_diff_prng_double(uint64_t s0, uint64_t s1, uint64_t *out_s0, uint64_t *out_s1);

static _Thread_local uint64_t pg_stub_prng_s0 = 0;
static _Thread_local uint64_t pg_stub_prng_s1 = 0;

void
pg_stub_prng_seed(uint64_t seed)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_diff_prng_seed(seed, &pg_stub_prng_s0, &pg_stub_prng_s1);
}

uint64_t
pg_stub_prng_u64(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_diff_prng_u64(pg_stub_prng_s0, pg_stub_prng_s1,
							&pg_stub_prng_s0, &pg_stub_prng_s1);
}

double
pg_stub_prng_double(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_diff_prng_double(pg_stub_prng_s0, pg_stub_prng_s1,
							   &pg_stub_prng_s0, &pg_stub_prng_s1);
}

/* ---- stub:workmem — pinned memory-ceiling GUCs ---------------------------- */

_Thread_local int pg_stub_work_mem = 4096;
_Thread_local int pg_stub_maintenance_work_mem = 65536;

void
pg_stub_set_work_mem(int work_mem_kb, int maintenance_work_mem_kb)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_work_mem = work_mem_kb;
	pg_stub_maintenance_work_mem = maintenance_work_mem_kb;
}

int
pg_stub_get_work_mem(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_stub_work_mem;
}

int
pg_stub_get_maintenance_work_mem(void)
{
	return pg_stub_maintenance_work_mem;
}

/* ---- consumer wrappers: pinned globals -> verbatim vendored consumers ---- */

/*
 * float8out under the pinned extra_float_digits (verbatim
 * float8out_internal_efd body in pg_float_io.c). Returns the NUL-terminated
 * image length; buf32 must hold >= 32 bytes.
 */
extern int pg_diff_float8out_efd(double num, int efd, char *buf32);

int
pg_stub_float8out_guc(double num, char *buf32)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_diff_float8out_efd(num, pg_stub_extra_float_digits, buf32);
}

/*
 * timestamp_out / interval_out under the pinned DateStyle/DateOrder/
 * IntervalStyle (verbatim EncodeDateTime/EncodeInterval in
 * pg_timestamp_io.c; that TU's pg_ts_reset installs the argument styles
 * into the vendored thread-locals before the exec). buf must hold
 * MAXDATELEN+1 (<=160) bytes; returns 0 or the oracle errcode class.
 */
extern int pg_tsdiff_timestamp_out(int64_t ts, int style, int order, int tz, char *buf);
extern int pg_tsdiff_interval_out(int64_t t, int32_t day, int32_t month, int istyle, char *buf);

int
pg_stub_timestamp_out_guc(int64_t ts, char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_tsdiff_timestamp_out(ts, pg_stub_DateStyle, pg_stub_DateOrder,
								   0 /* plain timestamp */, buf);
}

int
pg_stub_interval_out_guc(int64_t t, int32_t day, int32_t month, char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_tsdiff_interval_out(t, day, month, pg_stub_IntervalStyle, buf);
}

/*
 * bloom_create sizing under the pinned work_mem (verbatim bloomfilter.c in
 * pg_libfam_io.c): creates a filter with total_elems/seed under
 * pg_stub_work_mem and returns its bitset size in bits — the work_mem-
 * dependent observable the workmem control compares.
 */
extern void pg_diff_bloom_create(int64_t total_elems, int work_mem, uint64_t seed);
extern uint64_t pg_diff_bloom_m(void);

uint64_t
pg_stub_bloom_m_guc(int64_t total_elems, uint64_t seed)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_diff_bloom_create(total_elems, pg_stub_work_mem, seed);
	return pg_diff_bloom_m();
}
