/*
 * pg_stub_state.h — extern declarations for the shared stub-pin facility
 * (state + setters defined in pg_stub_state.c; Rust half in
 * fuzz/core/src/stubs.rs; usage contract in fuzz/STUBS.md).
 *
 * A NEW oracle TU that wants pinned session state includes this header and
 * reads the pg_stub_* globals (or maps its vendored global names onto them,
 * e.g. `#define extra_float_digits pg_stub_extra_float_digits` before the
 * verbatim paste) instead of defining another per-TU copy. The Rust driver
 * pins values through fuzz/core/src/stubs.rs, which derives each value once
 * from the fuzz input (bounded to the GUC's legal range) and writes it to
 * BOTH sides.
 */
#ifndef PG_STUB_STATE_H
#define PG_STUB_STATE_H

#include <stdint.h>

/* stub:guc */
extern _Thread_local int pg_stub_extra_float_digits;
extern _Thread_local int pg_stub_DateStyle;
extern _Thread_local int pg_stub_DateOrder;
extern _Thread_local int pg_stub_IntervalStyle;
extern _Thread_local int pg_stub_standard_conforming_strings;

void		pg_stub_set_extra_float_digits(int v);
int			pg_stub_get_extra_float_digits(void);
void		pg_stub_set_datestyle(int style, int order);
int			pg_stub_get_datestyle(void);
int			pg_stub_get_dateorder(void);
void		pg_stub_set_intervalstyle(int istyle);
int			pg_stub_get_intervalstyle(void);
void		pg_stub_set_standard_conforming_strings(int on);
int			pg_stub_get_standard_conforming_strings(void);

/* stub:guc — cryptbe family channels */
extern _Thread_local int pg_stub_md5_password_warnings;
extern _Thread_local int pg_stub_scram_iterations;

void		pg_stub_set_md5_password_warnings(int on);
int			pg_stub_get_md5_password_warnings(void);
void		pg_stub_set_scram_iterations(int iters);
int			pg_stub_get_scram_iterations(void);

/* stub:prng — scram-salt channel (pg_strong_random-shaped entropy read) */
extern _Thread_local uint8_t pg_stub_scram_salt[16];

void		pg_stub_set_scram_salt(const uint8_t *salt16);
void		pg_stub_get_scram_salt(uint8_t *out16);

/* stub:clock */
extern _Thread_local int64_t pg_stub_now_usecs;

void		pg_stub_set_current_timestamp(int64_t usecs);
int64_t		pg_stub_get_current_timestamp(void);

/* stub:clock monotonic half (INSTR_TIME_SET_CURRENT analog) */
extern _Thread_local uint64_t pg_stub_mono_ns_val;

void		pg_stub_set_mono_ns(uint64_t ns);
uint64_t	pg_stub_get_mono_ns(void);

/* stub:prng (verbatim vendored xoroshiro128** engine, global-state analog) */
void		pg_stub_prng_seed(uint64_t seed);
uint64_t	pg_stub_prng_u64(void);
double		pg_stub_prng_double(void);

/* stub:workmem */
extern _Thread_local int pg_stub_work_mem;
extern _Thread_local int pg_stub_maintenance_work_mem;

void		pg_stub_set_work_mem(int work_mem_kb, int maintenance_work_mem_kb);
int			pg_stub_get_work_mem(void);
int			pg_stub_get_maintenance_work_mem(void);

/* consumer wrappers (vendored consumers under the pinned globals) */
int			pg_stub_float8out_guc(double num, char *buf32);
int			pg_stub_timestamp_out_guc(int64_t ts, char *buf);
int			pg_stub_interval_out_guc(int64_t t, int32_t day, int32_t month, char *buf);
uint64_t	pg_stub_bloom_m_guc(int64_t total_elems, uint64_t seed);

#endif							/* PG_STUB_STATE_H */
