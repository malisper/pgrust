/* SHIM common/pg_prng.h — numeric.h pulls it for random-function protos
 * only; nothing PRNG-shaped is reachable in this oracle. */
#ifndef PG_JSONBFAM_SHIM_PG_PRNG_H
#define PG_JSONBFAM_SHIM_PG_PRNG_H
typedef struct pg_prng_state { uint64 s0, s1; } pg_prng_state;
#endif
