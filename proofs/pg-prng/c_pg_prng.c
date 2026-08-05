/*
 * Vendored PostgreSQL C for Kani dual-execution equivalence proofs
 * (compiled via `-Z c-ffi --c-lib`). Same verbatim sections as the
 * fuzz oracle csrc/pg_pg_prng_io.c; provenance:
 * src/common/pg_prng.c lines 37..95, 110..259, 309..318 @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp-18.3).
 * Shims are typedef/macro plumbing only, never logic. Assert() compiled
 * out (NDEBUG parity); harnesses fence preconditions with kani::assume.
 */
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef int32_t int32;
typedef int64_t int64;
typedef uint32_t uint32;
typedef uint64_t uint64;

#define UINT64CONST(x) UINT64_C(x)
#define unlikely(x) (x)
#define likely(x) (x)
#define Assert(x) ((void) 0)

#define PG_INT64_MAX INT64_MAX
#define PG_INT64_MIN INT64_MIN

typedef struct pg_prng_state { uint64 s0, s1; } pg_prng_state;

/* pg_bitutils.h @ 62d6c7d3df, VERBATIM (HAVE__BUILTIN_CLZ + SIZEOF_LONG==8 arm) */
static inline int
pg_leftmost_one_pos64(uint64 word)
{
	Assert(word != 0);

	return 63 - __builtin_clzl(word);
}
extern bool pg_prng_seed_check(pg_prng_state *state);

/* ==== VERBATIM src/common/pg_prng.c (rotl..pg_prng_seed) ==== */
/*
 * 64-bit rotate left
 */
static inline uint64
rotl(uint64 x, int bits)
{
	return (x << bits) | (x >> (64 - bits));
}

/*
 * The basic xoroshiro128** algorithm.
 * Generates and returns a 64-bit uniformly distributed number,
 * updating the state vector for next time.
 *
 * Note: the state vector must not be all-zeroes, as that is a fixed point.
 */
static uint64
xoroshiro128ss(pg_prng_state *state)
{
	uint64		s0 = state->s0,
				sx = state->s1 ^ s0,
				val = rotl(s0 * 5, 7) * 9;

	/* update state */
	state->s0 = rotl(s0, 24) ^ sx ^ (sx << 16);
	state->s1 = rotl(sx, 37);

	return val;
}

/*
 * We use this generator just to fill the xoroshiro128** state vector
 * from a 64-bit seed.
 */
static uint64
splitmix64(uint64 *state)
{
	/* state update */
	uint64		val = (*state += UINT64CONST(0x9E3779B97f4A7C15));

	/* value extraction */
	val = (val ^ (val >> 30)) * UINT64CONST(0xBF58476D1CE4E5B9);
	val = (val ^ (val >> 27)) * UINT64CONST(0x94D049BB133111EB);

	return val ^ (val >> 31);
}

/*
 * Initialize the PRNG state from a 64-bit integer,
 * taking care that we don't produce all-zeroes.
 */
void
pg_prng_seed(pg_prng_state *state, uint64 seed)
{
	state->s0 = splitmix64(&seed);
	state->s1 = splitmix64(&seed);
	/* Let's just make sure we didn't get all-zeroes */
	(void) pg_prng_seed_check(state);
}

/* ==== VERBATIM (pg_prng_seed_check..pg_prng_int32p; fseed/double arms live in the fuzz oracle — libm-free proof surface only) ==== */
/*
 * Validate a PRNG seed value.
 */
bool
pg_prng_seed_check(pg_prng_state *state)
{
	/*
	 * If the seeding mechanism chanced to produce all-zeroes, insert
	 * something nonzero.  Anything would do; use Knuth's LCG parameters.
	 */
	if (unlikely(state->s0 == 0 && state->s1 == 0))
	{
		state->s0 = UINT64CONST(0x5851F42D4C957F2D);
		state->s1 = UINT64CONST(0x14057B7EF767814F);
	}

	/* As a convenience for the pg_prng_strong_seed macro, return true */
	return true;
}

/*
 * Select a random uint64 uniformly from the range [0, PG_UINT64_MAX].
 */
uint64
pg_prng_uint64(pg_prng_state *state)
{
	return xoroshiro128ss(state);
}

/*
 * Select a random uint64 uniformly from the range [rmin, rmax].
 * If the range is empty, rmin is always produced.
 */
uint64
pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax)
{
	uint64		val;

	if (likely(rmax > rmin))
	{
		/*
		 * Use bitmask rejection method to generate an offset in 0..range.
		 * Each generated val is less than twice "range", so on average we
		 * should not have to iterate more than twice.
		 */
		uint64		range = rmax - rmin;
		uint32		rshift = 63 - pg_leftmost_one_pos64(range);

		do
		{
			val = xoroshiro128ss(state) >> rshift;
		} while (val > range);
	}
	else
		val = 0;

	return rmin + val;
}

/*
 * Select a random int64 uniformly from the range [PG_INT64_MIN, PG_INT64_MAX].
 */
int64
pg_prng_int64(pg_prng_state *state)
{
	return (int64) xoroshiro128ss(state);
}

/*
 * Select a random int64 uniformly from the range [0, PG_INT64_MAX].
 */
int64
pg_prng_int64p(pg_prng_state *state)
{
	return (int64) (xoroshiro128ss(state) & UINT64CONST(0x7FFFFFFFFFFFFFFF));
}

/*
 * Select a random int64 uniformly from the range [rmin, rmax].
 * If the range is empty, rmin is always produced.
 */
int64
pg_prng_int64_range(pg_prng_state *state, int64 rmin, int64 rmax)
{
	int64		val;

	if (likely(rmax > rmin))
	{
		uint64		uval;

		/*
		 * Use pg_prng_uint64_range().  Can't simply pass it rmin and rmax,
		 * since (uint64) rmin will be larger than (uint64) rmax if rmin < 0.
		 */
		uval = (uint64) rmin +
			pg_prng_uint64_range(state, 0, (uint64) rmax - (uint64) rmin);

		/*
		 * Safely convert back to int64, avoiding implementation-defined
		 * behavior for values larger than PG_INT64_MAX.  Modern compilers
		 * will reduce this to a simple assignment.
		 */
		if (uval > PG_INT64_MAX)
			val = (int64) (uval - PG_INT64_MIN) + PG_INT64_MIN;
		else
			val = (int64) uval;
	}
	else
		val = rmin;

	return val;
}

/*
 * Select a random uint32 uniformly from the range [0, PG_UINT32_MAX].
 */
uint32
pg_prng_uint32(pg_prng_state *state)
{
	/*
	 * Although xoroshiro128** is not known to have any weaknesses in
	 * randomness of low-order bits, we prefer to use the upper bits of its
	 * result here and below.
	 */
	uint64		v = xoroshiro128ss(state);

	return (uint32) (v >> 32);
}

/*
 * Select a random int32 uniformly from the range [PG_INT32_MIN, PG_INT32_MAX].
 */
int32
pg_prng_int32(pg_prng_state *state)
{
	uint64		v = xoroshiro128ss(state);

	return (int32) (v >> 32);
}

/*
 * Select a random int32 uniformly from the range [0, PG_INT32_MAX].
 */
int32
pg_prng_int32p(pg_prng_state *state)
{
	uint64		v = xoroshiro128ss(state);

	return (int32) (v >> 33);
}

/* ==== VERBATIM (pg_prng_bool) ==== */
/*
 * Select a random boolean value.
 */
bool
pg_prng_bool(pg_prng_state *state)
{
	uint64		v = xoroshiro128ss(state);

	return (bool) (v >> 63);
}

/* ==== proof-facing scalar wrappers (plumbing only, NOT Postgres code):
 * goto-cc rejects cross-language struct-pointer declarations, so state
 * crosses the FFI as (s0, s1) scalars exactly like the fuzz oracle. ==== */

int c_prng_seed(uint64 seed, uint64 *out_s0, uint64 *out_s1)
{
	pg_prng_state st = {0, 0};
	pg_prng_seed(&st, seed);
	*out_s0 = st.s0; *out_s1 = st.s1;
	return 0; /* Rust unit () cannot link against C void under goto-cc */
}

int c_prng_seed_check(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{
	pg_prng_state st = {s0, s1};
	int r = pg_prng_seed_check(&st) ? 1 : 0;
	*out_s0 = st.s0; *out_s1 = st.s1;
	return r;
}

uint64 c_prng_u64(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; uint64 v = pg_prng_uint64(&st); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

uint64 c_prng_u64_range(uint64 s0, uint64 s1, uint64 rmin, uint64 rmax, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; uint64 v = pg_prng_uint64_range(&st, rmin, rmax); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

int64 c_prng_i64(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; int64 v = pg_prng_int64(&st); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

int64 c_prng_i64p(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; int64 v = pg_prng_int64p(&st); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

int64 c_prng_i64_range(uint64 s0, uint64 s1, int64 rmin, int64 rmax, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; int64 v = pg_prng_int64_range(&st, rmin, rmax); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

uint32 c_prng_u32(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; uint32 v = pg_prng_uint32(&st); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

int32 c_prng_i32(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; int32 v = pg_prng_int32(&st); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

int32 c_prng_i32p(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; int32 v = pg_prng_int32p(&st); *out_s0 = st.s0; *out_s1 = st.s1; return v; }

int c_prng_bool(uint64 s0, uint64 s1, uint64 *out_s0, uint64 *out_s1)
{ pg_prng_state st = {s0, s1}; int v = pg_prng_bool(&st) ? 1 : 0; *out_s0 = st.s0; *out_s1 = st.s1; return v; }
