/* regex_diff oracle shim (p1-lanew): entry points over the verbatim
 * REL_18_3 Spencer regex engine vendored under regexfam/vendor/ (byte-for-
 * byte copy of bench/cref/regex_vendor — regcomp.c + its regc_* includes,
 * regexec.c + rege_dfa.c, regfree.c, regerror.c — plus regprefix.c,
 * regexport.c and regex/regexport.h fetched verbatim from upstream
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp-18.3), which the bench
 * tree did not carry).
 *
 * The engine TUs compile exactly as upstream does (separate TUs — the
 * compile and exec sides both define a `struct vars` and cannot share a
 * TU); this file hosts only the runtime the vendor shim headers declare
 * (interrupt flag, stack-depth check) plus the thin pg_diff_* entries the
 * Rust driver calls. Locale surface: C collation only
 * (PG_REGEX_STRATEGY_C); the BUILTIN/LIBC/ICU arms compile against
 * aborting stubs (vendor/utils, vendor/common) and are covered separately
 * by the exhaustive pg_wc_* class sweeps (see phase1-routes.tsv
 * regex/regex_core rows).
 *
 * Pattern/subject cross this boundary as pg_wchar code points directly —
 * no mb conversion on either side, so both engines see the identical chr
 * sequence (mb parity is owned by the mbutils lanes, not this one).
 *
 * One compiled-RE slot, explicit init/free per iteration; the driver
 * guarantees regcomp-before-exec ordering and always frees. */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

/* EXPLICIT vendor path: since the regexp_diff family landed its own
 * shim postgres.h as this file's directory neighbor, a bare
 * "postgres.h" quoted include would resolve to THAT tree first
 * (quoted-include search starts in the includer's directory) and
 * clash with the vendor headers this TU compiles against. */
#include "vendor/postgres.h"

volatile int cref_InterruptPending = 0;
int			cref_database_encoding = 6; /* PG_UTF8 (GetDatabaseEncoding shim) */

/* Engine live-allocation balance (vendor/postgres.h counting shim; task
 * #150 standing rail).  Per-thread: cargo-test drives the oracles from many
 * threads, and a shared counter would tangle unrelated lanes' engines. */
_Thread_local long cref_engine_live_allocs = 0;

long
pg_diff_regexfam_live_allocs(void)
{
	return cref_engine_live_allocs;
}

/* stack_depth.c shape (see bench/cref/regex_ref.c): base + frame compare.
 * Budget = 2048kB, the REAL SERVER default (guc.c max_stack_depth after the
 * rlimit adjustment every production platform hits) — the bench rig's 100kB
 * boot default fake-fired REG_ETOOBIG at duptraverse depths real PG happily
 * compiles (found day one by regex_diff; ground-truthed on postgres:18.3:
 * the {96}-dup pattern the 100kB guard rejected compiles fine there). The
 * Rust side pins the same 2048kB in the driver init. */
static ssize_t max_stack_depth_bytes = 2048 * (ssize_t) 1024;
/* __thread: cargo-test drives this oracle from many threads; a single
 * global base measured on one thread reads as kilometers-deep from any
 * other and fake-fires REG_ETOOBIG (seen in the first smoke run). Each
 * thread lazily anchors its own base at its first pg_diff_regcomp. */
static __thread char *stack_base_ptr = NULL;

bool
stack_is_too_deep(void)
{
	char		stack_top_loc;
	ssize_t		stack_depth;

	stack_depth = (ssize_t) (stack_base_ptr - &stack_top_loc);

	if (stack_depth < 0)
		stack_depth = -stack_depth;

	if (stack_depth > max_stack_depth_bytes &&
		stack_base_ptr != NULL)
		return true;

	return false;
}

/* Lazy per-thread base anchor, EXPORTED for sibling families that bind this
 * pristine-named engine copy across archives (trgm arm 9's oracle: the
 * build.rs trgmrxfam family — its pg_diff_trgm_* entries never pass through
 * pg_diff_regcomp's lazy anchor below, so the engine's rstacktoodeep
 * measured from a NULL base and the guard was INERT: the 2026-08-03 trgm
 * CONFIRM ASan stack-overflow class, unbounded duptraverse recursion on
 * quantified-alternation patterns). Same contract as the pg_diff_regcomp
 * anchor: first call on a thread wins; the 2048kB budget above is shared. */
void
pg_diff_regex_stack_arm(void)
{
	if (stack_base_ptr == NULL)
		stack_base_ptr = __builtin_frame_address(0);
}

#include "regex/regex.h"
#include "regex/regexport.h"

extern void pg_set_regex_collation(Oid collation);

#define C_COLLATION_OID 950

/* __thread for the same reason as stack_base_ptr: parallel test threads
 * each drive their own compile/exec/free sequence through the slot. */
static __thread regex_t cur_re;
static __thread int have_re = 0;

/* One-time init: stack base for stack_is_too_deep + C collation for the
 * engine's locale layer (regc_pg_locale.c static state). */
void
pg_diff_regex_init(void)
{
	pg_set_regex_collation(C_COLLATION_OID);
}

/* pg_regcomp over pg_wchar code points. Returns the REG_* code; on
 * REG_OKAY the compiled RE is held in the slot and *nsub_out is set. */
int
pg_diff_regcomp(const uint32_t *pattern, int plen, int cflags, int *nsub_out)
{
	int			code;

	if (stack_base_ptr == NULL)
		stack_base_ptr = __builtin_frame_address(0);
	if (have_re)
	{
		pg_regfree(&cur_re);
		have_re = 0;
	}
	code = pg_regcomp(&cur_re, (const pg_wchar *) pattern, (size_t) plen,
					  cflags, C_COLLATION_OID);
	if (code == REG_OKAY)
	{
		have_re = 1;
		*nsub_out = (int) cur_re.re_nsub;
	}
	return code;
}

/* pg_regexec against the slot. so_eo receives nmatch (rm_so, rm_eo) pairs
 * as int64 (pg_regoff_t is long). eflags fixed 0 (the seam contract).
 * details = NULL, exactly the pg_regexec(... , NULL, nmatch, pmatch, 0)
 * shape the Rust seam mirrors. */
int
pg_diff_regexec(const uint32_t *data, int dlen, int search_start,
				int nmatch, int64_t *so_eo)
{
	regmatch_t	pmatch[64];
	int			code;
	int			i;

	if (!have_re || nmatch > 64)
		return -100;			/* driver bug, not an engine verdict */
	code = pg_regexec(&cur_re, (const pg_wchar *) data, (size_t) dlen,
					  (size_t) search_start, NULL,
					  (size_t) nmatch, nmatch > 0 ? pmatch : NULL, 0);
	if (code == REG_OKAY)
		for (i = 0; i < nmatch; i++)
		{
			so_eo[2 * i] = (int64_t) pmatch[i].rm_so;
			so_eo[2 * i + 1] = (int64_t) pmatch[i].rm_eo;
		}
	return code;
}

/* pg_regprefix against the slot: returns the REG_* code; on REG_PREFIX /
 * REG_EXACT writes up to cap chars and the true length to *plen_out. */
int
pg_diff_regprefix(uint32_t *prefix_out, int cap, int *plen_out)
{
	pg_wchar   *string = NULL;	/* chr == pg_wchar (regcustom.h) */
	size_t		slength = 0;
	int			code;
	int			i;

	if (!have_re)
		return -100;
	code = pg_regprefix(&cur_re, &string, &slength);
	if (code == REG_PREFIX || code == REG_EXACT)
	{
		*plen_out = (int) slength;
		for (i = 0; i < (int) slength && i < cap; i++)
			prefix_out[i] = (uint32_t) string[i];
	}
	if (string)
		free(string);			/* vendor MALLOC == malloc */
	return code;
}

void
pg_diff_regfree(void)
{
	if (have_re)
	{
		pg_regfree(&cur_re);
		have_re = 0;
	}
}

/* regerror.c message plane. */
size_t
pg_diff_regerror(int errcode, char *errbuf, size_t errbuf_size)
{
	return pg_regerror(errcode, have_re ? &cur_re : NULL, errbuf, errbuf_size);
}

/* regexport.c plane: serialize the exported NFA view of the slot into a
 * flat int32 array. Layout (mirrored exactly by the Rust driver):
 *   [numstates, initialstate, finalstate, numcolors,
 *    per color co in 1..numcolors-1: (colorisbegin, colorisend, numchars,
 *        min(numchars, MAXCHARS) chars...),
 *    per state st in 0..numstates-1: (numoutarcs, arcs (co,to) pairs...)]
 * Writes at most cap int32s; returns the count written (deterministic
 * truncation — the driver compares the common prefix under the same cap).
 * Color 0 (WHITE) is skipped exactly as pg_trgm does (it has no chars). */
#define EXPORT_MAXCHARS 32

int
pg_diff_reg_export(int32_t *out, int cap)
{
	int			n = 0;
	int			numstates,
				numcolors,
				st,
				co,
				i;

	if (!have_re)
		return -1;
#define PUT(v) do { if (n >= cap) return n; out[n++] = (int32_t) (v); } while (0)
	numstates = pg_reg_getnumstates(&cur_re);
	numcolors = pg_reg_getnumcolors(&cur_re);
	PUT(numstates);
	PUT(pg_reg_getinitialstate(&cur_re));
	PUT(pg_reg_getfinalstate(&cur_re));
	PUT(numcolors);
	for (co = 1; co < numcolors; co++)
	{
		int			nchars = pg_reg_getnumcharacters(&cur_re, co);
		pg_wchar	chars[EXPORT_MAXCHARS];

		PUT(pg_reg_colorisbegin(&cur_re, co));
		PUT(pg_reg_colorisend(&cur_re, co));
		PUT(nchars);
		if (nchars > 0)
		{
			int			take = nchars < EXPORT_MAXCHARS ? nchars : EXPORT_MAXCHARS;

			pg_reg_getcharacters(&cur_re, co, chars, take);
			for (i = 0; i < take; i++)
				PUT(chars[i]);
		}
	}
	for (st = 0; st < numstates; st++)
	{
		int			narcs = pg_reg_getnumoutarcs(&cur_re, st);
		regex_arc_t arcs[64];
		int			take = narcs < 64 ? narcs : 64;

		PUT(narcs);
		pg_reg_getoutarcs(&cur_re, st, arcs, take);
		for (i = 0; i < take; i++)
		{
			PUT(arcs[i].co);
			PUT(arcs[i].to);
		}
	}
#undef PUT
	return n;
}

/* upstream check_stack_depth(): ereport when too deep — shim abort arm,
 * unreachable at the driver's pattern caps (see miscadmin.h note). */
void
check_stack_depth(void)
{
	if (stack_is_too_deep())
		abort();
}
