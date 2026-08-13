/*
 * Vendored PostgreSQL C: the PGLZ decompressor (pglz_decompress) —
 * differential-fuzz oracle for the pgrust `pglz` crate
 * (crates/common/pglz, pglz_decompress). This is the TOAST decompression
 * memory-safety surface: the classic OOB / decompression-bomb class where a
 * malformed compressed stream drives a copy-back-reference loop past the
 * output buffer's start or end.
 *
 * Provenance (body VERBATIM — no edit to a single statement), from the
 * repo's vendored ground-truth checkout
 * ../pgrust-reference/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 ("Stamp 18.3", REL_18):
 *   - src/common/pg_lzcompress.c pglz_decompress() — VERBATIM (the control
 *     byte loop, the two-byte match tag decode, the len==18 extension byte,
 *     the corrupt-data reject (sp > srcend || off == 0 ||
 *     off > dp - dest), the Min(len, destend - dp) clamp, and the
 *     doubling-offset non-overlapping memcpy copy-back loop).
 *
 * WHY THIS TARGET (the highest bug-yield surface of the VENDOR wave): the
 * decode loop copies attacker-controlled back-references from OUTPUT to
 * OUTPUT. A missing/weak bound on the back-reference offset reads before the
 * buffer start; a missing clamp on the match length writes past the end; a
 * truncated stream drives an OOB read of the tag/extension bytes. C guards
 * all three (the reject test + the Min clamp); the pgrust port MUST match
 * every accept/reject verdict AND every output byte. A pgrust panic / OOB /
 * assert where C safely returns -1 is the HIGH-severity finding this lane
 * hunts.
 *
 * lz4/zstd TOAST decompress are NOT vendored here (documented block, VENDOR
 * #882 precedent): pgrust's detoast dispatch is built WITHOUT USE_LZ4
 * (crates/backend/access/common/detoast/src/lib.rs header: "this build has
 * no LZ4, matching C without USE_LZ4"), and the only lz4 in the tree is
 * pgrcolumnar's pure-Rust `lz4_flex` reimplementation, which has no
 * verbatim-C TOAST counterpart to diff against. Vendoring liblz4/libzstd
 * would pull whole external libraries for a path pgrust does not build.
 * See findings-vendor-toast.md.
 *
 * Shims (plumbing only, never logic; stdint-only, no postgres.h tree — the
 * function needs only int32/bool/Min/unlikely/memcpy, the stubshims
 * convention):
 *   - int32 -> int32_t; bool/true/false -> stdbool.
 *   - Min(x,y) / unlikely(x) -> the c.h definitions.
 *   - pglz_decompress renamed (preprocessor only, body verbatim) so this TU
 *     can share the pg_difffuzz_oracle cc::Build without a duplicate symbol
 *     against any future pglz vendoring.
 */

/* FAMILY SYMBOL ISOLATION (pg_int_io.c/pg_varbit_io.c header precedent):
 * rename the verbatim export so ld does not hard-error on a duplicate symbol
 * in the shared pg_difffuzz_oracle build. Preprocessor rename ONLY — the C
 * body below is byte-for-byte the vendored function. */
#define pglz_decompress pglzio_pglz_decompress

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

typedef int32_t int32;

#define Min(x, y)		((x) < (y) ? (x) : (y))
#define unlikely(x)		__builtin_expect((x) != 0, 0)

/* ===================== VERBATIM vendored pglz_decompress ===================== */
/* clang-format off */
int32
pglz_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize, bool check_complete)
{
	const unsigned char *sp;
	const unsigned char *srcend;
	unsigned char *dp;
	unsigned char *destend;

	sp = (const unsigned char *) source;
	srcend = ((const unsigned char *) source) + slen;
	dp = (unsigned char *) dest;
	destend = dp + rawsize;

	while (sp < srcend && dp < destend)
	{
		/*
		 * Read one control byte and process the next 8 items (or as many as
		 * remain in the compressed input).
		 */
		unsigned char ctrl = *sp++;
		int			ctrlc;

		for (ctrlc = 0; ctrlc < 8 && sp < srcend && dp < destend; ctrlc++)
		{
			if (ctrl & 1)
			{
				/*
				 * Set control bit means we must read a match tag. The match
				 * is coded with two bytes. First byte uses lower nibble to
				 * code length - 3. Higher nibble contains upper 4 bits of the
				 * offset. The next following byte contains the lower 8 bits
				 * of the offset. If the length is coded as 18, another
				 * extension tag byte tells how much longer the match really
				 * was (0-255).
				 */
				int32		len;
				int32		off;

				len = (sp[0] & 0x0f) + 3;
				off = ((sp[0] & 0xf0) << 4) | sp[1];
				sp += 2;
				if (len == 18)
					len += *sp++;

				/*
				 * Check for corrupt data: if we fell off the end of the
				 * source, or if we obtained off = 0, or if off is more than
				 * the distance back to the buffer start, we have problems.
				 * (We must check for off = 0, else we risk an infinite loop
				 * below in the face of corrupt data.  Likewise, the upper
				 * limit on off prevents accessing outside the buffer
				 * boundaries.)
				 */
				if (unlikely(sp > srcend || off == 0 ||
							 off > (dp - (unsigned char *) dest)))
					return -1;

				/*
				 * Don't emit more data than requested.
				 */
				len = Min(len, destend - dp);

				/*
				 * Now we copy the bytes specified by the tag from OUTPUT to
				 * OUTPUT (copy len bytes from dp - off to dp).  The copied
				 * areas could overlap, so to avoid undefined behavior in
				 * memcpy(), be careful to copy only non-overlapping regions.
				 *
				 * Note that we cannot use memmove() instead, since while its
				 * behavior is well-defined, it's also not what we want.
				 */
				while (off < len)
				{
					/*
					 * We can safely copy "off" bytes since that clearly
					 * results in non-overlapping source and destination.
					 */
					memcpy(dp, dp - off, off);
					len -= off;
					dp += off;

					/*----------
					 * This bit is less obvious: we can double "off" after
					 * each such step.  Consider this raw input:
					 *		112341234123412341234
					 * This will be encoded as 5 literal bytes "11234" and
					 * then a match tag with length 16 and offset 4.  After
					 * memcpy'ing the first 4 bytes, we will have emitted
					 *		112341234
					 * so we can double "off" to 8, then after the next step
					 * we have emitted
					 *		11234123412341234
					 * Then we can double "off" again, after which it is more
					 * than the remaining "len" so we fall out of this loop
					 * and finish with a non-overlapping copy of the
					 * remainder.  In general, a match tag with off < len
					 * implies that the decoded data has a repeat length of
					 * "off".  We can handle 1, 2, 4, etc repetitions of the
					 * repeated string per memcpy until we get to a situation
					 * where the final copy step is non-overlapping.
					 *
					 * (Another way to understand this is that we are keeping
					 * the copy source point dp - off the same throughout.)
					 *----------
					 */
					off += off;
				}
				memcpy(dp, dp - off, len);
				dp += len;
			}
			else
			{
				/*
				 * An unset control bit means LITERAL BYTE. So we just copy
				 * one from INPUT to OUTPUT.
				 */
				*dp++ = *sp++;
			}

			/*
			 * Advance the control bit
			 */
			ctrl >>= 1;
		}
	}

	/*
	 * If requested, check we decompressed the right amount.
	 */
	if (check_complete && (dp != destend || sp != srcend))
		return -1;

	/*
	 * That's it.
	 */
	return (char *) dp - dest;
}
/* clang-format on */
/* =================== end VERBATIM vendored pglz_decompress =================== */

/*
 * Differential entry point (fuzz plumbing, NOT Postgres code): call the
 * verbatim decompressor over caller-owned buffers and return its result code
 * (>=0 bytes written, or -1 corrupt). The oracle-serialization holder check
 * proves the Rust caller holds the process-global oracle lock (this TU shares
 * the single-threaded pg_difffuzz_oracle build, though pglz_decompress itself
 * is pure — no process-global state).
 */
int32
pg_diff_pglz_decompress(const char *source, int32 slen, char *dest,
						int32 rawsize, int check_complete)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pglzio_pglz_decompress(source, slen, dest, rawsize,
								  check_complete != 0);
}
