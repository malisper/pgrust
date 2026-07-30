/*
 * Vendored PostgreSQL C for the text/bytea length + slicing family Kani
 * parity proofs (proofs/text-slice).
 *
 * Provenance (postgres/postgres REL_18_STABLE, fetched 2026-07-28):
 *   - src/backend/utils/adt/varlena.c: text_length (textlen), textoctetlen,
 *     text_catenate (textcat), byteaoctetlen, bytea_catenate (byteacat),
 *     text_substring (text_substr / text_substr_no_len),
 *     pg_mbcharcliplen_chars, bytea_substring (bytea_substr / _no_len),
 *     text_position + text_position_setup / text_position_next /
 *     text_position_next_internal / text_position_get_match_pos (textpos,
 *     strpos), text_starts_with, byteapos, text_left, text_right.
 *   - src/backend/utils/mb/mbutils.c: pg_mblen_with_len, pg_mblen_range,
 *     pg_mblen_unbounded, pg_mbstrlen_with_len, pg_mbcharcliplen, cliplen,
 *     pg_database_encoding_max_length, GetDatabaseEncoding.
 *   - src/common/wchar.c: pg_utf_mblen, pg_latin1_mblen.
 *   - src/backend/access/common/detoast.c: detoast_attr_slice — the
 *     inline-image tail only (pg_slice_inline): the DatumGetTextPSlice /
 *     DatumGetByteaPSlice read path for non-external, non-compressed
 *     images, the only forms the harnesses construct.
 *
 * SHIMS (everything else is verbatim):
 *  1. Names pg_-prefixed; typedefs inlined (int32 -> int, Size -> size_t,
 *     Oid -> unsigned int, bool -> int). text/bytea arguments ride as
 *     (const unsigned char *data, int len) payload pairs modeling the
 *     post-PG_GETARG_*_PP / inline-image caller contract; DETOASTING of
 *     external/compressed images is OUT OF SCOPE (established varlena
 *     pattern: proofs/bytea-cmp, proofs/text-cmp shim 2).
 *  2. toast_raw_datum_size(d) -> payload len + VARHDRSZ (raw size of an
 *     inline uncompressed image; proofs/text-cmp shim 3).
 *  3. palloc'd results -> caller-provided out buffers; string-returning
 *     functions return the payload byte length written.
 *  4. ereport/elog -> pg_errflag = <error class below> + PG_CERR sentinel
 *     return, propagated through callers (models C's longjmp unwind).
 *     Harnesses compare error verdict + class against the Rust PgError
 *     sqlstate. Classes: 1 = ERRCODE_SUBSTRING_ERROR (22011),
 *     2 = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (22021, invalid byte seq),
 *     3 = ERRCODE_INDETERMINATE_COLLATION (42P22),
 *     4 = ERRCODE_FEATURE_NOT_SUPPORTED (0A000, nondeterministic search).
 *  5. Encoding state: DatabaseEncoding / pg_wchar_table -> pg_db_encoding
 *     global (pg_set_db_encoding) + a two-encoding mblen dispatch
 *     (PG_UTF8 -> pg_utf_mblen verbatim, PG_LATIN1 -> pg_latin1_mblen
 *     verbatim; maxmblen 4/1 per the pg_wchar_table rows). ENCODING FENCE:
 *     harnesses set one of {PG_UTF8, PG_LATIN1} on both sides; all other
 *     encodings are out of proof scope.
 *  6. COLLATION FENCE (proofs/text-cmp shim 4): the
 *     pg_newlocale_from_collation(collid)->deterministic read -> true for
 *     the built-in C/POSIX collation oids, POISON otherwise; the
 *     pg_strncoll nondeterministic-search arm poisons (errflag 99).
 *     Harnesses fence collid == C_COLLATION_OID.
 *  7. pg_add_s32_overflow / pg_mul_s32_overflow -> __builtin_*_overflow,
 *     exactly how src/include/common/int.h defines them under gcc/clang.
 *  8. memcmp/memcpy are CBMC's built-in models.
 *  9. Assert -> no-op (compiled out of production builds); the
 *     VALGRIND_* client requests -> no-op.
 * 10. text_position machinery: `char *` cursors -> int offsets into the
 *     haystack (mechanical; avoids CBMC pointer-arithmetic-past-object
 *     noise on the B-M-H skip which can step beyond one-past-end before
 *     the loop guard rejects it). All comparisons/updates are the verbatim
 *     expressions rewritten offset-wise; the Rust port under proof is
 *     offset-based too.
 */

#include <stddef.h>
#include <string.h>

typedef unsigned int Oid;

#define VARHDRSZ 4
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))

#define PG_UTF8 6
#define PG_LATIN1 8

#define C_COLLATION_OID 950
#define POSIX_COLLATION_OID 951

/* shim 4: error model */
#define PG_CERR (-2100000000)
#define PG_E_SUBSTRING 1
#define PG_E_BADSEQ 2
#define PG_E_INDET_COLL 3
#define PG_E_NONDET 4
#define PG_E_POISON 99

static int pg_errflag = 0;

int
pg_take_err(void)
{
	int			e = pg_errflag;

	pg_errflag = 0;
	return e;
}

/* shim 7: src/include/common/int.h (gcc/clang arm, verbatim) */
static inline int
pg_add_s32_overflow(int a, int b, int *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline int
pg_mul_s32_overflow(int a, int b, int *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ---------------- shim 5: encoding state ---------------- */

static int pg_db_encoding = PG_UTF8;

/* int return: Kani lowers Rust () as `struct Unit`, which goto-cc rejects
 * against C void (prove-target trap) */
int
pg_set_db_encoding(int enc)
{
	pg_db_encoding = enc;
	return 0;
}

/* src/common/wchar.c pg_utf_mblen, verbatim (NOT_USED arms elided as in C) */
static int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
	else
		len = 1;
	return len;
}

/* src/common/wchar.c pg_latin1_mblen, verbatim */
static int
pg_latin1_mblen(const unsigned char *s)
{
	return 1;
}

/* pg_wchar_table[DatabaseEncoding->encoding].mblen dispatch (shim 5) */
static int
pg_enc_mblen(const unsigned char *s)
{
	return (pg_db_encoding == PG_UTF8) ? pg_utf_mblen(s) : pg_latin1_mblen(s);
}

/* pg_wchar_table maxmblen column: UTF8 = 4, LATIN1 = 1 (shim 5) */
static int
pg_database_encoding_max_length(void)
{
	return (pg_db_encoding == PG_UTF8) ? 4 : 1;
}

static int
GetDatabaseEncoding(void)
{
	return pg_db_encoding;
}

/* ---------------- mbutils.c (REL_18) ---------------- */

/* pg_mblen_with_len: ereport(invalid byte sequence) -> shim 4 */
static int
pg_mblen_with_len(const unsigned char *mbstr, int limit)
{
	int			length = pg_enc_mblen(mbstr);

	if (length > limit)
	{
		pg_errflag = PG_E_BADSEQ;
		return PG_CERR;
	}
	return length;
}

/* pg_mblen_range(mbstr, end): the (mbstr, remaining) offset form (shim 10) */
static int
pg_mblen_range_n(const unsigned char *mbstr, int remaining)
{
	int			length = pg_enc_mblen(mbstr);

	if (length > remaining)
	{
		pg_errflag = PG_E_BADSEQ;
		return PG_CERR;
	}
	return length;
}

static int
pg_mblen_unbounded(const unsigned char *mbstr)
{
	return pg_enc_mblen(mbstr);
}

/* pg_mbstrlen_with_len, verbatim + err propagation (shim 4) */
static int
pg_mbstrlen_with_len(const unsigned char *mbstr, int limit)
{
	int			len = 0;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return limit;

	while (limit > 0 && *mbstr)
	{
		int			l = pg_mblen_with_len(mbstr, limit);

		if (l == PG_CERR)
			return PG_CERR;
		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

/* mbutils.c cliplen, verbatim */
static int
cliplen(const unsigned char *str, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && str[l])
		l++;
	return l;
}

/* pg_mbcharcliplen, verbatim + err propagation (shim 4) */
static int
pg_mbcharcliplen(const unsigned char *mbstr, int len, int limit)
{
	int			clen = 0;
	int			nch = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return cliplen(mbstr, len, limit);

	while (len > 0 && *mbstr)
	{
		l = pg_mblen_with_len(mbstr, len);
		if (l == PG_CERR)
			return PG_CERR;
		nch++;
		if (nch > limit)
			break;
		clen += l;
		len -= l;
		mbstr += l;
	}
	return clen;
}

/* ---------------- shim 6: collation fence ---------------- */

static int
pg_collation_is_set(Oid collid)
{
	return collid != 0;			/* OidIsValid */
}

static int
pg_collate_deterministic(Oid collid)
{
	if (collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID)
		return 1;
	/* out-of-fence collations poison: harnesses never pass them */
	pg_errflag = PG_E_POISON;
	return 0;
}

/* ------------- detoast.c detoast_attr_slice, inline tail ------------- */

/*
 * The slice clamp + copy for a non-external, non-compressed image
 * (attrdata/attrsize already extracted from the header by the caller
 * contract). External/compressed arms are out of scope (shim 1); the
 * verbatim clamp below is detoast.c:205's tail.
 */
static int
pg_slice_inline(const unsigned char *attrdata, int attrsize,
				int sliceoffset, int slicelength, unsigned char *out)
{
	int			slicelimit;

	if (sliceoffset < 0)
	{
		/* elog(ERROR, "invalid sliceoffset") — unreachable from callers */
		pg_errflag = PG_E_POISON;
		return PG_CERR;
	}

	/*
	 * Compute slicelimit = offset + length, or -1 if we must fetch all of
	 * the value.  In case of integer overflow, we must fetch all.
	 */
	if (slicelength < 0)
		slicelimit = -1;
	else if (pg_add_s32_overflow(sliceoffset, slicelength, &slicelimit))
		slicelength = slicelimit = -1;

	/* slicing of datum for compressed cases and plain value */
	if (sliceoffset >= attrsize)
	{
		sliceoffset = 0;
		slicelength = 0;
	}
	else if (slicelength < 0 || slicelimit > attrsize)
		slicelength = attrsize - sliceoffset;

	if (slicelength > 0)
		memcpy(out, attrdata + sliceoffset, slicelength);
	return slicelength;
}

/* ---------------- varlena.c: lengths ---------------- */

/* text_length / textlen: toast_raw_datum_size -> len + VARHDRSZ (shim 2) */
int
pg_textlen(const unsigned char *d, int len)
{
	/* fastpath when max encoding length is one */
	if (pg_database_encoding_max_length() == 1)
		return (len + VARHDRSZ) - VARHDRSZ;
	else
		return pg_mbstrlen_with_len(d, len);
}

/* textoctetlen (shim 2) */
int
pg_textoctetlen(int len)
{
	return (len + VARHDRSZ) - VARHDRSZ;
}

/* byteaoctetlen (shim 2) */
int
pg_byteaoctetlen(int len)
{
	return (len + VARHDRSZ) - VARHDRSZ;
}

/* ---------------- varlena.c: catenate ---------------- */

/* text_catenate (textcat), verbatim; palloc -> out (shim 3) */
int
pg_text_catenate(const unsigned char *d1, int len1,
				 const unsigned char *d2, int len2, unsigned char *out)
{
	int			len;
	unsigned char *ptr;

	/* paranoia ... probably should throw error instead? */
	if (len1 < 0)
		len1 = 0;
	if (len2 < 0)
		len2 = 0;

	len = len1 + len2 + VARHDRSZ;

	/* Fill data field of result string... */
	ptr = out;
	if (len1 > 0)
		memcpy(ptr, d1, len1);
	if (len2 > 0)
		memcpy(ptr + len1, d2, len2);

	return len - VARHDRSZ;
}

/* bytea_catenate (byteacat) is byte-identical to text_catenate */
int
pg_bytea_catenate(const unsigned char *d1, int len1,
				  const unsigned char *d2, int len2, unsigned char *out)
{
	return pg_text_catenate(d1, len1, d2, len2, out);
}

/* ---------------- varlena.c: text_substring ---------------- */

/* pg_mbcharcliplen_chars (varlena.c), verbatim + err propagation */
static int
pg_mbcharcliplen_chars(const unsigned char *mbstr, int len, int limit)
{
	int			nch = 0;
	int			l;

	while (len > 0 && *mbstr)
	{
		l = pg_mblen_with_len(mbstr, len);
		if (l == PG_CERR)
			return PG_CERR;
		nch++;
		if (nch == limit)
			break;
		len -= l;
		mbstr += l;
	}
	return nch;
}

/*
 * text_substring, verbatim structure. `str` rides as (d, len): an INLINE
 * uncompressed image's payload (shim 1) — the VARATT_IS_COMPRESSED /
 * VARATT_IS_EXTERNAL slice-fetch arm is statically dead for such images
 * (slice = the argument itself), exactly as the Rust port's `sliced = None`
 * arm. DatumGetTextPSlice -> pg_slice_inline (detoast_attr_slice tail).
 * cstring_to_text("") -> return 0. Result -> out (shim 3).
 */
int
pg_text_substring(const unsigned char *d, int len, int start, int length,
				  int length_not_specified, unsigned char *out)
{
	int			eml = pg_database_encoding_max_length();
	int			S = start;		/* start position */
	int			S1;				/* adjusted start position */
	int			L1;				/* adjusted substring length */
	int			E;				/* end position, exclusive */

	S1 = Max(S, 1);

	/* life is easy if the encoding max length is 1 */
	if (eml == 1)
	{
		if (length_not_specified)	/* special case - get length to end of
									 * string */
			L1 = -1;
		else if (length < 0)
		{
			/* SQL99 says to throw an error for E < S, i.e., negative length */
			pg_errflag = PG_E_SUBSTRING;
			return PG_CERR;
		}
		else if (pg_add_s32_overflow(S, length, &E))
		{
			/*
			 * L could be large enough for S + L to overflow, in which case
			 * the substring must run to end of string.
			 */
			L1 = -1;
		}
		else
		{
			/*
			 * A zero or negative value for the end position can happen if
			 * the start was negative or one. SQL99 says to return a
			 * zero-length string.
			 */
			if (E < 1)
				return 0;		/* cstring_to_text("") */

			L1 = E - S1;
		}

		/*
		 * If the start position is past the end of the string, SQL99 says
		 * to return a zero-length string -- DatumGetTextPSlice() will do
		 * that for us.  We need only convert S1 to zero-based starting
		 * position.
		 */
		return pg_slice_inline(d, len, S1 - 1, L1, out);
	}
	else						/* eml > 1 */
	{
		int			slice_start;
		int			slice_size;
		int			slice_strlen;
		int			slice_len;
		int			E1;
		int			i;
		int			p;			/* offset cursor (shim 10) */
		int			s;

		/*
		 * We need to start at position zero because there is no way to know
		 * in advance which byte offset corresponds to the supplied start
		 * position.
		 */
		slice_start = 0;

		if (length_not_specified)	/* special case - get length to end of
									 * string */
			E = slice_size = L1 = -1;
		else if (length < 0)
		{
			/* SQL99 says to throw an error for E < S, i.e., negative length */
			pg_errflag = PG_E_SUBSTRING;
			return PG_CERR;
		}
		else if (pg_add_s32_overflow(S, length, &E))
		{
			slice_size = L1 = -1;
		}
		else
		{
			/*
			 * Ending at position 1, exclusive, obviously yields an empty
			 * string.
			 */
			if (E <= 1)
				return 0;		/* cstring_to_text("") */

			L1 = E - S1;

			/*
			 * Total slice size in bytes can't be any longer than the
			 * inclusive end position times the encoding max length.  If
			 * that overflows, we can just use -1.
			 */
			if (pg_mul_s32_overflow(E - 1, eml, &slice_size))
				slice_size = -1;
		}

		/*
		 * Inline uncompressed source: no extra copying step (the
		 * compressed/external DatumGetTextPSlice arm is out of scope,
		 * shim 1): slice = the argument.
		 */
		slice_len = len;

		/* see if we got back an empty string */
		if (slice_len == 0)
			return 0;			/* cstring_to_text("") */

		/*
		 * Now we can get the actual length of the slice in MB characters,
		 * stopping at the end of the substring.
		 */
		slice_strlen = (slice_size == -1 ?
						pg_mbstrlen_with_len(d, slice_len) :
						pg_mbcharcliplen_chars(d, slice_len, E - 1));
		if (slice_strlen == PG_CERR)
			return PG_CERR;

		/*
		 * Check that the start position wasn't > slice_strlen. If so, SQL99
		 * says to return a zero-length string.
		 */
		if (S1 > slice_strlen)
			return 0;			/* cstring_to_text("") */

		/*
		 * Adjust L1 and E1 now that we know the slice string length. Again
		 * remember that S1 is one based, and slice_start is zero based.
		 */
		if (L1 > -1)
			E1 = Min(S1 + L1, slice_start + 1 + slice_strlen);
		else
			E1 = slice_start + 1 + slice_strlen;

		/*
		 * Find the start position in the slice; remember S1 is not zero
		 * based.
		 */
		p = 0;
		for (i = 0; i < S1 - 1; i++)
			p += pg_mblen_unbounded(d + p);

		/* hang onto a pointer to our start position */
		s = p;

		/*
		 * Count the actual bytes used by the substring of the requested
		 * length.
		 */
		for (i = S1; i < E1; i++)
			p += pg_mblen_unbounded(d + p);

		memcpy(out, d + s, p - s);
		return p - s;
	}
}

/* ---------------- varlena.c: bytea_substring ---------------- */

int
pg_bytea_substring(const unsigned char *d, int len, int S, int L,
				   int length_not_specified, unsigned char *out)
{
	int			S1;				/* adjusted start position */
	int			L1;				/* adjusted substring length */
	int			E;				/* end position */

	/*
	 * The logic here should generally match text_substring().
	 */
	S1 = Max(S, 1);

	if (length_not_specified)
	{
		/*
		 * Not passed a length - DatumGetByteaPSlice() grabs everything to
		 * the end of the string if we pass it a negative value for length.
		 */
		L1 = -1;
	}
	else if (L < 0)
	{
		/* SQL99 says to throw an error for E < S, i.e., negative length */
		pg_errflag = PG_E_SUBSTRING;
		return PG_CERR;
	}
	else if (pg_add_s32_overflow(S, L, &E))
	{
		/*
		 * L could be large enough for S + L to overflow, in which case the
		 * substring must run to end of string.
		 */
		L1 = -1;
	}
	else
	{
		/*
		 * A zero or negative value for the end position can happen if the
		 * start was negative or one. SQL99 says to return a zero-length
		 * string.
		 */
		if (E < 1)
			return 0;			/* PG_STR_GET_BYTEA("") */

		L1 = E - S1;
	}

	/*
	 * If the start position is past the end of the string, SQL99 says to
	 * return a zero-length string -- DatumGetByteaPSlice() will do that for
	 * us.  We need only convert S1 to zero-based starting position.
	 */
	return pg_slice_inline(d, len, S1 - 1, L1, out);
}

/* ---------------- varlena.c: text_position family ---------------- */

typedef struct
{
	const unsigned char *str1;	/* haystack */
	const unsigned char *str2;	/* needle */
	int			len1;
	int			len2;
	int			last_match_len;
	int			last_match_len_tmp;
	int			is_multibyte_char_in_char;
	int			greedy;
	int			last_match;		/* offset, -1 = none (shim 10) */
	int			refpoint;		/* offset (shim 10) */
	int			refpos;
	int			skiptablemask;
	int			skiptable[256];
	Oid			collid;
} PgTextPositionState;

/* text_position_setup, verbatim (locale fence: shim 6; offsets: shim 10) */
static int
pg_text_position_setup(const unsigned char *t1, int len1,
					   const unsigned char *t2, int len2,
					   Oid collid, PgTextPositionState *state)
{
	if (!pg_collation_is_set(collid))
	{
		pg_errflag = PG_E_INDET_COLL;
		return PG_CERR;
	}

	state->collid = collid;
	state->greedy = 1;

	if (pg_database_encoding_max_length() == 1)
		state->is_multibyte_char_in_char = 0;
	else if (GetDatabaseEncoding() == PG_UTF8)
		state->is_multibyte_char_in_char = 0;
	else
		state->is_multibyte_char_in_char = 1;

	state->str1 = t1;
	state->str2 = t2;
	state->len1 = len1;
	state->len2 = len2;
	state->last_match = -1;
	state->refpoint = 0;
	state->refpos = 0;
	state->skiptablemask = 0;

	if (len1 >= len2 && len2 > 1 && pg_collate_deterministic(collid))
	{
		int			searchlength = len1 - len2;
		int			skiptablemask;
		int			last;
		int			i;
		const unsigned char *str2 = state->str2;

		if (searchlength < 16)
			skiptablemask = 3;
		else if (searchlength < 64)
			skiptablemask = 7;
		else if (searchlength < 128)
			skiptablemask = 15;
		else if (searchlength < 512)
			skiptablemask = 31;
		else if (searchlength < 2048)
			skiptablemask = 63;
		else if (searchlength < 4096)
			skiptablemask = 127;
		else
			skiptablemask = 255;
		state->skiptablemask = skiptablemask;

		for (i = 0; i <= skiptablemask; i++)
			state->skiptable[i] = len2;

		last = len2 - 1;

		for (i = 0; i < last; i++)
			state->skiptable[str2[i] & skiptablemask] = last - i;
	}
	return 0;
}

/*
 * text_position_next_internal, verbatim (offsets: shim 10). Returns match
 * offset, -1 = no match, PG_CERR = error. The nondeterministic-collation
 * arm (pg_strncoll walk) is out of the collation fence and poisons.
 */
static int
pg_text_position_next_internal(int start_off, PgTextPositionState *state)
{
	int			haystack_len = state->len1;
	int			needle_len = state->len2;
	int			skiptablemask = state->skiptablemask;
	const unsigned char *haystack = state->str1;
	const unsigned char *needle = state->str2;
	int			hptr;

	state->last_match_len_tmp = needle_len;

	if (!pg_collate_deterministic(state->collid))
	{
		/* nondeterministic pg_strncoll search: out of proof scope (shim 6) */
		pg_errflag = PG_E_POISON;
		return PG_CERR;
	}
	else if (needle_len == 1)
	{
		/* No point in using B-M-H for a one-character needle */
		unsigned char nchar = needle[0];

		hptr = start_off;
		while (hptr < haystack_len)
		{
			if (haystack[hptr] == nchar)
				return hptr;
			hptr++;
		}
	}
	else
	{
		int			needle_last = needle_len - 1;

		/* Start at startpos plus the length of the needle */
		hptr = start_off + needle_len - 1;
		while (hptr < haystack_len)
		{
			/* Match the needle scanning *backward* */
			int			nptr;
			int			p;

			nptr = needle_last;
			p = hptr;
			while (needle[nptr] == haystack[p])
			{
				/* Matched it all?	If so, return 1-based position */
				if (nptr == 0)
					return p;
				nptr--, p--;
			}

			hptr += state->skiptable[haystack[hptr] & skiptablemask];
		}
	}

	return -1;					/* not found */
}

/* text_position_next, verbatim (offsets: shim 10; err propagation shim 4) */
static int
pg_text_position_next(PgTextPositionState *state)
{
	int			needle_len = state->len2;
	int			start_off;
	int			matchoff;

	if (needle_len <= 0)
		return 0;				/* result for empty pattern */

	/* Start from the point right after the previous match. */
	if (state->last_match >= 0)
		start_off = state->last_match + state->last_match_len;
	else
		start_off = 0;

retry:
	matchoff = pg_text_position_next_internal(start_off, state);

	if (matchoff == PG_CERR)
		return PG_CERR;
	if (matchoff < 0)
		return 0;

	/*
	 * Found a match for the byte sequence.  If this is a multibyte
	 * encoding, where one character's byte sequence can appear inside a
	 * longer multi-byte character, we need to verify that the match was at
	 * a character boundary, not in the middle of a multi-byte character.
	 */
	if (state->is_multibyte_char_in_char && pg_collate_deterministic(state->collid))
	{
		/* Walk one character at a time, until we reach the match. */
		while (state->refpoint < matchoff)
		{
			int			l = pg_mblen_range_n(state->str1 + state->refpoint,
											 state->len1 - state->refpoint);

			if (l == PG_CERR)
				return PG_CERR;
			state->refpoint += l;
			state->refpos++;

			/*
			 * If we stepped over the match's start position, then it was a
			 * false positive, where the byte sequence appeared in the
			 * middle of a multi-byte character.  Skip it, and continue the
			 * search at the next character boundary.
			 */
			if (state->refpoint > matchoff)
			{
				start_off = state->refpoint;
				goto retry;
			}
		}
	}

	state->last_match = matchoff;
	state->last_match_len = state->last_match_len_tmp;
	return 1;
}

/* text_position_get_match_pos, verbatim + err propagation */
static int
pg_text_position_get_match_pos(PgTextPositionState *state)
{
	int			l = pg_mbstrlen_with_len(state->str1 + state->refpoint,
										 state->last_match - state->refpoint);

	if (l == PG_CERR)
		return PG_CERR;
	/* Convert the byte position to char position. */
	state->refpos += l;
	state->refpoint = state->last_match;
	return state->refpos + 1;
}

/* text_position (textpos/strpos), verbatim */
int
pg_textpos(const unsigned char *t1, int len1,
		   const unsigned char *t2, int len2, Oid collid)
{
	PgTextPositionState state;
	int			result;
	int			found;

	if (!pg_collation_is_set(collid))
	{
		pg_errflag = PG_E_INDET_COLL;
		return PG_CERR;
	}

	/* Empty needle always matches at position 1 */
	if (len2 < 1)
		return 1;

	/* Otherwise, can't match if haystack is shorter than needle */
	if (len1 < len2 && pg_collate_deterministic(collid))
		return 0;

	if (pg_text_position_setup(t1, len1, t2, len2, collid, &state) == PG_CERR)
		return PG_CERR;
	/* don't need greedy mode here */
	state.greedy = 0;

	found = pg_text_position_next(&state);
	if (found == PG_CERR)
		return PG_CERR;
	if (!found)
		result = 0;
	else
	{
		result = pg_text_position_get_match_pos(&state);
		if (result == PG_CERR)
			return PG_CERR;
	}
	return result;
}

/* ---------------- varlena.c: text_starts_with ---------------- */

/*
 * Verbatim structure: raw sizes via shim 2, text_substring internal call
 * into a local buffer (16 bytes covers cap-8 harness payloads: the
 * substring of arg1 is at most len1 <= 8 bytes).
 */
int
pg_text_starts_with(const unsigned char *d1, int len1,
					const unsigned char *d2, int len2, Oid collid)
{
	int			result;
	size_t		rawlen1,
				rawlen2;

	if (!pg_collation_is_set(collid))
	{
		pg_errflag = PG_E_INDET_COLL;
		return PG_CERR;
	}

	if (!pg_collate_deterministic(collid))
	{
		pg_errflag = PG_E_NONDET;
		return PG_CERR;
	}

	rawlen1 = len1 + VARHDRSZ;	/* toast_raw_datum_size (shim 2) */
	rawlen2 = len2 + VARHDRSZ;
	if (rawlen2 > rawlen1)
		result = 0;
	else
	{
		unsigned char targ1[16];
		int			t1len = pg_text_substring(d1, len1, 1, (int) rawlen2, 0,
											  targ1);

		if (t1len == PG_CERR)
			return PG_CERR;

		result = (memcmp(targ1, d2, len2) == 0);
	}

	return result;
}

/* ---------------- varlena.c: byteapos ---------------- */

int
pg_byteapos(const unsigned char *t1, int len1,
			const unsigned char *t2, int len2)
{
	int			pos;
	int			px,
				p;
	const unsigned char *p1,
			   *p2;

	if (len2 <= 0)
		return 1;				/* result for empty pattern */

	p1 = t1;
	p2 = t2;

	pos = 0;
	px = (len1 - len2);
	for (p = 0; p <= px; p++)
	{
		if ((*p2 == *p1) && (memcmp(p1, p2, len2) == 0))
		{
			pos = p + 1;
			break;
		};
		p1++;
	};

	return pos;
}

/* ---------------- varlena.c: text_left / text_right ---------------- */

int
pg_text_left(const unsigned char *d, int len, int n, unsigned char *out)
{
	if (n < 0)
	{
		int			slen = pg_mbstrlen_with_len(d, len);
		int			rlen;

		if (slen == PG_CERR)
			return PG_CERR;
		n = slen + n;
		rlen = pg_mbcharcliplen(d, len, n);
		if (rlen == PG_CERR)
			return PG_CERR;
		if (rlen > 0)
			memcpy(out, d, rlen);
		return rlen;
	}
	else
		return pg_text_substring(d, len, 1, n, 0, out);
}

int
pg_text_right(const unsigned char *d, int len, int n, unsigned char *out)
{
	int			off;

	if (n < 0)
		n = -n;					/* -fwrapv: INT_MIN stays INT_MIN */
	else
	{
		int			slen = pg_mbstrlen_with_len(d, len);

		if (slen == PG_CERR)
			return PG_CERR;
		n = slen - n;
	}
	off = pg_mbcharcliplen(d, len, n);
	if (off == PG_CERR)
		return PG_CERR;

	if (len - off > 0)
		memcpy(out, d + off, len - off);
	return len - off;
}
