/*
 * Vendored PostgreSQL C for the oracle_compat string family Kani parity
 * proofs (proofs/oracle-compat).
 *
 * Provenance (postgres/postgres REL_18_STABLE, fetched 2026-07-28):
 *   - src/backend/utils/adt/oracle_compat.c: lpad, rpad, dotrim (btrim/
 *     ltrim/rtrim + the fixed-' ' btrim1/ltrim1/rtrim1 forms), dobyteatrim
 *     (byteatrim/bytealtrim/byteartrim), translate, repeat.
 *   - src/backend/utils/adt/varlena.c: text_reverse.
 *   - src/backend/utils/mb/mbutils.c: pg_mblen_with_len, pg_mblen_range,
 *     pg_mblen_unbounded, pg_mbstrlen_with_len, pg_database_encoding_max_length,
 *     GetDatabaseEncoding.
 *   - src/common/wchar.c: pg_utf_mblen, pg_latin1_mblen.
 *
 * lower/upper/initcap/casefold and ascii/chr are NOT vendored here:
 * the case family routes through formatting.c/pg_locale locale dispatch
 * (its own future lane); ascii/chr are owned by the in-flight strings-misc
 * lane (ledger rows 1620/1621 in-progress).
 *
 * SHIMS (everything else is verbatim):
 *  1. Names pgoc_-prefixed on all EXPORTED symbols (this family's C file may
 *     never share a link with proofs/oracle-compat's pg_varchar.c, but the
 *     compile gate builds both; statics keep their C names). Typedefs
 *     inlined (int32 -> int, bool -> int, Size -> size_t).
 *  2. text/bytea arguments ride as (const unsigned char *data, int len)
 *     payload pairs modeling the post-PG_GETARG_*_PP inline-image caller
 *     contract (established varlena pattern: proofs/text-cmp/text-slice);
 *     DETOASTING is out of scope.
 *  3. palloc'd results -> caller-provided out buffers; string-returning
 *     functions return the payload byte length written. dotrim/dobyteatrim
 *     additionally report the surviving WINDOW (*out_start byte offset)
 *     so window-verdict harnesses can compare the scalar claim without the
 *     derived-length result copy (the copy itself is exercised by the
 *     concrete image-spot harnesses through the same out buffer).
 *  4. ereport/ereturn -> pg_errflag = <class below> + PGOC_CERR sentinel
 *     return, propagated through callers (models C's longjmp unwind).
 *     Classes: 1 = ERRCODE_PROGRAM_LIMIT_EXCEEDED (54000, requested
 *     length/character too large), 4 = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
 *     (22021, invalid byte sequence via pg_mblen_range), 99 = POISON
 *     (harness contract violation, never a valid verdict).
 *  5. Encoding state: DatabaseEncoding / pg_wchar_table -> pgoc_db_encoding
 *     global (pgoc_set_db_encoding) + a two-encoding mblen dispatch
 *     (PG_UTF8 -> pg_utf_mblen verbatim, PG_LATIN1 -> pg_latin1_mblen
 *     verbatim; maxmblen 4/1 per the pg_wchar_table rows). ENCODING FENCE:
 *     harnesses pin one of {PG_UTF8, PG_LATIN1} on both sides.
 *  6. pg_add_s32_overflow / pg_mul_s32_overflow -> __builtin_*_overflow,
 *     exactly how src/include/common/int.h defines them under gcc/clang.
 *  7. dotrim's palloc'd stringchars/setchars pointer+mblen arrays -> fixed
 *     PGOC_CAP/PGOC_SETCAP-sized locals (harness caps inputs; a cap overrun
 *     poisons loudly instead of overflowing).
 *  8. CHECK_FOR_INTERRUPTS() -> no-op (harnesses run with no interrupt
 *     pending; the Rust side's InterruptPending() pre-check is false for
 *     the same reason, so both sides skip the seam identically).
 *  9. Assert -> no-op (compiled out of production builds).
 * 10. memcpy/memset are CBMC's built-in models.
 */

#include <stddef.h>
#include <string.h>

#define VARHDRSZ 4

#define PG_UTF8 6
#define PG_LATIN1 8

/* shim 4: error model */
#define PGOC_CERR (-2100000000)
#define PGOC_E_LIMIT 1
#define PGOC_E_BADSEQ 4
#define PGOC_E_POISON 99

/* shim 7: fixed array caps (mirror the harness input caps) */
#define PGOC_CAP 8
#define PGOC_SETCAP 8

static int pg_errflag = 0;

int
pgoc_take_err(void)
{
	int			e = pg_errflag;

	pg_errflag = 0;
	return e;
}

/* shim 6: src/include/common/int.h (gcc/clang arm, verbatim) */
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

/* src/include/utils/memutils.h, verbatim */
#define MaxAllocSize	((size_t) 0x3fffffff)	/* 1 gigabyte - 1 */
#define AllocSizeIsValid(size)	((size_t) (size) <= MaxAllocSize)

/* ---------------- shim 5: encoding state ---------------- */

static int pgoc_db_encoding = PG_UTF8;

/* int return: Kani lowers Rust () as `struct Unit`, which goto-cc rejects
 * against C void (prove-target trap) */
int
pgoc_set_db_encoding(int enc)
{
	pgoc_db_encoding = enc;
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
	return (pgoc_db_encoding == PG_UTF8) ? pg_utf_mblen(s) : pg_latin1_mblen(s);
}

/* pg_wchar_table maxmblen column: UTF8 = 4, LATIN1 = 1 (shim 5) */
static int
pg_database_encoding_max_length(void)
{
	return (pgoc_db_encoding == PG_UTF8) ? 4 : 1;
}

/* ---------------- mbutils.c (REL_18) ---------------- */

/* pg_mblen_with_len: ereport(invalid byte sequence) -> shim 4 */
static int
pg_mblen_with_len(const unsigned char *mbstr, int limit)
{
	int			length = pg_enc_mblen(mbstr);

	if (length > limit)
	{
		pg_errflag = PGOC_E_BADSEQ;
		return PGOC_CERR;
	}
	return length;
}

/* pg_mblen_range, verbatim + err propagation (shim 4) */
static int
pg_mblen_range(const unsigned char *mbstr, const unsigned char *end)
{
	int			length = pg_enc_mblen(mbstr);

	if (mbstr + length > end)
	{
		pg_errflag = PGOC_E_BADSEQ;
		return PGOC_CERR;
	}
	return length;
}

/* pg_mblen_unbounded, verbatim */
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

		if (l == PGOC_CERR)
			return PGOC_CERR;
		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

/* ---------------- oracle_compat.c lpad / rpad (REL_18) ---------------- */

/*
 * lpad, verbatim; shims 2/3 (payload args, out buffer, returned length).
 * The harness's out buffer must cover max_length*len_arg bytes for the
 * in-bounds regimes it constructs.
 */
int
pgoc_lpad(const unsigned char *string1, int s1bytes,
		  int len,
		  const unsigned char *string2, int s2bytes,
		  unsigned char *out)
{
	unsigned char *ptr1;
	const unsigned char *ptr2,
			   *ptr2start,
			   *ptr2end;
	unsigned char *ptr_ret;
	int			m,
				s1len,
				s2len;
	int			bytelen;

	/* Negative len is silently taken as zero */
	if (len < 0)
		len = 0;

	s1len = s1bytes;
	if (s1len < 0)
		s1len = 0;				/* shouldn't happen */

	s2len = s2bytes;
	if (s2len < 0)
		s2len = 0;				/* shouldn't happen */

	s1len = pg_mbstrlen_with_len(string1, s1len);
	if (s1len == PGOC_CERR)
		return PGOC_CERR;		/* shim 4 propagation (C longjmps) */

	if (s1len > len)
		s1len = len;			/* truncate string1 to len chars */

	if (s2len <= 0)
		len = s1len;			/* nothing to pad with, so don't pad */

	/* compute worst-case output length */
	if (pg_mul_s32_overflow(pg_database_encoding_max_length(), len,
							&bytelen) ||
		pg_add_s32_overflow(bytelen, VARHDRSZ, &bytelen) ||
		!AllocSizeIsValid(bytelen))
	{
		pg_errflag = PGOC_E_LIMIT;
		return PGOC_CERR;
	}

	m = len - s1len;

	ptr2 = ptr2start = string2;
	ptr2end = ptr2 + s2len;
	ptr_ret = out;

	while (m--)
	{
		int			mlen = pg_mblen_range(ptr2, ptr2end);

		if (mlen == PGOC_CERR)
			return PGOC_CERR;
		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;
		if (ptr2 == ptr2end)	/* wrap around at end of s2 */
			ptr2 = ptr2start;
	}

	ptr1 = (unsigned char *) string1;

	while (s1len--)
	{
		int			mlen = pg_mblen_unbounded(ptr1);

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;
	}

	return (int) (ptr_ret - out);
}

/* rpad, verbatim; same shims as lpad */
int
pgoc_rpad(const unsigned char *string1, int s1bytes,
		  int len,
		  const unsigned char *string2, int s2bytes,
		  unsigned char *out)
{
	unsigned char *ptr1;
	const unsigned char *ptr2,
			   *ptr2start,
			   *ptr2end;
	unsigned char *ptr_ret;
	int			m,
				s1len,
				s2len;
	int			bytelen;

	/* Negative len is silently taken as zero */
	if (len < 0)
		len = 0;

	s1len = s1bytes;
	if (s1len < 0)
		s1len = 0;				/* shouldn't happen */

	s2len = s2bytes;
	if (s2len < 0)
		s2len = 0;				/* shouldn't happen */

	s1len = pg_mbstrlen_with_len(string1, s1len);
	if (s1len == PGOC_CERR)
		return PGOC_CERR;		/* shim 4 propagation */

	if (s1len > len)
		s1len = len;			/* truncate string1 to len chars */

	if (s2len <= 0)
		len = s1len;			/* nothing to pad with, so don't pad */

	/* compute worst-case output length */
	if (pg_mul_s32_overflow(pg_database_encoding_max_length(), len,
							&bytelen) ||
		pg_add_s32_overflow(bytelen, VARHDRSZ, &bytelen) ||
		!AllocSizeIsValid(bytelen))
	{
		pg_errflag = PGOC_E_LIMIT;
		return PGOC_CERR;
	}

	m = len - s1len;

	ptr1 = (unsigned char *) string1;
	ptr_ret = out;

	while (s1len--)
	{
		int			mlen = pg_mblen_unbounded(ptr1);

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;
	}

	ptr2 = ptr2start = string2;
	ptr2end = ptr2 + s2len;

	while (m--)
	{
		int			mlen = pg_mblen_range(ptr2, ptr2end);

		if (mlen == PGOC_CERR)
			return PGOC_CERR;
		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;
		if (ptr2 == ptr2end)	/* wrap around at end of s2 */
			ptr2 = ptr2start;
	}

	return (int) (ptr_ret - out);
}

/* ---------------- oracle_compat.c dotrim (REL_18) ---------------- */

/*
 * dotrim, verbatim; shims 3 (window out-param + out-buffer copy in place of
 * cstring_to_text_with_len) and 7 (fixed arrays). Returns the surviving
 * window length; *out_start = its byte offset into `string`.
 */
int
pgoc_dotrim(const unsigned char *string, int stringlen,
			const unsigned char *set, int setlen,
			int doltrim, int dortrim,
			int *out_start, unsigned char *out)
{
	int			i;
	const unsigned char *string0 = string;

	/* shim 7: harness cap guard (poison, not a verdict) */
	if (stringlen > PGOC_CAP || setlen > PGOC_SETCAP)
	{
		pg_errflag = PGOC_E_POISON;
		return PGOC_CERR;
	}

	/* Nothing to do if either string or set is empty */
	if (stringlen > 0 && setlen > 0)
	{
		if (pg_database_encoding_max_length() > 1)
		{
			/*
			 * In the multibyte-encoding case, build arrays of pointers to
			 * character starts, so that we can avoid inefficient checks in
			 * the inner loops.
			 */
			const unsigned char *stringchars[PGOC_CAP];	/* shim 7 */
			const unsigned char *setchars[PGOC_SETCAP]; /* shim 7 */
			const unsigned char *setend;
			int			stringmblen[PGOC_CAP];	/* shim 7 */
			int			setmblen[PGOC_SETCAP];	/* shim 7 */
			int			stringnchars;
			int			setnchars;
			int			resultndx;
			int			resultnchars;
			const unsigned char *p;
			const unsigned char *pend;
			int			len;
			int			mblen;
			const unsigned char *str_pos;
			int			str_len;

			stringnchars = 0;
			p = string;
			len = stringlen;
			pend = p + len;
			while (len > 0)
			{
				stringchars[stringnchars] = p;
				stringmblen[stringnchars] = mblen = pg_mblen_range(p, pend);
				if (mblen == PGOC_CERR)
					return PGOC_CERR;	/* shim 4 propagation */
				stringnchars++;
				p += mblen;
				len -= mblen;
			}

			setnchars = 0;
			p = set;
			len = setlen;
			setend = set + setlen;
			while (len > 0)
			{
				setchars[setnchars] = p;
				setmblen[setnchars] = mblen = pg_mblen_range(p, setend);
				if (mblen == PGOC_CERR)
					return PGOC_CERR;	/* shim 4 propagation */
				setnchars++;
				p += mblen;
				len -= mblen;
			}

			resultndx = 0;		/* index in stringchars[] */
			resultnchars = stringnchars;

			if (doltrim)
			{
				while (resultnchars > 0)
				{
					str_pos = stringchars[resultndx];
					str_len = stringmblen[resultndx];
					for (i = 0; i < setnchars; i++)
					{
						if (str_len == setmblen[i] &&
							memcmp(str_pos, setchars[i], str_len) == 0)
							break;
					}
					if (i >= setnchars)
						break;	/* no match here */
					string += str_len;
					stringlen -= str_len;
					resultndx++;
					resultnchars--;
				}
			}

			if (dortrim)
			{
				while (resultnchars > 0)
				{
					str_pos = stringchars[resultndx + resultnchars - 1];
					str_len = stringmblen[resultndx + resultnchars - 1];
					for (i = 0; i < setnchars; i++)
					{
						if (str_len == setmblen[i] &&
							memcmp(str_pos, setchars[i], str_len) == 0)
							break;
					}
					if (i >= setnchars)
						break;	/* no match here */
					stringlen -= str_len;
					resultnchars--;
				}
			}
		}
		else
		{
			/*
			 * In the single-byte-encoding case, we don't need such overhead.
			 */
			if (doltrim)
			{
				while (stringlen > 0)
				{
					unsigned char str_ch = *string;

					for (i = 0; i < setlen; i++)
					{
						if (str_ch == set[i])
							break;
					}
					if (i >= setlen)
						break;	/* no match here */
					string++;
					stringlen--;
				}
			}

			if (dortrim)
			{
				while (stringlen > 0)
				{
					unsigned char str_ch = string[stringlen - 1];

					for (i = 0; i < setlen; i++)
					{
						if (str_ch == set[i])
							break;
					}
					if (i >= setlen)
						break;	/* no match here */
					stringlen--;
				}
			}
		}
	}

	/* Return selected portion of string (cstring_to_text_with_len -> shim 3) */
	*out_start = (int) (string - string0);
	memcpy(out, string, stringlen);
	return stringlen;
}

/* ---------------- oracle_compat.c dobyteatrim (REL_18) ---------------- */

/*
 * dobyteatrim, verbatim; shim 3 (window out-param + out-buffer copy).
 * C returns the input bytea untouched when either side is empty; here
 * that is the (*out_start = 0, return stringlen) window.
 */
int
pgoc_dobyteatrim(const unsigned char *string, int stringlen,
				 const unsigned char *set, int setlen,
				 int doltrim, int dortrim,
				 int *out_start, unsigned char *out)
{
	const unsigned char *ptr,
			   *end,
			   *ptr2,
			   *ptr2start,
			   *end2;
	int			m;

	if (stringlen <= 0 || setlen <= 0)
	{
		*out_start = 0;
		memcpy(out, string, stringlen < 0 ? 0 : stringlen);
		return stringlen;
	}

	m = stringlen;
	ptr = string;
	end = ptr + stringlen - 1;
	ptr2start = set;
	end2 = ptr2start + setlen - 1;

	if (doltrim)
	{
		while (m > 0)
		{
			ptr2 = ptr2start;
			while (ptr2 <= end2)
			{
				if (*ptr == *ptr2)
					break;
				++ptr2;
			}
			if (ptr2 > end2)
				break;
			ptr++;
			m--;
		}
	}

	if (dortrim)
	{
		while (m > 0)
		{
			ptr2 = ptr2start;
			while (ptr2 <= end2)
			{
				if (*end == *ptr2)
					break;
				++ptr2;
			}
			if (ptr2 > end2)
				break;
			end--;
			m--;
		}
	}

	*out_start = (int) (ptr - string);
	memcpy(out, ptr, m);
	return m;
}

/* ---------------- oracle_compat.c translate (REL_18) ---------------- */

/* translate, verbatim; shims 2/3/4. Empty-string identity handled here. */
int
pgoc_translate(const unsigned char *string, int m,
			   const unsigned char *from_arg, int fromlen,
			   const unsigned char *to_arg, int tolen,
			   unsigned char *out)
{
	const unsigned char *from_ptr,
			   *to_ptr;
	const unsigned char *to_end;
	const unsigned char *source;
	unsigned char *target;
	const unsigned char *source_end;
	const unsigned char *from_end;
	int			retlen,
				i;
	int			bytelen;
	int			len;
	int			source_len;
	int			from_index;

	if (m <= 0)
	{
		/* PG_RETURN_TEXT_P(string): identity */
		memcpy(out, string, m < 0 ? 0 : m);
		return m;
	}
	source = string;
	source_end = source + m;

	from_ptr = from_arg;
	from_end = from_ptr + fromlen;
	to_ptr = to_arg;
	to_end = to_ptr + tolen;

	/*
	 * The worst-case expansion is to substitute a max-length character for a
	 * single-byte character at each position of the string.
	 */
	if (pg_mul_s32_overflow(pg_database_encoding_max_length(), m,
							&bytelen) ||
		pg_add_s32_overflow(bytelen, VARHDRSZ, &bytelen) ||
		!AllocSizeIsValid(bytelen))
	{
		pg_errflag = PGOC_E_LIMIT;
		return PGOC_CERR;
	}

	target = out;
	retlen = 0;

	while (m > 0)
	{
		source_len = pg_mblen_range(source, source_end);
		if (source_len == PGOC_CERR)
			return PGOC_CERR;	/* shim 4 propagation */
		from_index = 0;

		for (i = 0; i < fromlen; i += len)
		{
			len = pg_mblen_range(&from_ptr[i], from_end);
			if (len == PGOC_CERR)
				return PGOC_CERR;	/* shim 4 propagation */
			if (len == source_len &&
				memcmp(source, &from_ptr[i], len) == 0)
				break;

			from_index++;
		}
		if (i < fromlen)
		{
			/* substitute, or delete if no corresponding "to" character */
			const unsigned char *p = to_ptr;

			for (i = 0; i < from_index; i++)
			{
				if (p >= to_end)
					break;
				len = pg_mblen_range(p, to_end);
				if (len == PGOC_CERR)
					return PGOC_CERR;	/* shim 4 propagation */
				p += len;
			}
			if (p < to_end)
			{
				len = pg_mblen_range(p, to_end);
				if (len == PGOC_CERR)
					return PGOC_CERR;	/* shim 4 propagation */
				memcpy(target, p, len);
				target += len;
				retlen += len;
			}
		}
		else
		{
			/* no match, so copy */
			memcpy(target, source, source_len);
			target += source_len;
			retlen += source_len;
		}

		source += source_len;
		m -= source_len;
	}

	return retlen;
}

/* ---------------- oracle_compat.c repeat (REL_18) ---------------- */

/* repeat, verbatim; shims 2/3/4/8. */
int
pgoc_repeat(const unsigned char *string, int slen, int count,
			unsigned char *out)
{
	int			tlen;
	int			i;
	unsigned char *cp;
	const unsigned char *sp;

	if (count < 0)
		count = 0;

	if (pg_mul_s32_overflow(count, slen, &tlen) ||
		pg_add_s32_overflow(tlen, VARHDRSZ, &tlen) ||
		!AllocSizeIsValid(tlen))
	{
		pg_errflag = PGOC_E_LIMIT;
		return PGOC_CERR;
	}

	cp = out;
	sp = string;
	for (i = 0; i < count; i++)
	{
		memcpy(cp, sp, slen);
		cp += slen;
		/* CHECK_FOR_INTERRUPTS(): shim 8 (no-op) */
	}

	return tlen - VARHDRSZ;
}

/* ---------------- varlena.c text_reverse (REL_18) ---------------- */

/* text_reverse, verbatim; shims 2/3/4. */
int
pgoc_text_reverse(const unsigned char *str, int len, unsigned char *out)
{
	const unsigned char *p = str;
	const unsigned char *endp = p + len;
	unsigned char *dst;

	dst = out + len;

	if (pg_database_encoding_max_length() > 1)
	{
		/* multibyte version */
		while (p < endp)
		{
			int			sz;

			sz = pg_mblen_range(p, endp);
			if (sz == PGOC_CERR)
				return PGOC_CERR;	/* shim 4 propagation */
			dst -= sz;
			memcpy(dst, p, sz);
			p += sz;
		}
	}
	else
	{
		/* single byte version */
		while (p < endp)
			*(--dst) = *p++;
	}

	return len;
}
