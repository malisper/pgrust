/*
 * Vendored PostgreSQL C for the varchar/bpchar family Kani parity proofs
 * (proofs/oracle-compat).
 *
 * Provenance (postgres/postgres REL_18_STABLE, fetched 2026-07-28):
 *   - src/backend/utils/adt/varchar.c: anychar_typmodin, anychar_typmodout,
 *     bpchar_input (decision half of bpcharin/bpcharrecv), bpchar
 *     (length coercion, OID 668), char_bpchar, bpchar_name, name_bpchar,
 *     varchar_input (decision half of varcharin/varcharrecv), varchar
 *     (length coercion, OID 669), bpchartruelen/bcTruelen, bpcharlen,
 *     bpcharoctetlen.
 *   - src/backend/utils/mb/mbutils.c: pg_mblen_with_len,
 *     pg_mbstrlen_with_len, cliplen, pg_mbcharcliplen,
 *     pg_encoding_mbcliplen (pg_mbcliplen), pg_database_encoding_max_length.
 *   - src/common/wchar.c: pg_utf_mblen, pg_latin1_mblen.
 *   - src/backend/utils/adt/numutils.c: DIGIT_TABLE, decimalLength32,
 *     pg_ultoa_n, pg_ltoa (for the anychar_typmodout shim, see shim 8).
 *   - src/include/port/pg_bitutils.h: pg_leftmost_one_pos32
 *     (HAVE__BUILTIN_CLZ arm, verbatim).
 *
 * SHIMS (everything else is verbatim):
 *  1. Names pgvc_-prefixed on all EXPORTED symbols; typedefs inlined
 *     (int32 -> int, uint32 -> unsigned int, bool -> int, Size -> size_t).
 *  2. bpchar/varchar/text arguments ride as (const unsigned char *data,
 *     int len) payload pairs modeling the post-PG_GETARG_*_PP inline-image
 *     caller contract; DETOASTING out of scope (varlena pattern).
 *  3. palloc'd results -> caller out buffers / out-params.
 *     bpchar_input/varchar_input are DECISION shims: they run the verbatim
 *     clip logic and report (copy, total) byte counts without performing
 *     the palloc+memcpy+memset image build (the image half is exercised by
 *     the wrapper-level coercion harnesses and by the Rust image compare
 *     in eq_bpchar_coerce_*). toast_raw_datum_size(d) - VARHDRSZ ->
 *     payload len (bpcharoctetlen; text-slice shim 2 precedent).
 *  4. ereport/ereturn -> pg_errflag = <class below> + PGVC_CERR sentinel
 *     return (models C's longjmp/soft-error escape; harnesses drive the
 *     Rust side with escontext = None so both sides take the hard path).
 *     Classes: 2 = ERRCODE_INVALID_PARAMETER_VALUE (22023, typmod errors),
 *     3 = ERRCODE_STRING_DATA_RIGHT_TRUNCATION (22001, value too long),
 *     4 = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (22021, bad byte sequence),
 *     5 = palloc alloc-guard ("invalid memory alloc request size",
 *     verdict-only class — sqlstate parity NOT asserted), 99 = POISON.
 *  5. Encoding state: pgvc_db_encoding global + two-encoding dispatch
 *     {PG_UTF8, PG_LATIN1} exactly as in c/pg_oracle_compat.c (shim 5
 *     there). ENCODING FENCE: harnesses pin the same value on both sides.
 *  6. bpchar's `palloc(maxlen + VARHDRSZ)` after wrapping int arithmetic:
 *     the request is computed in int (as C does under -fwrapv), then
 *     sign-extended to Size and checked with AllocSizeIsValid — the exact
 *     guard MemoryContextAlloc applies. Failure -> class 5. This mirrors
 *     the shipped Rust comment-claim (varchar/src/lib.rs bpchar) that
 *     pgrust reproduces C's exact wrapped request.
 *  7. Assert -> no-op; memcpy/memset are CBMC built-ins; palloc0 ->
 *     memset(out, 0, ..) on the caller buffer (bpchar_name).
 *  8. anychar_typmodout's snprintf(res, 64, "(%d)") -> '(' + pg_ltoa + ')'.
 *     libc snprintf has no CBMC model; pg_ltoa is REL_18's own decimal
 *     emitter (numutils.c), vendored verbatim below, and is PG's documented
 *     %d-equivalent. This substitutes ONE C-library formatter for PG's own
 *     formatter of the same integer — recorded prominently because it is
 *     the only shim in this file that replaces a formatting mechanism
 *     rather than plumbing.
 *  9. NameData (name_bpchar): a 64-byte block; the harness assumes the
 *     Name datatype invariant (NUL-terminated within NAMEDATALEN, i.e.
 *     name[63] == 0) so C's strlen walk (NameStr) is defined. strlen ->
 *     bounded scan (no libc model).
 */

#include <stddef.h>
#include <string.h>

#define VARHDRSZ 4
#define NAMEDATALEN 64

#define PG_UTF8 6
#define PG_LATIN1 8

/* src/include/access/htup_details.h, verbatim */
#define MaxAttrSize (10 * 1024 * 1024)

/* shim 4: error model */
#define PGVC_CERR (-2100000000)
#define PGVC_E_PARAM 2
#define PGVC_E_TRUNC 3
#define PGVC_E_BADSEQ 4
#define PGVC_E_ALLOC 5
#define PGVC_E_POISON 99

static int pg_errflag = 0;

int
pgvc_take_err(void)
{
	int			e = pg_errflag;

	pg_errflag = 0;
	return e;
}

/* src/include/utils/memutils.h, verbatim */
#define MaxAllocSize	((size_t) 0x3fffffff)	/* 1 gigabyte - 1 */
#define AllocSizeIsValid(size)	((size_t) (size) <= MaxAllocSize)

/* ---------------- shim 5: encoding state ---------------- */

static int pgvc_db_encoding = PG_UTF8;

int
pgvc_set_db_encoding(int enc)
{
	pgvc_db_encoding = enc;
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

static int
pg_enc_mblen(const unsigned char *s)
{
	return (pgvc_db_encoding == PG_UTF8) ? pg_utf_mblen(s) : pg_latin1_mblen(s);
}

static int
pg_database_encoding_max_length(void)
{
	return (pgvc_db_encoding == PG_UTF8) ? 4 : 1;
}

/* ---------------- mbutils.c (REL_18) ---------------- */

/* pg_mblen_with_len: ereport(invalid byte sequence) -> shim 4 */
static int
pg_mblen_with_len(const unsigned char *mbstr, int limit)
{
	int			length = pg_enc_mblen(mbstr);

	if (length > limit)
	{
		pg_errflag = PGVC_E_BADSEQ;
		return PGVC_CERR;
	}
	return length;
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

		if (l == PGVC_CERR)
			return PGVC_CERR;
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

	len = (len < limit) ? len : limit;	/* Min(len, limit) */
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
		if (l == PGVC_CERR)
			return PGVC_CERR;
		nch++;
		if (nch > limit)
			break;
		clen += l;
		len -= l;
		mbstr += l;
	}
	return clen;
}

/* pg_encoding_mbcliplen, verbatim (mblen_fn dispatch -> shim 5);
 * pg_mbcliplen is its DatabaseEncoding wrapper */
static int
pg_mbcliplen(const unsigned char *mbstr, int len, int limit)
{
	int			clen = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return cliplen(mbstr, len, limit);

	while (len > 0 && *mbstr)
	{
		l = pg_enc_mblen(mbstr);
		if ((clen + l) > limit)
			break;
		clen += l;
		if (clen == limit)
			break;
		len -= l;
		mbstr += l;
	}
	return clen;
}

/* ---------------- numutils.c (REL_18): pg_ltoa (shim 8) ---------------- */

typedef unsigned int uint32;

/* src/include/port/pg_bitutils.h pg_leftmost_one_pos32, __builtin_clz arm */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
	return 31 - __builtin_clz(word);
}

static const char DIGIT_TABLE[200] =
"00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
"10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
"20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
"30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
"40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
"50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
"60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
"70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
"80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
"90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

/* numutils.c decimalLength32, verbatim */
static inline int
decimalLength32(const uint32 v)
{
	int			t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/* numutils.c pg_ultoa_n, verbatim */
static int
pg_ultoa_n(uint32 value, char *a)
{
	int			olength,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

/* numutils.c pg_ltoa, verbatim */
static int
pg_ltoa(int value, char *a)
{
	uint32		uvalue = (uint32) value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint32) 0 - uvalue;
		a[len++] = '-';
	}
	len += pg_ultoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/* ---------------- varchar.c (REL_18) ---------------- */

/*
 * anychar_typmodin, verbatim over the extracted (tl, n) integer list.
 * ArrayGetIntegerTypmods (the cstring[] array walk) is OUT OF SCOPE —
 * ledger rows 2913/2915 are covered for the check core only.
 */
int
pgvc_anychar_typmodin(const int *tl, int n, int *out_typmod)
{
	int			typmod;

	/*
	 * we're not too tense about good error message here because grammar
	 * shouldn't allow wrong number of modifiers for CHAR
	 */
	if (n != 1)
	{
		pg_errflag = PGVC_E_PARAM;
		return PGVC_CERR;
	}

	if (*tl < 1)
	{
		pg_errflag = PGVC_E_PARAM;
		return PGVC_CERR;
	}
	if (*tl > MaxAttrSize)
	{
		pg_errflag = PGVC_E_PARAM;
		return PGVC_CERR;
	}

	/*
	 * For largely historical reasons, the typmod is VARHDRSZ plus the number
	 * of characters; there is enough client-side code that knows about that
	 * that we'd better not change it.
	 */
	typmod = VARHDRSZ + *tl;

	*out_typmod = typmod;
	return 0;
}

/* anychar_typmodout, verbatim modulo shim 8 (snprintf -> pg_ltoa).
 * Returns the formatted byte length ("" -> 0). */
int
pgvc_anychar_typmodout(int typmod, char *res)
{
	if (typmod > VARHDRSZ)
	{
		int			n;

		res[0] = '(';
		n = pg_ltoa(typmod - VARHDRSZ, res + 1);
		res[1 + n] = ')';
		return n + 2;
	}
	else
	{
		*res = '\0';
		return 0;
	}
}

/*
 * bpchar_input, verbatim DECISION half (shim 3): the clip/pad computation
 * up to (but not including) the palloc+memcpy+memset. *out_copy = the
 * final `len` (bytes copied), *out_total = the final `maxlen` (payload
 * bytes incl blank padding). escontext = NULL (hard-error path, shim 4).
 */
int
pgvc_bpchar_input(const unsigned char *s, size_t len, int atttypmod,
				  size_t *out_copy, size_t *out_total)
{
	size_t		maxlen;

	/* If typmod is -1 (or invalid), use the actual string length */
	if (atttypmod < (int) VARHDRSZ)
		maxlen = len;
	else
	{
		size_t		charlen;	/* number of CHARACTERS in the input */
		int			charlen_i;

		maxlen = atttypmod - VARHDRSZ;
		charlen_i = pg_mbstrlen_with_len(s, len);
		if (charlen_i == PGVC_CERR)
			return PGVC_CERR;	/* shim 4 propagation */
		charlen = (size_t) charlen_i;
		if (charlen > maxlen)
		{
			/* Verify that extra characters are spaces, and clip them off */
			int			mbmaxlen_i = pg_mbcharcliplen(s, len, maxlen);
			size_t		mbmaxlen;
			size_t		j;

			if (mbmaxlen_i == PGVC_CERR)
				return PGVC_CERR;	/* shim 4 propagation */
			mbmaxlen = (size_t) mbmaxlen_i;

			/*
			 * at this point, len is the actual BYTE length of the input
			 * string, maxlen is the max number of CHARACTERS allowed for this
			 * bpchar type, mbmaxlen is the length in BYTES of those chars.
			 */
			for (j = mbmaxlen; j < len; j++)
			{
				if (s[j] != ' ')
				{
					pg_errflag = PGVC_E_TRUNC;
					return PGVC_CERR;
				}
			}

			/*
			 * Now we set maxlen to the necessary byte length, not the number
			 * of CHARACTERS!
			 */
			maxlen = len = mbmaxlen;
		}
		else
		{
			/*
			 * Now we set maxlen to the necessary byte length, not the number
			 * of CHARACTERS!
			 */
			maxlen = len + (maxlen - charlen);
		}
	}

	/* result = palloc(maxlen + VARHDRSZ); memcpy(r, s, len);
	 * memset(r + len, ' ', maxlen - len): shim 3 — decision out-params */
	*out_copy = len;
	*out_total = maxlen;
	return 0;
}

/*
 * varchar_input, verbatim DECISION half (shim 3): *out_len = the byte
 * count fed to cstring_to_text_with_len. escontext = NULL (shim 4).
 */
int
pgvc_varchar_input(const unsigned char *s, size_t len, int atttypmod,
				   size_t *out_len)
{
	size_t		maxlen;

	maxlen = atttypmod - VARHDRSZ;

	if (atttypmod >= (int) VARHDRSZ && len > maxlen)
	{
		/* Verify that extra characters are spaces, and clip them off */
		int			mbmaxlen_i = pg_mbcharcliplen(s, len, maxlen);
		size_t		mbmaxlen;
		size_t		j;

		if (mbmaxlen_i == PGVC_CERR)
			return PGVC_CERR;	/* shim 4 propagation */
		mbmaxlen = (size_t) mbmaxlen_i;

		for (j = mbmaxlen; j < len; j++)
		{
			if (s[j] != ' ')
			{
				pg_errflag = PGVC_E_TRUNC;
				return PGVC_CERR;
			}
		}

		len = mbmaxlen;
	}

	*out_len = len;
	return 0;
}

/*
 * bpchar (length coercion, OID 668), verbatim; shims 2/3/4/6.
 * *ret_source = 1 models PG_RETURN_BPCHAR_P(source) (the identity planes).
 * On the build plane the blank-padded image is written to out (the caller
 * bounds total so the write fits its buffer) and the payload length
 * returned.
 */
int
pgvc_bpchar(const unsigned char *s, int len, int maxlen, int isExplicit,
			int *ret_source, unsigned char *out, int outcap)
{
	int			i;
	int			charlen;		/* number of characters in the input string */
	size_t		request;

	*ret_source = 0;

	/* No work if typmod is invalid */
	if (maxlen < (int) VARHDRSZ)
	{
		*ret_source = 1;
		return len;
	}

	maxlen -= VARHDRSZ;

	charlen = pg_mbstrlen_with_len(s, len);
	if (charlen == PGVC_CERR)
		return PGVC_CERR;		/* shim 4 propagation */

	/* No work if supplied data matches typmod already */
	if (charlen == maxlen)
	{
		*ret_source = 1;
		return len;
	}

	if (charlen > maxlen)
	{
		/* Verify that extra characters are spaces, and clip them off */
		size_t		maxmblen;
		int			maxmblen_i;

		maxmblen_i = pg_mbcharcliplen(s, len, maxlen);
		if (maxmblen_i == PGVC_CERR)
			return PGVC_CERR;	/* shim 4 propagation */
		maxmblen = (size_t) maxmblen_i;

		if (!isExplicit)
		{
			for (i = maxmblen; i < len; i++)
				if (s[i] != ' ')
				{
					pg_errflag = PGVC_E_TRUNC;
					return PGVC_CERR;
				}
		}

		len = maxmblen;

		/*
		 * At this point, maxlen is the necessary byte length, not the number
		 * of CHARACTERS!
		 */
		maxlen = len;
	}
	else
	{
		/*
		 * At this point, maxlen is the necessary byte length, not the number
		 * of CHARACTERS!
		 */
		maxlen = len + (maxlen - charlen);
	}

	/*
	 * result = palloc(maxlen + VARHDRSZ): shim 6 — the request is the
	 * int-wrapped sum sign-extended to Size, guarded exactly as
	 * MemoryContextAlloc guards it.
	 */
	request = (size_t) (long long) (maxlen + VARHDRSZ);
	if (!AllocSizeIsValid(request))
	{
		pg_errflag = PGVC_E_ALLOC;
		return PGVC_CERR;
	}

	if (maxlen > outcap)
	{
		pg_errflag = PGVC_E_POISON; /* harness buffer contract violation */
		return PGVC_CERR;
	}

	memcpy(out, s, len);

	/* blank pad the string if necessary */
	if (maxlen > len)
		memset(out + len, ' ', maxlen - len);

	return maxlen;
}

/*
 * varchar (length coercion, OID 669), verbatim; shims 2/3/4.
 * *ret_source = 1 models PG_RETURN_VARCHAR_P(source).
 */
int
pgvc_varchar(const unsigned char *s, int len, int typmod, int isExplicit,
			 int *ret_source, unsigned char *out)
{
	int			maxlen;
	size_t		maxmblen;
	int			maxmblen_i;
	int			i;

	*ret_source = 0;

	maxlen = typmod - VARHDRSZ;

	/* No work if typmod is invalid or supplied data fits it already */
	if (maxlen < 0 || len <= maxlen)
	{
		*ret_source = 1;
		return len;
	}

	/* only reach here if string is too long... */

	/* truncate multibyte string preserving multibyte boundary */
	maxmblen_i = pg_mbcharcliplen(s, len, maxlen);
	if (maxmblen_i == PGVC_CERR)
		return PGVC_CERR;		/* shim 4 propagation */
	maxmblen = (size_t) maxmblen_i;

	if (!isExplicit)
	{
		for (i = maxmblen; i < len; i++)
			if (s[i] != ' ')
			{
				pg_errflag = PGVC_E_TRUNC;
				return PGVC_CERR;
			}
	}

	/* cstring_to_text_with_len(s_data, maxmblen): shim 3 */
	memcpy(out, s, maxmblen);
	return (int) maxmblen;
}

/* char_bpchar, verbatim; shim 3 (palloc(VARHDRSZ + 1) -> out). */
int
pgvc_char_bpchar(signed char c, unsigned char *out)
{
	*out = (unsigned char) c;
	return 1;
}

/*
 * bpchar_name, verbatim; shim 3 (palloc0(NAMEDATALEN) -> memset + memcpy
 * into the caller's 64-byte buffer). Returns the trimmed length.
 */
int
pgvc_bpchar_name(const unsigned char *s_data, int len, unsigned char *out)
{
	/* Truncate oversize input */
	if (len >= NAMEDATALEN)
		len = pg_mbcliplen(s_data, len, NAMEDATALEN - 1);

	/* Remove trailing blanks */
	while (len > 0)
	{
		if (s_data[len - 1] != ' ')
			break;
		len--;
	}

	/* We use palloc0 here to ensure result is zero-padded */
	memset(out, 0, NAMEDATALEN);
	memcpy(out, s_data, len);

	return len;
}

/*
 * name_bpchar, verbatim semantics: cstring_to_text(NameStr(*s)) — a strlen
 * walk over the NameData block + copy (shims 3/9: bounded strlen, out
 * buffer). The harness assumes the Name invariant name[NAMEDATALEN-1] == 0.
 */
int
pgvc_name_bpchar(const unsigned char *name, unsigned char *out)
{
	int			len = 0;

	while (len < NAMEDATALEN && name[len])
		len++;
	memcpy(out, name, len);
	return len;
}

/* bpchartruelen, verbatim */
static int
bpchartruelen(const unsigned char *s, int len)
{
	int			i;

	/*
	 * Note that we rely on the assumption that ' ' is a singleton unit on
	 * every supported multibyte server encoding.
	 */
	for (i = len - 1; i >= 0; i--)
	{
		if (s[i] != ' ')
			break;
	}
	return i + 1;
}

/* bpcharlen, verbatim (bcTruelen inlined over the payload pair, shim 2) */
int
pgvc_bpcharlen(const unsigned char *arg, int arglen)
{
	int			len;

	/* get number of bytes, ignoring trailing spaces */
	len = bpchartruelen(arg, arglen);

	/* in multibyte encoding, convert to number of characters */
	if (pg_database_encoding_max_length() != 1)
	{
		len = pg_mbstrlen_with_len(arg, len);
		if (len == PGVC_CERR)
			return PGVC_CERR;	/* shim 4 propagation */
	}

	return len;
}

/* bpcharoctetlen: toast_raw_datum_size(arg) - VARHDRSZ -> payload len
 * (shim 3; the function never detoasts) */
int
pgvc_bpcharoctetlen(int arglen)
{
	return arglen;
}
