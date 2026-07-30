/*
 * Vendored PostgreSQL C for the text/bpchar/name-cross comparator-family
 * Kani parity proofs.
 *
 * Provenance:
 *   - src/backend/utils/adt/varlena.c  (postgres/postgres REL_18_STABLE,
 *     fetched 2026-07-28): varstr_cmp, text_cmp, texteq, textne, text_lt,
 *     text_le, text_gt, text_ge, bttextcmp, text_larger, text_smaller,
 *     nameeqtext, texteqname, namenetext, textnename, btnametextcmp,
 *     bttextnamecmp, namelttext/nameletext/namegttext/namegetext,
 *     textltname/textlename/textgtname/textgename (CmpCall family),
 *     internal_text_pattern_compare, text_pattern_lt/le/ge/gt,
 *     bttext_pattern_cmp.
 *   - src/backend/utils/adt/varchar.c  (postgres/postgres REL_18_STABLE,
 *     fetched 2026-07-28): bpchartruelen, bcTruelen, bpchareq, bpcharne,
 *     bpcharlt/le/gt/ge, bpcharcmp, bpchar_larger, bpchar_smaller,
 *     internal_bpchar_pattern_compare, bpchar_pattern_lt/le/ge/gt,
 *     btbpchar_pattern_cmp.
 *
 * SHIMS (everything else is verbatim):
 *  1. Names pg_-prefixed; postgres typedefs inlined (Size -> size_t,
 *     int32 -> int, Oid -> unsigned int, bool -> int); Min() and VARHDRSZ
 *     defined per c.h / varatt.h.
 *  2. fmgr plumbing: PG_GETARG_TEXT_PP / PG_GETARG_BPCHAR_PP +
 *     VARDATA_ANY / VARSIZE_ANY_EXHDR -> plain (const unsigned char *data,
 *     int len) parameter pairs. DETOASTING IS OUT OF SCOPE: inputs model
 *     the post-PG_GETARG_*_PP caller contract (pre-detoasted payloads) —
 *     the established varlena harness pattern (proofs/bytea-cmp).
 *     PG_GETARG_NAME -> const unsigned char * (64-byte NAMEDATALEN block).
 *     PG_FREE_IF_COPY -> dropped (memory management, no value effect).
 *     PG_RETURN_BOOL/INT32 -> int return.
 *  3. texteq/textne's toast_raw_datum_size(argN) -> lenN + VARHDRSZ (raw
 *     size = payload + 4-byte header); the fast-path length inequality and
 *     the later `len1 - VARHDRSZ` memcmp count are kept verbatim (same shim
 *     as proofs/bytea-cmp byteaeq/byteane).
 *  4. COLLATION FENCE: pg_newlocale_from_collation(collid)->collate_is_c
 *     and ->deterministic -> the pg_collate_is_c / pg_collate_deterministic
 *     models below: true for C_COLLATION_OID / POSIX_COLLATION_OID (the
 *     built-in C-locale collations: collate_is_c and deterministic are both
 *     catalog-invariant true for them), POISONED otherwise. The non-C
 *     varstr_cmp arm (pg_strncoll/locale) returns the poison sentinel
 *     -2147483647, and the nondeterministic texteq/bpchareq arms return the
 *     out-of-domain verdict -1 — any harness that reaches them fails loudly.
 *     Harnesses fence collid == C_COLLATION_OID; non-C collations route to
 *     locale code on BOTH sides and are out of proof scope (same fence as
 *     proofs/name-ascii).
 *  5. check_collation_set(collid) -> kept as a guard: invalid collid (0)
 *     would ereport on both sides; here it poisons (callers fence collid
 *     to a valid constant, so the arm is dead in every harness).
 *  6. libc strlen (NameStr length in the name-cross family) ->
 *     pg_ref_strlen byte loop (CBMC has no libc strlen model; precedent
 *     proofs/name-ascii shim 3).
 *  7. text_larger/text_smaller/bpchar_larger/bpchar_smaller return the
 *     winning ARGUMENT (C returns one of the detoasted input pointers);
 *     shimmed to a winner INDEX (0 = arg1, 1 = arg2). The harness checks
 *     the shipped wrapper returned the matching input image pointer.
 *  8. memcmp is CBMC's built-in model (byte loop returning the difference
 *     of the first mismatching unsigned chars — the glibc convention the
 *     shipped Rust core documents at varlena/src/lib.rs:122; ratified by
 *     the bytea-cmp/network family proofs).
 *  9. varchar.c's bpchartruelen takes `char *s`; parameter widened to
 *     const unsigned char * (the loop only compares against ' ' = 0x20,
 *     signedness-invisible).
 */

#include <stddef.h>
#include <string.h>

typedef unsigned int Oid;

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define VARHDRSZ ((size_t) 4)
#define C_COLLATION_OID 950		/* catalog/pg_collation.h */
#define POSIX_COLLATION_OID 951

#define POISON_CMP (-2147483647)	/* out-of-fence sentinel (shim 4) */

/* --- shim 6: strlen reference --- */
static int
pg_ref_strlen(const unsigned char *s)
{
	int			n = 0;

	while (s[n] != 0)
		n++;
	return n;
}

/* --- shim 4: pg_newlocale_from_collation field models --- */
static int
pg_collate_is_c(Oid collid)
{
	return collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID;
}

static int
pg_collate_deterministic(Oid collid)
{
	/* C/POSIX are deterministic; anything else is out of the fence. */
	return pg_collate_is_c(collid);
}

/* --- shim 5: check_collation_set -> poison-valued guard --- */
static int
pg_collation_is_set(Oid collid)
{
	return collid != 0;			/* OidIsValid */
}

/*
 * varstr_cmp (varlena.c REL_18, C-collation arm verbatim; the locale arm
 * is out of the proof fence and poisoned — shim 4).
 */
int
pg_varstr_cmp(const unsigned char *arg1, int len1,
			  const unsigned char *arg2, int len2, Oid collid)
{
	int			result;

	if (!pg_collation_is_set(collid))
		return POISON_CMP;		/* shim 5: ereport(indeterminate collation) */

	if (pg_collate_is_c(collid))
	{
		result = memcmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
	}
	else
	{
		/* pg_strncoll / locale path: out of proof scope (shim 4) */
		result = POISON_CMP;
	}

	return result;
}

/* text_cmp (varlena.c): VARDATA_ANY/VARSIZE_ANY_EXHDR -> (ptr,len) (shim 2) */
static int
pg_text_cmp(const unsigned char *a1p, int len1,
			const unsigned char *a2p, int len2, Oid collid)
{
	return pg_varstr_cmp(a1p, len1, a2p, len2, collid);
}

/* ---------------- text comparators (varlena.c) ---------------- */

int
pg_texteq(const unsigned char *d1, size_t rawlen1_exhdr,
		  const unsigned char *d2, size_t rawlen2_exhdr, Oid collid)
{
	int			result;			/* shim: bool -> int */

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	if (pg_collate_deterministic(collid))
	{
		size_t		len1,
					len2;

		/*
		 * Since we only care about equality or not-equality, we can avoid all
		 * the expense of strcoll() here, and just do bitwise comparison.  In
		 * fact, we don't even have to do a bitwise comparison if we can show
		 * the lengths of the strings are unequal; which might save us from
		 * having to detoast one or both values.
		 */
		len1 = rawlen1_exhdr + VARHDRSZ;	/* shim 3: toast_raw_datum_size */
		len2 = rawlen2_exhdr + VARHDRSZ;	/* shim 3 */
		if (len1 != len2)
			result = 0;
		else
			result = (memcmp(d1, d2, len1 - VARHDRSZ) == 0);
	}
	else
	{
		/* nondeterministic-collation arm: out of proof scope (shim 4) */
		result = -1;
	}

	return result;
}

int
pg_textne(const unsigned char *d1, size_t rawlen1_exhdr,
		  const unsigned char *d2, size_t rawlen2_exhdr, Oid collid)
{
	int			result;			/* shim: bool -> int */

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	if (pg_collate_deterministic(collid))
	{
		size_t		len1,
					len2;

		/* See comment in texteq() */
		len1 = rawlen1_exhdr + VARHDRSZ;	/* shim 3 */
		len2 = rawlen2_exhdr + VARHDRSZ;	/* shim 3 */
		if (len1 != len2)
			result = 1;
		else
			result = (memcmp(d1, d2, len1 - VARHDRSZ) != 0);
	}
	else
	{
		/* nondeterministic-collation arm: out of proof scope (shim 4) */
		result = -1;
	}

	return result;
}

int
pg_text_lt(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2, Oid collid)
{
	return pg_text_cmp(d1, len1, d2, len2, collid) < 0;
}

int
pg_text_le(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2, Oid collid)
{
	return pg_text_cmp(d1, len1, d2, len2, collid) <= 0;
}

int
pg_text_gt(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2, Oid collid)
{
	return pg_text_cmp(d1, len1, d2, len2, collid) > 0;
}

int
pg_text_ge(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2, Oid collid)
{
	return pg_text_cmp(d1, len1, d2, len2, collid) >= 0;
}

int
pg_bttextcmp(const unsigned char *d1, int len1,
			 const unsigned char *d2, int len2, Oid collid)
{
	return pg_text_cmp(d1, len1, d2, len2, collid);
}

/* winner index (shim 7): C returns (text_cmp > 0) ? arg1 : arg2 */
int
pg_text_larger(const unsigned char *d1, int len1,
			   const unsigned char *d2, int len2, Oid collid)
{
	return (pg_text_cmp(d1, len1, d2, len2, collid) > 0) ? 0 : 1;
}

/* winner index (shim 7): C returns (text_cmp < 0) ? arg1 : arg2 */
int
pg_text_smaller(const unsigned char *d1, int len1,
				const unsigned char *d2, int len2, Oid collid)
{
	return (pg_text_cmp(d1, len1, d2, len2, collid) < 0) ? 0 : 1;
}

/* ---------------- name <-> text cross comparators (varlena.c) ------------ */

int
pg_nameeqtext(const unsigned char *name1,
			  const unsigned char *d2, size_t len2, Oid collid)
{
	size_t		len1 = pg_ref_strlen(name1);	/* shim 6 */
	int			result;

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	if (collid == C_COLLATION_OID)
		result = (len1 == len2 &&
				  memcmp(name1, d2, len1) == 0);
	else
		result = (pg_varstr_cmp(name1, len1,
								d2, len2,
								collid) == 0);

	return result;
}

int
pg_texteqname(const unsigned char *d1, size_t len1,
			  const unsigned char *name2, Oid collid)
{
	size_t		len2 = pg_ref_strlen(name2);	/* shim 6 */
	int			result;

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	if (collid == C_COLLATION_OID)
		result = (len1 == len2 &&
				  memcmp(d1, name2, len1) == 0);
	else
		result = (pg_varstr_cmp(d1, len1,
								name2, len2,
								collid) == 0);

	return result;
}

int
pg_namenetext(const unsigned char *name1,
			  const unsigned char *d2, size_t len2, Oid collid)
{
	size_t		len1 = pg_ref_strlen(name1);	/* shim 6 */
	int			result;

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	if (collid == C_COLLATION_OID)
		result = !(len1 == len2 &&
				   memcmp(name1, d2, len1) == 0);
	else
		result = !(pg_varstr_cmp(name1, len1,
								 d2, len2,
								 collid) == 0);

	return result;
}

int
pg_textnename(const unsigned char *d1, size_t len1,
			  const unsigned char *name2, Oid collid)
{
	size_t		len2 = pg_ref_strlen(name2);	/* shim 6 */
	int			result;

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	if (collid == C_COLLATION_OID)
		result = !(len1 == len2 &&
				   memcmp(d1, name2, len1) == 0);
	else
		result = !(pg_varstr_cmp(d1, len1,
								 name2, len2,
								 collid) == 0);

	return result;
}

int
pg_btnametextcmp(const unsigned char *name1,
				 const unsigned char *d2, int len2, Oid collid)
{
	return pg_varstr_cmp(name1, pg_ref_strlen(name1),
						 d2, len2,
						 collid);
}

int
pg_bttextnamecmp(const unsigned char *d1, int len1,
				 const unsigned char *name2, Oid collid)
{
	return pg_varstr_cmp(d1, len1,
						 name2, pg_ref_strlen(name2),
						 collid);
}

/* CmpCall(btnametextcmp/bttextnamecmp) family (varlena.c) */

int
pg_namelttext(const unsigned char *name1,
			  const unsigned char *d2, int len2, Oid collid)
{
	return pg_btnametextcmp(name1, d2, len2, collid) < 0;
}

int
pg_nameletext(const unsigned char *name1,
			  const unsigned char *d2, int len2, Oid collid)
{
	return pg_btnametextcmp(name1, d2, len2, collid) <= 0;
}

int
pg_namegttext(const unsigned char *name1,
			  const unsigned char *d2, int len2, Oid collid)
{
	return pg_btnametextcmp(name1, d2, len2, collid) > 0;
}

int
pg_namegetext(const unsigned char *name1,
			  const unsigned char *d2, int len2, Oid collid)
{
	return pg_btnametextcmp(name1, d2, len2, collid) >= 0;
}

int
pg_textltname(const unsigned char *d1, int len1,
			  const unsigned char *name2, Oid collid)
{
	return pg_bttextnamecmp(d1, len1, name2, collid) < 0;
}

int
pg_textlename(const unsigned char *d1, int len1,
			  const unsigned char *name2, Oid collid)
{
	return pg_bttextnamecmp(d1, len1, name2, collid) <= 0;
}

int
pg_textgtname(const unsigned char *d1, int len1,
			  const unsigned char *name2, Oid collid)
{
	return pg_bttextnamecmp(d1, len1, name2, collid) > 0;
}

int
pg_textgename(const unsigned char *d1, int len1,
			  const unsigned char *name2, Oid collid)
{
	return pg_bttextnamecmp(d1, len1, name2, collid) >= 0;
}

/* ---------------- text pattern ops (varlena.c) ---------------- */

static int
pg_internal_text_pattern_compare(const unsigned char *d1, int len1,
								 const unsigned char *d2, int len2)
{
	int			result;

	result = memcmp(d1, d2, Min(len1, len2));
	if (result != 0)
		return result;
	else if (len1 < len2)
		return -1;
	else if (len1 > len2)
		return 1;
	else
		return 0;
}

int
pg_text_pattern_lt(const unsigned char *d1, int len1,
				   const unsigned char *d2, int len2)
{
	return pg_internal_text_pattern_compare(d1, len1, d2, len2) < 0;
}

int
pg_text_pattern_le(const unsigned char *d1, int len1,
				   const unsigned char *d2, int len2)
{
	return pg_internal_text_pattern_compare(d1, len1, d2, len2) <= 0;
}

int
pg_text_pattern_ge(const unsigned char *d1, int len1,
				   const unsigned char *d2, int len2)
{
	return pg_internal_text_pattern_compare(d1, len1, d2, len2) >= 0;
}

int
pg_text_pattern_gt(const unsigned char *d1, int len1,
				   const unsigned char *d2, int len2)
{
	return pg_internal_text_pattern_compare(d1, len1, d2, len2) > 0;
}

int
pg_bttext_pattern_cmp(const unsigned char *d1, int len1,
					  const unsigned char *d2, int len2)
{
	return pg_internal_text_pattern_compare(d1, len1, d2, len2);
}

/* ---------------- bpchar comparators (varchar.c) ---------------- */

/* bpchartruelen (varchar.c REL_18, verbatim modulo shim 9) */
int
pg_bpchartruelen(const unsigned char *s, int len)
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

/* bcTruelen: VARDATA_ANY/VARSIZE_ANY_EXHDR -> (ptr,len) (shim 2) */
static int
pg_bcTruelen(const unsigned char *d, int len)
{
	return pg_bpchartruelen(d, len);
}

int
pg_bpchareq(const unsigned char *d1, int rawlen1,
			const unsigned char *d2, int rawlen2, Oid collid)
{
	int			len1,
				len2;
	int			result;			/* shim: bool -> int */

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	len1 = pg_bcTruelen(d1, rawlen1);
	len2 = pg_bcTruelen(d2, rawlen2);

	if (pg_collate_deterministic(collid))
	{
		/*
		 * Since we only care about equality or not-equality, we can avoid all
		 * the expense of strcoll() here, and just do bitwise comparison.
		 */
		if (len1 != len2)
			result = 0;
		else
			result = (memcmp(d1, d2, len1) == 0);
	}
	else
	{
		/* nondeterministic-collation arm: out of proof scope (shim 4) */
		result = -1;
	}

	return result;
}

int
pg_bpcharne(const unsigned char *d1, int rawlen1,
			const unsigned char *d2, int rawlen2, Oid collid)
{
	int			len1,
				len2;
	int			result;			/* shim: bool -> int */

	if (!pg_collation_is_set(collid))
		return -1;				/* shim 5 */

	len1 = pg_bcTruelen(d1, rawlen1);
	len2 = pg_bcTruelen(d2, rawlen2);

	if (pg_collate_deterministic(collid))
	{
		/*
		 * Since we only care about equality or not-equality, we can avoid all
		 * the expense of strcoll() here, and just do bitwise comparison.
		 */
		if (len1 != len2)
			result = 1;
		else
			result = (memcmp(d1, d2, len1) != 0);
	}
	else
	{
		/* nondeterministic-collation arm: out of proof scope (shim 4) */
		result = -1;
	}

	return result;
}

static int
pg_bpchar_cmp_internal(const unsigned char *d1, int rawlen1,
					   const unsigned char *d2, int rawlen2, Oid collid)
{
	int			len1,
				len2;

	len1 = pg_bcTruelen(d1, rawlen1);
	len2 = pg_bcTruelen(d2, rawlen2);

	return pg_varstr_cmp(d1, len1, d2, len2, collid);
}

int
pg_bpcharlt(const unsigned char *d1, int rawlen1,
			const unsigned char *d2, int rawlen2, Oid collid)
{
	return pg_bpchar_cmp_internal(d1, rawlen1, d2, rawlen2, collid) < 0;
}

int
pg_bpcharle(const unsigned char *d1, int rawlen1,
			const unsigned char *d2, int rawlen2, Oid collid)
{
	return pg_bpchar_cmp_internal(d1, rawlen1, d2, rawlen2, collid) <= 0;
}

int
pg_bpchargt(const unsigned char *d1, int rawlen1,
			const unsigned char *d2, int rawlen2, Oid collid)
{
	return pg_bpchar_cmp_internal(d1, rawlen1, d2, rawlen2, collid) > 0;
}

int
pg_bpcharge(const unsigned char *d1, int rawlen1,
			const unsigned char *d2, int rawlen2, Oid collid)
{
	return pg_bpchar_cmp_internal(d1, rawlen1, d2, rawlen2, collid) >= 0;
}

int
pg_bpcharcmp(const unsigned char *d1, int rawlen1,
			 const unsigned char *d2, int rawlen2, Oid collid)
{
	return pg_bpchar_cmp_internal(d1, rawlen1, d2, rawlen2, collid);
}

/* winner index (shim 7): C returns (cmp >= 0) ? arg1 : arg2 */
int
pg_bpchar_larger(const unsigned char *d1, int rawlen1,
				 const unsigned char *d2, int rawlen2, Oid collid)
{
	return (pg_bpchar_cmp_internal(d1, rawlen1, d2, rawlen2, collid) >= 0) ? 0 : 1;
}

/* winner index (shim 7): C returns (cmp <= 0) ? arg1 : arg2 */
int
pg_bpchar_smaller(const unsigned char *d1, int rawlen1,
				  const unsigned char *d2, int rawlen2, Oid collid)
{
	return (pg_bpchar_cmp_internal(d1, rawlen1, d2, rawlen2, collid) <= 0) ? 0 : 1;
}

/* ---------------- bpchar pattern ops (varchar.c) ---------------- */

static int
pg_internal_bpchar_pattern_compare(const unsigned char *d1, int rawlen1,
								   const unsigned char *d2, int rawlen2)
{
	int			result;
	int			len1,
				len2;

	len1 = pg_bcTruelen(d1, rawlen1);
	len2 = pg_bcTruelen(d2, rawlen2);

	result = memcmp(d1, d2, Min(len1, len2));
	if (result != 0)
		return result;
	else if (len1 < len2)
		return -1;
	else if (len1 > len2)
		return 1;
	else
		return 0;
}

int
pg_bpchar_pattern_lt(const unsigned char *d1, int rawlen1,
					 const unsigned char *d2, int rawlen2)
{
	return pg_internal_bpchar_pattern_compare(d1, rawlen1, d2, rawlen2) < 0;
}

int
pg_bpchar_pattern_le(const unsigned char *d1, int rawlen1,
					 const unsigned char *d2, int rawlen2)
{
	return pg_internal_bpchar_pattern_compare(d1, rawlen1, d2, rawlen2) <= 0;
}

int
pg_bpchar_pattern_ge(const unsigned char *d1, int rawlen1,
					 const unsigned char *d2, int rawlen2)
{
	return pg_internal_bpchar_pattern_compare(d1, rawlen1, d2, rawlen2) >= 0;
}

int
pg_bpchar_pattern_gt(const unsigned char *d1, int rawlen1,
					 const unsigned char *d2, int rawlen2)
{
	return pg_internal_bpchar_pattern_compare(d1, rawlen1, d2, rawlen2) > 0;
}

int
pg_btbpchar_pattern_cmp(const unsigned char *d1, int rawlen1,
						const unsigned char *d2, int rawlen2)
{
	return pg_internal_bpchar_pattern_compare(d1, rawlen1, d2, rawlen2);
}
