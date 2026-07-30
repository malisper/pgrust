/*
 * Vendored PostgreSQL C for the bytea comparator-family proofs.
 *
 * Provenance: fetched 2026-07-28 from postgres/postgres master
 * src/backend/utils/adt/bytea.c (byteaeq..byteacmp, lines ~813-980).
 * REL_18_STABLE ref: src/backend/utils/adt/varlena.c (byteaeq..byteacmp,
 * lines ~3918-4062) — REL_18 keeps these functions in varlena.c; the
 * bytea.c split is master-era (post-18). Bodies byte-identical, zero code
 * drift (provenance audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * SHIMS (everything else is verbatim):
 *  - names pg_-prefixed; postgres typedefs inlined (Size -> size_t,
 *    int32 -> int); Min() and VARHDRSZ defined per c.h / varatt.h.
 *  - DETOASTING IS OUT OF SCOPE.  The fmgr wrappers operate on possibly
 *    toasted varlena; the C caller contract post-PG_GETARG_BYTEA_PP is a
 *    detoasted (possibly short-header) varlena, from which the body only
 *    ever uses VARDATA_ANY (payload pointer) and VARSIZE_ANY_EXHDR
 *    (payload length).  Each function is therefore shimmed to plain
 *    (const unsigned char *data, len) pairs:
 *      PG_GETARG_BYTEA_PP(n) + VARDATA_ANY / VARSIZE_ANY_EXHDR
 *        -> (dN, lenN) parameters
 *      byteaeq/byteane's toast_raw_datum_size(argN)
 *        -> lenN + VARHDRSZ  (raw size = payload + 4-byte header; the
 *           fast-path inequality test and the later `len1 - VARHDRSZ`
 *           memcmp count are kept verbatim)
 *      PG_FREE_IF_COPY -> dropped (memory management, no value effect)
 *      PG_RETURN_BOOL  -> int return (0/1); Kani lowers Rust bool/() in
 *                         ways goto-cc rejects against C _Bool/void
 *      PG_RETURN_INT32 -> int return
 *  - memcmp is CBMC's built-in model (byte loop returning the difference
 *    of the first mismatching unsigned chars — the glibc convention the
 *    shipped Rust core documents at varlena/src/lib.rs:122).
 */

#include <stddef.h>
#include <string.h>

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define VARHDRSZ ((size_t) 4)

int
pg_byteaeq(const unsigned char *d1, size_t rawlen1_exhdr,
		   const unsigned char *d2, size_t rawlen2_exhdr)
{
	int			result;			/* shim: bool -> int */
	size_t		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = rawlen1_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg1) */
	len2 = rawlen2_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg2) */
	if (len1 != len2)
		result = 0;
	else
	{
		result = (memcmp(d1, d2, len1 - VARHDRSZ) == 0);
	}

	return result;
}

int
pg_byteane(const unsigned char *d1, size_t rawlen1_exhdr,
		   const unsigned char *d2, size_t rawlen2_exhdr)
{
	int			result;			/* shim: bool -> int */
	size_t		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = rawlen1_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg1) */
	len2 = rawlen2_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg2) */
	if (len1 != len2)
		result = 1;
	else
	{
		result = (memcmp(d1, d2, len1 - VARHDRSZ) != 0);
	}

	return result;
}

int
pg_bytealt(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp < 0) || ((cmp == 0) && (len1 < len2));
}

int
pg_byteale(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp < 0) || ((cmp == 0) && (len1 <= len2));
}

int
pg_byteagt(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp > 0) || ((cmp == 0) && (len1 > len2));
}

int
pg_byteage(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp > 0) || ((cmp == 0) && (len1 >= len2));
}

int
pg_byteacmp(const unsigned char *d1, int len1,
			const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));
	if ((cmp == 0) && (len1 != len2))
		cmp = (len1 < len2) ? -1 : 1;

	return cmp;
}

/*
 * bytea_larger / bytea_smaller (pg_proc oids 6393/6394).
 *
 * Provenance: src/backend/utils/adt/varlena.c, postgres/postgres
 * REL_18_STABLE, fetched 2026-07-28.
 *
 * SHIMS (comparison/selection expressions verbatim):
 *  - same (data, len) pair shim as the comparators above;
 *  - the C function returns the WINNING INPUT POINTER
 *    (PG_RETURN_BYTEA_P(result) where result is arg1 or arg2); shimmed to
 *    return 1 when result == arg1 and 2 when result == arg2, so the
 *    harness can assert winning-input identity against the Rust
 *    reference-returning core.
 */

int
pg_bytea_larger(const unsigned char *d1, int len1,
				const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return ((cmp > 0) || ((cmp == 0) && (len1 > len2)) ? 1 : 2);
}

int
pg_bytea_smaller(const unsigned char *d1, int len1,
				 const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return ((cmp < 0) || ((cmp == 0) && (len1 < len2)) ? 1 : 2);
}

/*
 * byteaGetByte / byteaGetBit / byteaSetByte / byteaSetBit
 * (pg_proc oids 721 / 723 / 722 / 724 — extraction-gap wave 2026-07-28).
 *
 * Provenance: src/backend/utils/adt/varlena.c, postgres/postgres
 * REL_18_STABLE, lines ~3305-3455, fetched 2026-07-28.  (REL_18 keeps
 * these in varlena.c; the bytea.c split is master-era.)
 *
 * SHIMS (all logic verbatim; this list is exhaustive):
 *  - shared typedefs via pg_proof_shim.h (int32/int64; its Min/VARHDRSZ
 *    redefinitions are token-identical to the ones above — benign).
 *  - Get*: PG_GETARG_BYTEA_PP(0) + VARSIZE_ANY_EXHDR(v)/VARDATA_ANY(v)
 *    -> (vdata, len) parameters, same pre-detoasted caller contract as the
 *    comparator shims above.  PG_GETARG_INT32/INT64 -> plain args;
 *    PG_RETURN_INT32 -> int return.
 *  - Set*: PG_GETARG_BYTEA_P_COPY(0) makes C mutate a private copy and
 *    return it; shimmed to a caller-provided mutable payload buffer `res`
 *    that the HARNESS pre-fills with the input bytes (the copy), with
 *    `len = VARSIZE(res) - VARHDRSZ` -> len parameter.  VARDATA(res) ->
 *    res.  PG_RETURN_BYTEA_P(res) -> return 0 (the result IMAGE is the
 *    mutated buffer, byte-compared by the harness); C's returned image
 *    length == input length is represented by the buffer having exactly
 *    len bytes.
 *  - ereport(ERROR, ...) -> PROOF_EREPORT_FLAG out-param + early return 0
 *    at the exact ereport program point (message text never crosses the
 *    seam).  Per the shim-header convention, distinct flag values encode
 *    the errcode: *err = 1 for ERRCODE_ARRAY_SUBSCRIPT_ERROR (2202E),
 *    *err = 2 for ERRCODE_INVALID_PARAMETER_VALUE (22023, byteaSetBit's
 *    "new bit must be 0 or 1").
 *  - THEOREM PLANES kept verbatim and in-proof: byteaSetByte's
 *    `((unsigned char *) VARDATA(res))[n] = newByte;` int->unsigned char
 *    truncating store (matches Rust `new_byte as u8`); byteaGetBit/
 *    SetBit's `(int64) len * 8` widening and n/8, n%8 index math; the
 *    range-check-THEN-bit-value-check order in byteaSetBit.
 */

#include "../../support/c/pg_proof_shim.h"

int
pg_byteaGetByte(const unsigned char *vdata, int len, int32 n, int *err)
{
	int			byte;

	/* shim: len = VARSIZE_ANY_EXHDR(v) */

	if (n < 0 || n >= len)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
		 * errmsg("index %d out of valid range, 0..%d", n, len - 1)) */
		*err = 1;
		return 0;
	}

	byte = vdata[n];			/* shim: ((unsigned char *) VARDATA_ANY(v))[n] */

	return byte;
}

int
pg_byteaGetBit(const unsigned char *vdata, int len, int64 n, int *err)
{
	int			byteNo,
				bitNo;
	int			byte;

	/* shim: len = VARSIZE_ANY_EXHDR(v) */

	if (n < 0 || n >= (int64) len * 8)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
		 * errmsg("index %PRId64 out of valid range, 0..%PRId64",
		 * n, (int64) len * 8 - 1)) */
		*err = 1;
		return 0;
	}

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	byte = vdata[byteNo];		/* shim: ((unsigned char *) VARDATA_ANY(v))[byteNo] */

	if (byte & (1 << bitNo))
		return 1;				/* shim: PG_RETURN_INT32(1) */
	else
		return 0;				/* shim: PG_RETURN_INT32(0) */
}

int
pg_byteaSetByte(unsigned char *res, int len, int32 n, int32 newByte, int *err)
{
	/* shim: res = payload of PG_GETARG_BYTEA_P_COPY(0);
	 * len = VARSIZE(res) - VARHDRSZ */

	if (n < 0 || n >= len)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), ...) */
		*err = 1;
		return 0;
	}

	/*
	 * Now set the byte.
	 */
	res[n] = newByte;			/* shim: ((unsigned char *) VARDATA(res))[n]
								 * = newByte; — truncating store in-theorem */

	return 0;					/* shim: PG_RETURN_BYTEA_P(res) */
}

int
pg_byteaSetBit(unsigned char *res, int len, int64 n, int32 newBit, int *err)
{
	int			oldByte,
				newByte;
	int			byteNo,
				bitNo;

	/* shim: res = payload of PG_GETARG_BYTEA_P_COPY(0);
	 * len = VARSIZE(res) - VARHDRSZ */

	if (n < 0 || n >= (int64) len * 8)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), ...) */
		*err = 1;
		return 0;
	}

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		 * errmsg("new bit must be 0 or 1")) */
		*err = 2;
		return 0;
	}

	/*
	 * Update the byte.
	 */
	oldByte = res[byteNo];		/* shim: ((unsigned char *) VARDATA(res))[byteNo] */

	if (newBit == 0)
		newByte = oldByte & (~(1 << bitNo));
	else
		newByte = oldByte | (1 << bitNo);

	res[byteNo] = newByte;		/* shim: ((unsigned char *) VARDATA(res))[byteNo] */

	return 0;					/* shim: PG_RETURN_BYTEA_P(res) */
}
