/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/network_gist.c
 * (fetched 2026-07-30): inet_gist_consistent, calc_inet_union_params,
 * build_inet_union_key, inet_gist_compress (key-construction fragment),
 * inet_gist_fetch (value-construction fragment), inet_gist_penalty,
 * inet_gist_same.  Bodies verbatim.
 *
 * SHIMS (plumbing only, never logic):
 *  - GistInetKey struct verbatim from network_gist.c (all unsigned char
 *    fields, no padding — byte-compatible with the Rust 20-byte gk image).
 *  - pgc_inet value struct + ip_* macros as in proofs/network/csrc
 *    (inet varlena unwrapped to {family, bits, addr[16]}).
 *  - bitncmp/bitncommon resolve to pgc_bitncmp/pgc_bitncommon from
 *    c/pg_net_bits.c (copy of proofs/network/csrc/net_shim.c, which the
 *    network lane proved equivalent to the shipped Rust bitncmp/bitncommon;
 *    memcmp there is the byte-difference loop — see its header).
 *  - memcmp in inet_gist_same -> png_memcmp, first-differing-byte-difference
 *    loop; only ==0 is consumed, so any conforming memcmp agrees.
 *  - PG_FUNCTION_ARGS / PG_GETARG_* / PG_RETURN_* unwrapped to plain C
 *    signatures around the verbatim bodies; pointer out-params replace
 *    PG_RETURN_POINTER where the pointee is the compared value.
 *  - GISTENTRY reduced to { Datum key; } (C_GISTENTRY): the only member the
 *    vendored bodies read.  GIST_LEAF(ent) -> the leaf_flag parameter
 *    (gist.h's GIST_LEAF reads page state that fmgr hands alongside the
 *    entry; the flag is that bit of protocol state).
 *  - palloc0(sizeof(GistInetKey)) / palloc0(sizeof(inet)) -> caller passes a
 *    ZEROED result struct (palloc0's all-zero guarantee is part of the C
 *    contract for bytes past the copied prefix).
 *  - SET_GK_VARSIZE kept (1B short-varlena header IS part of the stored key
 *    image; SET_VARSIZE_SHORT expanded little-endian as in varatt.h).
 *  - SET_INET_VARSIZE(dst) dropped in the fetch fragment (4B varlena header
 *    housekeeping; the header is not part of the compared inet VALUE — the
 *    Rust harness asserts the Rust-side header against the varlena spec).
 *  - elog(ERROR, "unknown strategy...") -> PROOF_EREPORT_FLAG out-param
 *    (harnesses fence strategy to the opclass's 11 strategies, and a
 *    dedicated harness pins the err flag unreachable under the fence).
 *  - Assert() compiled out (release-build semantics, shim header).
 */

#include "../../support/c/pg_proof_shim.h"

typedef unsigned long Datum;
#define DatumGetPointer(X) ((char *) (X))

/* ---- inet value shim (proofs/network conventions) ---- */
typedef struct
{
	unsigned char family;
	unsigned char bits;
	unsigned char addr[16];
} pgc_inet;

#define PGSQL_AF_INET	2
#define PGSQL_AF_INET6	3

#define ip_family(inetptr)	((inetptr)->family)
#define ip_bits(inetptr)	((inetptr)->bits)
#define ip_addr(inetptr)	((inetptr)->addr)
#define ip_maxbits(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 32 : 128)
#define ip_addrsize(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 4 : 16)

typedef pgc_inet inet;

extern int pgc_bitncmp(const unsigned char *l, const unsigned char *r, int n);
extern int pgc_bitncommon(const unsigned char *l, const unsigned char *r, int n);
#define bitncmp pgc_bitncmp
#define bitncommon pgc_bitncommon

void *memcpy(void *dst, const void *src, unsigned long n);

static int
png_memcmp(const unsigned char *l, const unsigned char *r, unsigned long n)
{
	unsigned long i;

	for (i = 0; i < n; i++)
		if (l[i] != r[i])
			return (int) l[i] - (int) r[i];
	return 0;
}

/* ---- strategy numbers (access/stratnum.h values, verbatim) ---- */
#define INETSTRAT_OVERLAPS		3
#define INETSTRAT_EQ			18
#define INETSTRAT_NE			19
#define INETSTRAT_LT			20
#define INETSTRAT_LE			21
#define INETSTRAT_GT			22
#define INETSTRAT_GE			23
#define INETSTRAT_SUB			24
#define INETSTRAT_SUBEQ			25
#define INETSTRAT_SUP			26
#define INETSTRAT_SUPEQ			27

/* ---- GistInetKey, verbatim from network_gist.c ---- */
typedef struct GistInetKey
{
	uint8		va_header;		/* varlena header --- don't touch directly */
	unsigned char family;		/* PGSQL_AF_INET, PGSQL_AF_INET6, or zero */
	unsigned char minbits;		/* minimum number of bits in netmask */
	unsigned char commonbits;	/* number of common prefix bits in addresses */
	unsigned char ipaddr[16];	/* up to 128 bits of common address */
} GistInetKey;

#define DatumGetInetKeyP(X) ((GistInetKey *) DatumGetPointer(X))

#define gk_ip_family(gkptr)		((gkptr)->family)
#define gk_ip_minbits(gkptr)	((gkptr)->minbits)
#define gk_ip_commonbits(gkptr) ((gkptr)->commonbits)
#define gk_ip_addr(gkptr)		((gkptr)->ipaddr)
#define ip_family_maxbits(fam)	((fam) == PGSQL_AF_INET6 ? 128 : 32)

#define gk_ip_addrsize(gkptr) \
	(gk_ip_family(gkptr) == PGSQL_AF_INET6 ? 16 : 4)
#define gk_ip_maxbits(gkptr) \
	ip_family_maxbits(gk_ip_family(gkptr))

/* SET_VARSIZE_SHORT expanded (varatt.h little-endian): 1B header (len<<1)|1,
 * len = offsetof(GistInetKey, ipaddr) + addrsize. */
#define SET_GK_VARSIZE(dst) \
	((dst)->va_header = (uint8) ((((unsigned) ((char *) &(dst)->ipaddr[0] - (char *) (dst)) + gk_ip_addrsize(dst)) << 1) | 1))

/* GISTENTRY shim: only the key datum is read by the vendored bodies.
 * key is a pointer, not an integer Datum: CBMC loses pointer provenance
 * through integer smuggling (typed-staging rule, proofs/TRIAGE.md), and
 * DatumGetPointer below is then the identity cast. */
typedef struct
{
	const unsigned char *key;
} C_GISTENTRY;
#define GISTENTRY C_GISTENTRY

typedef uint16 OffsetNumber;

/*
 * The GiST query consistency check.
 * [verbatim body; fmgr unwrapped: key/query/strategy/leaf_flag/err params;
 *  GIST_LEAF(ent) -> leaf_flag; PG_RETURN_BOOL -> return; elog -> err flag]
 */
int
pg_inet_gist_consistent(const GistInetKey *key, const pgc_inet *query,
						uint16 strategy, int leaf_flag, int *err)
{
#define GIST_LEAF(ent) (leaf_flag)
	int			minbits,
				order;

	/*
	 * Check 0: different families
	 */
	if (gk_ip_family(key) == 0)
	{
		Assert(!GIST_LEAF(ent));
		return 1;
	}

	/*
	 * Check 1: different families
	 */
	if (gk_ip_family(key) != ip_family(query))
	{
		switch (strategy)
		{
			case INETSTRAT_LT:
			case INETSTRAT_LE:
				if (gk_ip_family(key) < ip_family(query))
					return 1;
				break;

			case INETSTRAT_GE:
			case INETSTRAT_GT:
				if (gk_ip_family(key) > ip_family(query))
					return 1;
				break;

			case INETSTRAT_NE:
				return 1;
		}
		/* For all other cases, we can be sure there is no match */
		return 0;
	}

	/*
	 * Check 2: network bit count
	 */
	switch (strategy)
	{
		case INETSTRAT_SUB:
			if (GIST_LEAF(ent) && gk_ip_minbits(key) <= ip_bits(query))
				return 0;
			break;

		case INETSTRAT_SUBEQ:
			if (GIST_LEAF(ent) && gk_ip_minbits(key) < ip_bits(query))
				return 0;
			break;

		case INETSTRAT_SUPEQ:
		case INETSTRAT_EQ:
			if (gk_ip_minbits(key) > ip_bits(query))
				return 0;
			break;

		case INETSTRAT_SUP:
			if (gk_ip_minbits(key) >= ip_bits(query))
				return 0;
			break;
	}

	/*
	 * Check 3: common network bits
	 */
	minbits = Min(gk_ip_commonbits(key), gk_ip_minbits(key));
	minbits = Min(minbits, ip_bits(query));

	order = bitncmp(gk_ip_addr(key), ip_addr(query), minbits);

	switch (strategy)
	{
		case INETSTRAT_SUB:
		case INETSTRAT_SUBEQ:
		case INETSTRAT_OVERLAPS:
		case INETSTRAT_SUPEQ:
		case INETSTRAT_SUP:
			return order == 0;

		case INETSTRAT_LT:
		case INETSTRAT_LE:
			if (order > 0)
				return 0;
			if (order < 0 || !GIST_LEAF(ent))
				return 1;
			break;

		case INETSTRAT_EQ:
			if (order != 0)
				return 0;
			if (!GIST_LEAF(ent))
				return 1;
			break;

		case INETSTRAT_GE:
		case INETSTRAT_GT:
			if (order < 0)
				return 0;
			if (order > 0 || !GIST_LEAF(ent))
				return 1;
			break;

		case INETSTRAT_NE:
			if (order != 0 || !GIST_LEAF(ent))
				return 1;
			break;
	}

	/*
	 * Remaining checks are only for leaves and basic comparison strategies.
	 */
	Assert(GIST_LEAF(ent));

	/*
	 * Check 4: network bit count
	 */
	switch (strategy)
	{
		case INETSTRAT_LT:
		case INETSTRAT_LE:
			if (gk_ip_minbits(key) < ip_bits(query))
				return 1;
			if (gk_ip_minbits(key) > ip_bits(query))
				return 0;
			break;

		case INETSTRAT_EQ:
			if (gk_ip_minbits(key) != ip_bits(query))
				return 0;
			break;

		case INETSTRAT_GE:
		case INETSTRAT_GT:
			if (gk_ip_minbits(key) > ip_bits(query))
				return 1;
			if (gk_ip_minbits(key) < ip_bits(query))
				return 0;
			break;

		case INETSTRAT_NE:
			if (gk_ip_minbits(key) != ip_bits(query))
				return 1;
			break;
	}

	/*
	 * Check 5: whole address
	 */
	order = bitncmp(gk_ip_addr(key), ip_addr(query), gk_ip_maxbits(key));

	switch (strategy)
	{
		case INETSTRAT_LT:
			return order < 0;

		case INETSTRAT_LE:
			return order <= 0;

		case INETSTRAT_EQ:
			return order == 0;

		case INETSTRAT_GE:
			return order >= 0;

		case INETSTRAT_GT:
			return order > 0;

		case INETSTRAT_NE:
			return order != 0;
	}

	PROOF_EREPORT_FLAG(err);	/* elog(ERROR, "unknown strategy for inet GiST") */
	return 0;					/* keep compiler quiet */
#undef GIST_LEAF
}

/*
 * Arity-fixed entry-array builders: the GISTENTRY array is assembled in C so
 * key pointers never cross the FFI boundary inside a struct field (CBMC
 * loses provenance on struct-smuggled pointers; typed-staging rule).
 */
int pg_calc_inet_union_params(C_GISTENTRY *ent, int m, int n,
							  int *minfamily_p, int *maxfamily_p,
							  int *minbits_p, int *commonbits_p);

int
pg_calc_union_params_2(const unsigned char *k1, const unsigned char *k2,
					   int *mf, int *xf, int *mb, int *cb)
{
	C_GISTENTRY ent[2];

	ent[0].key = k1;
	ent[1].key = k2;
	pg_calc_inet_union_params(ent, 0, 1, mf, xf, mb, cb);
	return 0;
}

int
pg_calc_union_params_3(const unsigned char *k1, const unsigned char *k2,
					   const unsigned char *k3,
					   int *mf, int *xf, int *mb, int *cb)
{
	C_GISTENTRY ent[3];

	ent[0].key = k1;
	ent[1].key = k2;
	ent[2].key = k3;
	pg_calc_inet_union_params(ent, 0, 2, mf, xf, mb, cb);
	return 0;
}

/*
 * Calculate parameters of the union of some GistInetKeys.
 * [verbatim body; GISTENTRY -> C_GISTENTRY shim]
 */
int
pg_calc_inet_union_params(C_GISTENTRY *ent,
						  int m, int n,
						  int *minfamily_p,
						  int *maxfamily_p,
						  int *minbits_p,
						  int *commonbits_p)
{
	int			minfamily,
				maxfamily,
				minbits,
				commonbits;
	unsigned char *addr;
	GistInetKey *tmp;
	int			i;

	/* Must be at least one key. */
	Assert(m <= n);

	/* Initialize variables using the first key. */
	tmp = DatumGetInetKeyP(ent[m].key);
	minfamily = maxfamily = gk_ip_family(tmp);
	minbits = gk_ip_minbits(tmp);
	commonbits = gk_ip_commonbits(tmp);
	addr = gk_ip_addr(tmp);

	/* Scan remaining keys. */
	for (i = m + 1; i <= n; i++)
	{
		tmp = DatumGetInetKeyP(ent[i].key);

		/* Determine range of family numbers */
		if (minfamily > gk_ip_family(tmp))
			minfamily = gk_ip_family(tmp);
		if (maxfamily < gk_ip_family(tmp))
			maxfamily = gk_ip_family(tmp);

		/* Find minimum minbits */
		if (minbits > gk_ip_minbits(tmp))
			minbits = gk_ip_minbits(tmp);

		/* Find minimum number of bits in common */
		if (commonbits > gk_ip_commonbits(tmp))
			commonbits = gk_ip_commonbits(tmp);
		if (commonbits > 0)
			commonbits = bitncommon(addr, gk_ip_addr(tmp), commonbits);
	}

	/* Force minbits/commonbits to zero if more than one family. */
	if (minfamily != maxfamily)
		minbits = commonbits = 0;

	*minfamily_p = minfamily;
	*maxfamily_p = maxfamily;
	*minbits_p = minbits;
	*commonbits_p = commonbits;
	return 0;					/* int shim: Kani lowers Rust () as struct Unit */
}

/*
 * Construct a GistInetKey representing a union value.
 * [verbatim body; palloc0 -> caller-zeroed *result]
 */
int
pg_build_inet_union_key(int family, int minbits, int commonbits,
						const unsigned char *addr, GistInetKey *result)
{
	gk_ip_family(result) = family;
	gk_ip_minbits(result) = minbits;
	gk_ip_commonbits(result) = commonbits;

	/* Clone appropriate bytes of the address. */
	if (commonbits > 0)
		memcpy(gk_ip_addr(result), addr, (commonbits + 7) / 8);

	/* Clean any unwanted bits in the last partial byte. */
	if (commonbits % 8 != 0)
		gk_ip_addr(result)[commonbits / 8] &= ~(0xFF >> (commonbits % 8));

	/* Set varlena header correctly. */
	SET_GK_VARSIZE(result);
	return 0;					/* int shim */
}

/*
 * inet_gist_compress: leaf-key construction fragment.
 * [verbatim inner block of the entry->leafkey && key != NULL arm;
 *  palloc0 -> caller-zeroed *r; the gistentryinit protocol fields
 *  (offset preserved, leafkey false, page preserved) are asserted
 *  spec-side in the Rust harness]
 */
int
pg_inet_gist_compress_key(const pgc_inet *in, GistInetKey *r)
{
	gk_ip_family(r) = ip_family(in);
	gk_ip_minbits(r) = ip_bits(in);
	gk_ip_commonbits(r) = gk_ip_maxbits(r);
	memcpy(gk_ip_addr(r), ip_addr(in), gk_ip_addrsize(r));
	SET_GK_VARSIZE(r);
	return 0;					/* int shim */
}

/*
 * inet_gist_fetch: inet value construction fragment.
 * [verbatim body minus GISTENTRY/palloc plumbing; palloc0 -> caller-zeroed
 *  *dst; SET_INET_VARSIZE dropped per header]
 */
int
pg_inet_gist_fetch_val(const GistInetKey *key, pgc_inet *dst)
{
	ip_family(dst) = gk_ip_family(key);
	ip_bits(dst) = gk_ip_minbits(key);
	memcpy(ip_addr(dst), gk_ip_addr(key), ip_addrsize(dst));
	return 0;					/* int shim */
}

/*
 * The GiST page split penalty function.
 * [verbatim body; fmgr unwrapped; float out-param retained]
 */
int
pg_inet_gist_penalty(const GistInetKey *orig, const GistInetKey *newk,
					 float *penalty)
{
	int			commonbits;

	if (gk_ip_family(orig) == gk_ip_family(newk))
	{
		if (gk_ip_minbits(orig) <= gk_ip_minbits(newk))
		{
			commonbits = bitncommon(gk_ip_addr(orig), gk_ip_addr(newk),
									Min(gk_ip_commonbits(orig),
										gk_ip_commonbits(newk)));
			if (commonbits > 0)
				*penalty = 1.0f / commonbits;
			else
				*penalty = 2;
		}
		else
			*penalty = 3;
	}
	else
		*penalty = 4;
	return 0;					/* int shim */
}

/*
 * The GiST equality function.
 * [verbatim body; fmgr unwrapped; memcmp -> png_memcmp (==0 only consumed)]
 */
int
pg_inet_gist_same(const GistInetKey *left, const GistInetKey *right)
{
	int			result;

	result = (gk_ip_family(left) == gk_ip_family(right) &&
			  gk_ip_minbits(left) == gk_ip_minbits(right) &&
			  gk_ip_commonbits(left) == gk_ip_commonbits(right) &&
			  png_memcmp(gk_ip_addr(left), gk_ip_addr(right),
						 gk_ip_addrsize(left)) == 0);

	return result;
}
