/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-29 via
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE):
 *
 *   src/backend/utils/adt/network_spgist.c — inet_spg_config,
 *       inet_spg_choose, inet_spg_inner_consistent, inet_spg_leaf_consistent,
 *       inet_spg_node_number, inet_spg_consistent_bitmap
 *       (inet_spg_picksplit NOT vendored: ledger 3797 stays
 *        blocked(refactor: symbolic nTuples + mcx node arrays))
 *   src/backend/utils/adt/network.c — bitncmp, bitncommon,
 *       cidr_set_masklen_internal
 *   src/include/utils/inet.h — inet_struct/inet + ip_* macros +
 *       SET_INET_VARSIZE (verbatim)
 *   src/include/access/spgist.h — spgConfigOut, spgChooseIn/Out,
 *       spgInnerConsistentIn/Out, spgLeafConsistentIn/Out (verbatim shapes;
 *       fields the vendored code never touches ride as opaque void
 *       pointers)
 *   src/include/access/stratnum.h — RT*StrategyNumber values (verbatim)
 *
 * varatt.h is vendored VERBATIM alongside as varatt_rel18.h (LE arm).
 *
 * SHIMS (everything else is verbatim):
 *  - fmgr unwrapping: PG_FUNCTION_ARGS -> plain (in, out) struct-pointer
 *    parameters; PG_RETURN_VOID -> int 0; PG_RETURN_BOOL -> int 0/1.
 *  - Datum = uintptr_t; DatumGetPointer/PointerGetDatum casts;
 *    DatumGetInetPP(X) -> ((inet *) DatumGetPointer(X)): the harness passes
 *    inline 4B-header images only, for which PG_DETOAST_DATUM_PACKED is the
 *    identity — mirroring the Rust side's PackedVarlena contract (inet never
 *    TOASTs external/compressed).
 *  - AF_INET pinned to 2 (POSIX value on all pgrust targets), giving
 *    PGSQL_AF_INET 2 / PGSQL_AF_INET6 3 exactly as the Rust constants.
 *  - ScanKeyData reduced to { sk_strategy, sk_argument }: the vendored code
 *    reads exactly those two fields (the real struct's other fields,
 *    including a 48-byte FmgrInfo, never enter these functions).
 *  - palloc/palloc0: individually-NAMED static slots (jsonb-cmp round-2
 *    field-sensitivity law): pg_choose_prefix_slot for
 *    cidr_set_masklen_internal's palloc0(sizeof(inet)) (zeroed at entry —
 *    palloc0's zeroing is load-bearing for the VARDATA_ANY construction
 *    trick, see inet.h comment), pg_inner_nodenumbers_slot for
 *    inner_consistent's palloc(sizeof(int) * nNodes).
 *  - memcmp: <string.h> builtin (CBMC model), operand length <= 16 here.
 * No logic is shimmed: every strategy switch arm, bit test, mask expression
 * and loop is byte-for-byte the postgres body.
 */

#include "../../support/c/pg_proof_shim.h"

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
struct varlena
{
	char		vl_len_[4];
	char		vl_dat[4];		/* PROOF: fixed stand-in, never indexed here */
};

#include "varatt_rel18.h"

#include <string.h>

typedef uintptr_t Datum;
#define DatumGetPointer(X) ((void *) (X))
#define PointerGetDatum(X) ((Datum) (X))

/* ---- inet.h, verbatim ---- */

#define AF_INET 2				/* PROOF SHIM: POSIX value, all pgrust targets */
#define PGSQL_AF_INET	(AF_INET + 0)
#define PGSQL_AF_INET6	(AF_INET + 1)

typedef struct
{
	unsigned char family;		/* PGSQL_AF_INET or PGSQL_AF_INET6 */
	unsigned char bits;			/* number of bits in netmask */
	unsigned char ipaddr[16];	/* up to 128 bits of address */
} inet_struct;

typedef struct
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	inet_struct inet_data;
} inet;

#define ip_family(inetptr) \
	(((inet_struct *) VARDATA_ANY(inetptr))->family)

#define ip_bits(inetptr) \
	(((inet_struct *) VARDATA_ANY(inetptr))->bits)

#define ip_addr(inetptr) \
	(((inet_struct *) VARDATA_ANY(inetptr))->ipaddr)

#define ip_addrsize(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 4 : 16)

#define ip_maxbits(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 32 : 128)

#define SET_INET_VARSIZE(dst) \
	SET_VARSIZE(dst, VARHDRSZ + offsetof(inet_struct, ipaddr) + \
				ip_addrsize(dst))

#define DatumGetInetPP(X)	((inet *) DatumGetPointer(X))	/* PROOF SHIM */
#define InetPGetDatum(X)	PointerGetDatum(X)

/* IS_HIGHBIT_SET (c.h), verbatim */
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/* ---- stratnum.h, verbatim values ---- */
typedef uint16 StrategyNumber;
#define RTEqualStrategyNumber			18
#define RTNotEqualStrategyNumber		19
#define RTLessStrategyNumber			20
#define RTLessEqualStrategyNumber		21
#define RTGreaterStrategyNumber			22
#define RTGreaterEqualStrategyNumber	23
#define RTSubStrategyNumber				24
#define RTSubEqualStrategyNumber		25
#define RTSuperStrategyNumber			26
#define RTSuperEqualStrategyNumber		27

/* ---- ScanKeyData, reduced (see header) ---- */
typedef struct ScanKeyData
{
	StrategyNumber sk_strategy;
	Datum		sk_argument;
} ScanKeyData;
typedef ScanKeyData *ScanKey;

/* ---- spgist.h structs, verbatim shapes (untouched fields opaque) ---- */

typedef struct spgConfigOut
{
	Oid			prefixType;
	Oid			labelType;
	Oid			leafType;
	bool		canReturnData;
	bool		longValuesOK;
} spgConfigOut;

typedef struct spgChooseIn
{
	Datum		datum;
	Datum		leafDatum;
	int			level;

	bool		allTheSame;
	bool		hasPrefix;
	Datum		prefixDatum;
	int			nNodes;
	Datum	   *nodeLabels;
} spgChooseIn;

typedef enum spgChooseResultType
{
	spgMatchNode = 1,
	spgAddNode,
	spgSplitTuple,
} spgChooseResultType;

typedef struct spgChooseOut
{
	spgChooseResultType resultType;
	union
	{
		struct
		{
			int			nodeN;
			int			levelAdd;
			Datum		restDatum;
		}			matchNode;
		struct
		{
			Datum		nodeLabel;
			int			nodeN;
		}			addNode;
		struct
		{
			bool		prefixHasPrefix;
			Datum		prefixPrefixDatum;
			int			prefixNNodes;
			Datum	   *prefixNodeLabels;
			int			childNodeN;

			bool		postfixHasPrefix;
			Datum		postfixPrefixDatum;
		}			splitTuple;
	}			result;
} spgChooseOut;

typedef struct spgInnerConsistentIn
{
	ScanKey		scankeys;
	ScanKey		orderbys;
	int			nkeys;
	int			norderbys;

	Datum		reconstructedValue;
	void	   *traversalValue;
	void	   *traversalMemoryContext; /* MemoryContext, untouched here */
	int			level;
	bool		returnData;

	bool		allTheSame;
	bool		hasPrefix;
	Datum		prefixDatum;
	int			nNodes;
	Datum	   *nodeLabels;
} spgInnerConsistentIn;

typedef struct spgInnerConsistentOut
{
	int			nNodes;
	int		   *nodeNumbers;
	int		   *levelAdds;
	Datum	   *reconstructedValues;
	void	  **traversalValues;
	double	  **distances;
} spgInnerConsistentOut;

typedef struct spgLeafConsistentIn
{
	ScanKey		scankeys;
	ScanKey		orderbys;
	int			nkeys;
	int			norderbys;

	Datum		reconstructedValue;
	void	   *traversalValue;
	int			level;
	bool		returnData;

	Datum		leafDatum;
} spgLeafConsistentIn;

typedef struct spgLeafConsistentOut
{
	Datum		leafValue;
	bool		recheck;
	bool		recheckDistances;
	double	   *distances;
} spgLeafConsistentOut;

/* ======================================================================
 * network.c dependencies, verbatim
 * ====================================================================== */

/* PROOF SHIM: palloc0(sizeof(inet)) -> named static slot, zeroed at the
 * single call site's entry (palloc0 semantics; zeroing is load-bearing,
 * see inet.h VARDATA_ANY construction comment). */
static inet pg_choose_prefix_slot;

/*
 * Copy src and set mask length to 'bits' (which must be valid for the family)
 */
static inet *
cidr_set_masklen_internal(const inet *src, int bits)
{
	inet	   *dst = &pg_choose_prefix_slot;	/* PROOF SHIM: palloc0 */

	memset(dst, 0, sizeof(inet));				/* PROOF SHIM: palloc0 zeroing */

	ip_family(dst) = ip_family(src);
	ip_bits(dst) = bits;

	if (bits > 0)
	{
		Assert(bits <= ip_maxbits(dst));

		/* Clone appropriate bytes of the address, leaving the rest 0 */
		memcpy(ip_addr(dst), ip_addr(src), (bits + 7) / 8);

		/* Clear any unwanted bits in the last partial byte */
		if (bits % 8)
			ip_addr(dst)[bits / 8] &= ~(0xFF >> (bits % 8));
	}

	/* Set varlena header correctly */
	SET_INET_VARSIZE(dst);

	return dst;
}

int
bitncmp(const unsigned char *l, const unsigned char *r, int n)
{
	unsigned int lb,
				rb;
	int			x,
				b;

	b = n / 8;
	x = memcmp(l, r, b);
	if (x || (n % 8) == 0)
		return x;

	lb = l[b];
	rb = r[b];
	for (b = n % 8; b > 0; b--)
	{
		if (IS_HIGHBIT_SET(lb) != IS_HIGHBIT_SET(rb))
		{
			if (IS_HIGHBIT_SET(lb))
				return 1;
			return -1;
		}
		lb <<= 1;
		rb <<= 1;
	}
	return 0;
}

int
bitncommon(const unsigned char *l, const unsigned char *r, int n)
{
	int			byte,
				nbits;

	/* number of bits to examine in last byte */
	nbits = n % 8;

	/* check whole bytes */
	for (byte = 0; byte < n / 8; byte++)
	{
		if (l[byte] != r[byte])
		{
			/* at least one bit in the last byte is not common */
			nbits = 7;
			break;
		}
	}

	/* check bits in last partial byte */
	if (nbits != 0)
	{
		/* calculate diff of first non-matching bytes */
		unsigned int diff = l[byte] ^ r[byte];

		/* compare the bits from the most to the least */
		while ((diff >> (8 - nbits)) != 0)
			nbits--;
	}

	return (8 * byte) + nbits;
}

/* ======================================================================
 * network_spgist.c, verbatim
 * ====================================================================== */

#define CIDROID 650				/* catalog/pg_type_d.h value, as Rust */
#define VOIDOID 2278

static int	inet_spg_node_number(const inet *val, int commonbits);
static int	inet_spg_consistent_bitmap(const inet *prefix, int nkeys,
									   ScanKey scankeys, bool leaf);

int
pg_inet_spg_config(spgConfigOut *cfg)
{
	/* spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0); */

	cfg->prefixType = CIDROID;
	cfg->labelType = VOIDOID;
	cfg->canReturnData = true;
	cfg->longValuesOK = false;

	return 0;					/* PG_RETURN_VOID() */
}

int
pg_inet_spg_choose(const spgChooseIn *in, spgChooseOut *out)
{
	inet	   *val = DatumGetInetPP(in->datum),
			   *prefix;
	int			commonbits;

	if (!in->hasPrefix)
	{
		Assert(!in->allTheSame);
		Assert(in->nNodes == 2);

		out->resultType = spgMatchNode;
		out->result.matchNode.nodeN = (ip_family(val) == PGSQL_AF_INET) ? 0 : 1;
		out->result.matchNode.restDatum = InetPGetDatum(val);

		return 0;				/* PG_RETURN_VOID() */
	}

	Assert(in->nNodes == 4 || in->allTheSame);

	prefix = DatumGetInetPP(in->prefixDatum);
	commonbits = ip_bits(prefix);

	if (ip_family(val) != ip_family(prefix))
	{
		out->resultType = spgSplitTuple;
		out->result.splitTuple.prefixHasPrefix = false;
		out->result.splitTuple.prefixNNodes = 2;
		out->result.splitTuple.prefixNodeLabels = NULL;

		out->result.splitTuple.childNodeN =
			(ip_family(prefix) == PGSQL_AF_INET) ? 0 : 1;

		out->result.splitTuple.postfixHasPrefix = true;
		out->result.splitTuple.postfixPrefixDatum = InetPGetDatum(prefix);

		return 0;				/* PG_RETURN_VOID() */
	}

	if (ip_bits(val) < commonbits ||
		bitncmp(ip_addr(prefix), ip_addr(val), commonbits) != 0)
	{
		commonbits = bitncommon(ip_addr(prefix), ip_addr(val),
								Min(ip_bits(val), commonbits));

		out->resultType = spgSplitTuple;
		out->result.splitTuple.prefixHasPrefix = true;
		out->result.splitTuple.prefixPrefixDatum =
			InetPGetDatum(cidr_set_masklen_internal(val, commonbits));
		out->result.splitTuple.prefixNNodes = 4;
		out->result.splitTuple.prefixNodeLabels = NULL;

		out->result.splitTuple.childNodeN =
			inet_spg_node_number(prefix, commonbits);

		out->result.splitTuple.postfixHasPrefix = true;
		out->result.splitTuple.postfixPrefixDatum = InetPGetDatum(prefix);

		return 0;				/* PG_RETURN_VOID() */
	}

	out->resultType = spgMatchNode;
	out->result.matchNode.nodeN = inet_spg_node_number(val, commonbits);
	out->result.matchNode.restDatum = InetPGetDatum(val);

	return 0;					/* PG_RETURN_VOID() */
}

/* PROOF SHIM: palloc(sizeof(int) * in->nNodes) -> named static slot;
 * nNodes <= 4 for this opclass (2-node family split / 4-node prefix). */
static int	pg_inner_nodenumbers_slot[32];

int
pg_inet_spg_inner_consistent(const spgInnerConsistentIn *in,
							 spgInnerConsistentOut *out)
{
	int			i;
	int			which;

	if (!in->hasPrefix)
	{
		Assert(!in->allTheSame);
		Assert(in->nNodes == 2);

		which = 1 | (1 << 1);

		for (i = 0; i < in->nkeys; i++)
		{
			StrategyNumber strategy = in->scankeys[i].sk_strategy;
			inet	   *argument = DatumGetInetPP(in->scankeys[i].sk_argument);

			switch (strategy)
			{
				case RTLessStrategyNumber:
				case RTLessEqualStrategyNumber:
					if (ip_family(argument) == PGSQL_AF_INET)
						which &= 1;
					break;

				case RTGreaterEqualStrategyNumber:
				case RTGreaterStrategyNumber:
					if (ip_family(argument) == PGSQL_AF_INET6)
						which &= (1 << 1);
					break;

				case RTNotEqualStrategyNumber:
					break;

				default:
					/* all other ops can only match addrs of same family */
					if (ip_family(argument) == PGSQL_AF_INET)
						which &= 1;
					else
						which &= (1 << 1);
					break;
			}
		}
	}
	else if (!in->allTheSame)
	{
		Assert(in->nNodes == 4);

		which = inet_spg_consistent_bitmap(DatumGetInetPP(in->prefixDatum),
										   in->nkeys, in->scankeys, false);
	}
	else
	{
		/* Must visit all nodes; we assume there are less than 32 of 'em */
		which = ~0;
	}

	out->nNodes = 0;

	if (which)
	{
		out->nodeNumbers = pg_inner_nodenumbers_slot;	/* PROOF SHIM: palloc */

		for (i = 0; i < in->nNodes; i++)
		{
			if (which & (1 << i))
			{
				out->nodeNumbers[out->nNodes] = i;
				out->nNodes++;
			}
		}
	}

	return 0;					/* PG_RETURN_VOID() */
}

int
pg_inet_spg_leaf_consistent(const spgLeafConsistentIn *in,
							spgLeafConsistentOut *out)
{
	inet	   *leaf = DatumGetInetPP(in->leafDatum);

	/* All tests are exact. */
	out->recheck = false;

	/* Leaf is what it is... */
	out->leafValue = InetPGetDatum(leaf);

	/* Use common code to apply the tests. */
	return inet_spg_consistent_bitmap(leaf, in->nkeys, in->scankeys,
									  true) ? 1 : 0;	/* PG_RETURN_BOOL */
}

static int
inet_spg_node_number(const inet *val, int commonbits)
{
	int			nodeN = 0;

	if (commonbits < ip_maxbits(val) &&
		ip_addr(val)[commonbits / 8] & (1 << (7 - commonbits % 8)))
		nodeN |= 1;
	if (commonbits < ip_bits(val))
		nodeN |= 2;

	return nodeN;
}

static int
inet_spg_consistent_bitmap(const inet *prefix, int nkeys, ScanKey scankeys,
						   bool leaf)
{
	int			bitmap;
	int			commonbits,
				i;

	/* Initialize result to allow visiting all children */
	if (leaf)
		bitmap = 1;
	else
		bitmap = 1 | (1 << 1) | (1 << 2) | (1 << 3);

	commonbits = ip_bits(prefix);

	for (i = 0; i < nkeys; i++)
	{
		inet	   *argument = DatumGetInetPP(scankeys[i].sk_argument);
		StrategyNumber strategy = scankeys[i].sk_strategy;
		int			order;

		/*
		 * Check 0: different families
		 */
		if (ip_family(argument) != ip_family(prefix))
		{
			switch (strategy)
			{
				case RTLessStrategyNumber:
				case RTLessEqualStrategyNumber:
					if (ip_family(argument) < ip_family(prefix))
						bitmap = 0;
					break;

				case RTGreaterEqualStrategyNumber:
				case RTGreaterStrategyNumber:
					if (ip_family(argument) > ip_family(prefix))
						bitmap = 0;
					break;

				case RTNotEqualStrategyNumber:
					break;

				default:
					/* For all other cases, we can be sure there is no match */
					bitmap = 0;
					break;
			}

			if (!bitmap)
				break;

			/* Other checks make no sense with different families. */
			continue;
		}

		/*
		 * Check 1: network bit count
		 */
		switch (strategy)
		{
			case RTSubStrategyNumber:
				if (commonbits <= ip_bits(argument))
					bitmap &= (1 << 2) | (1 << 3);
				break;

			case RTSubEqualStrategyNumber:
				if (commonbits < ip_bits(argument))
					bitmap &= (1 << 2) | (1 << 3);
				break;

			case RTSuperStrategyNumber:
				if (commonbits == ip_bits(argument) - 1)
					bitmap &= 1 | (1 << 1);
				else if (commonbits >= ip_bits(argument))
					bitmap = 0;
				break;

			case RTSuperEqualStrategyNumber:
				if (commonbits == ip_bits(argument))
					bitmap &= 1 | (1 << 1);
				else if (commonbits > ip_bits(argument))
					bitmap = 0;
				break;

			case RTEqualStrategyNumber:
				if (commonbits < ip_bits(argument))
					bitmap &= (1 << 2) | (1 << 3);
				else if (commonbits == ip_bits(argument))
					bitmap &= 1 | (1 << 1);
				else
					bitmap = 0;
				break;
		}

		if (!bitmap)
			break;

		/*
		 * Check 2: common network bits
		 */
		order = bitncmp(ip_addr(prefix), ip_addr(argument),
						Min(commonbits, ip_bits(argument)));

		if (order != 0)
		{
			switch (strategy)
			{
				case RTLessStrategyNumber:
				case RTLessEqualStrategyNumber:
					if (order > 0)
						bitmap = 0;
					break;

				case RTGreaterEqualStrategyNumber:
				case RTGreaterStrategyNumber:
					if (order < 0)
						bitmap = 0;
					break;

				case RTNotEqualStrategyNumber:
					break;

				default:
					/* For all other cases, we can be sure there is no match */
					bitmap = 0;
					break;
			}

			if (!bitmap)
				break;

			/*
			 * Remaining checks make no sense when common bits don't match.
			 */
			continue;
		}

		/*
		 * Check 3: next network bit
		 */
		if (bitmap & ((1 << 2) | (1 << 3)) &&
			commonbits < ip_bits(argument))
		{
			int			nextbit;

			nextbit = ip_addr(argument)[commonbits / 8] &
				(1 << (7 - commonbits % 8));

			switch (strategy)
			{
				case RTLessStrategyNumber:
				case RTLessEqualStrategyNumber:
					if (!nextbit)
						bitmap &= 1 | (1 << 1) | (1 << 2);
					break;

				case RTGreaterEqualStrategyNumber:
				case RTGreaterStrategyNumber:
					if (nextbit)
						bitmap &= 1 | (1 << 1) | (1 << 3);
					break;

				case RTNotEqualStrategyNumber:
					break;

				default:
					if (!nextbit)
						bitmap &= 1 | (1 << 1) | (1 << 2);
					else
						bitmap &= 1 | (1 << 1) | (1 << 3);
					break;
			}

			if (!bitmap)
				break;
		}

		/*
		 * Remaining checks are only for the basic comparison strategies.
		 */
		if (strategy < RTEqualStrategyNumber ||
			strategy > RTGreaterEqualStrategyNumber)
			continue;

		/*
		 * Check 4: network bit count
		 */
		switch (strategy)
		{
			case RTLessStrategyNumber:
			case RTLessEqualStrategyNumber:
				if (commonbits == ip_bits(argument))
					bitmap &= 1 | (1 << 1);
				else if (commonbits > ip_bits(argument))
					bitmap = 0;
				break;

			case RTGreaterEqualStrategyNumber:
			case RTGreaterStrategyNumber:
				if (commonbits < ip_bits(argument))
					bitmap &= (1 << 2) | (1 << 3);
				break;
		}

		if (!bitmap)
			break;

		/* Remaining checks don't make sense with different ip_bits. */
		if (commonbits != ip_bits(argument))
			continue;

		/*
		 * Check 5: next host bit
		 */
		if (!leaf && bitmap & (1 | (1 << 1)) &&
			commonbits < ip_maxbits(argument))
		{
			int			nextbit;

			nextbit = ip_addr(argument)[commonbits / 8] &
				(1 << (7 - commonbits % 8));

			switch (strategy)
			{
				case RTLessStrategyNumber:
				case RTLessEqualStrategyNumber:
					if (!nextbit)
						bitmap &= 1 | (1 << 2) | (1 << 3);
					break;

				case RTGreaterEqualStrategyNumber:
				case RTGreaterStrategyNumber:
					if (nextbit)
						bitmap &= (1 << 1) | (1 << 2) | (1 << 3);
					break;

				case RTNotEqualStrategyNumber:
					break;

				default:
					if (!nextbit)
						bitmap &= 1 | (1 << 2) | (1 << 3);
					else
						bitmap &= (1 << 1) | (1 << 2) | (1 << 3);
					break;
			}

			if (!bitmap)
				break;
		}

		/*
		 * Check 6: whole address
		 */
		if (leaf)
		{
			/* Redo ordering comparison using all address bits */
			order = bitncmp(ip_addr(prefix), ip_addr(argument),
							ip_maxbits(prefix));

			switch (strategy)
			{
				case RTLessStrategyNumber:
					if (order >= 0)
						bitmap = 0;
					break;

				case RTLessEqualStrategyNumber:
					if (order > 0)
						bitmap = 0;
					break;

				case RTEqualStrategyNumber:
					if (order != 0)
						bitmap = 0;
					break;

				case RTGreaterEqualStrategyNumber:
					if (order < 0)
						bitmap = 0;
					break;

				case RTGreaterStrategyNumber:
					if (order <= 0)
						bitmap = 0;
					break;

				case RTNotEqualStrategyNumber:
					if (order == 0)
						bitmap = 0;
					break;
			}

			if (!bitmap)
				break;
		}
	}

	return bitmap;
}
