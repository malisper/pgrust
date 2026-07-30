/*
 * Vendored PostgreSQL C for the state-seam-probe proof family — ACL
 * permission-bit logic.
 *
 * Provenance:
 *   - src/backend/utils/adt/acl.c   (aclmask, aclmask_direct, aclitem_eq,
 *                                    hash_aclitem[_extended], aclcontains,
 *                                    makeaclitem's bit assembly)
 *   - src/include/utils/acl.h       (AclItem, ACLITEM_* macros, verbatim)
 *   - src/common/hashfn.c           (hash_bytes_uint32_extended + mix/final,
 *                                    for hash_aclitem_extended)
 *   ref: postgres/postgres REL_18_STABLE
 *   fetched: 2026-07-28
 *
 * STATE-SEAM: has_privs_of_role() is a catalog/role-membership read (syscache
 * walk over pg_auth_members). Within one aclmask() call its first argument is
 * always the fixed `roleid`, so the reachable seam surface is a boolean
 * function of the second argument (the role being tested). It is modeled by
 * a first-match oracle table (pg_oracle_role[i] -> pg_oracle_ans[i], else
 * pg_oracle_default) whose contents the harness sets to universally
 * quantified symbolic values, IDENTICALLY on the Rust side (kani::stub of
 * adt_acl's has_privs_of_role reading the same table). The table indexes the
 * exact set of roles aclmask can query (ownerId + every grantee), so the
 * proof quantifies over every boolean membership assignment on that set;
 * only the seam INTERNALS (catalog walk, superuser check, caching) are
 * outside the proof. A skew control (different answers on the two sides)
 * must fail, witnessing the model is load-bearing.
 *
 * Function-local shims (plumbing only, never logic):
 *   - `pg_` prefix on every function.
 *   - Acl* varlena container -> (const AclItem *aidat, int num): ACL_NUM()/
 *     ACL_DAT() header unwrapping done by the caller-side signature. The
 *     `if (acl == NULL) elog(ERROR)` guard and check_acl() (varlena array
 *     shape validation) are dropped with the container — the harness always
 *     passes a well-formed item array, which is exactly the state check_acl
 *     certifies.
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures
 *     (PG_GETARG_ACLITEM_P -> const AclItem *, PG_RETURN_BOOL/UINT32 ->
 *     int/uint32 returns).
 *   - makeaclitem: text privilege parsing (convert_any_priv_string) and
 *     palloc stay OUT of the extracted core; pg_makeaclitem_bits takes the
 *     already-parsed AclMode `priv` and performs the verbatim field/
 *     ACLITEM_SET_PRIVS_GOPTIONS assembly into a caller-provided AclItem.
 *   - hash_uint32_extended: vendored verbatim as hash_bytes_uint32_extended
 *     from src/common/hashfn.c (acl.c's hash_uint32_extended is the fmgr
 *     alias of it), with rot() = the standard rotate expression.
 *
 * Bodies are otherwise verbatim, comments included.
 */

#include "../../support/c/pg_proof_shim.h"

typedef uint64 AclMode;

typedef struct AclItem
{
	Oid			ai_grantee;		/* ID that this item grants privs to */
	Oid			ai_grantor;		/* grantor of privs */
	AclMode		ai_privs;		/* privilege bits */
} AclItem;

/* acl.h, verbatim */
#define ACLITEM_GET_PRIVS(item)    ((item).ai_privs & 0xFFFFFFFF)
#define ACLITEM_GET_GOPTIONS(item) (((item).ai_privs >> 32) & 0xFFFFFFFF)
#define ACLITEM_GET_RIGHTS(item)   ((item).ai_privs)

#define ACLITEM_SET_PRIVS_GOPTIONS(item,privs,goptions) \
  ((item).ai_privs = ((AclMode) (privs) & 0xFFFFFFFF) | \
					 (((AclMode) (goptions) & 0xFFFFFFFF) << 32))

#define ACLITEM_ALL_PRIV_BITS		((AclMode) 0xFFFFFFFF)
#define ACLITEM_ALL_GOPTION_BITS	((AclMode) 0xFFFFFFFF << 32)

#define ACL_ID_PUBLIC	0
#define ACL_NO_RIGHTS	0

typedef enum
{
	ACLMASK_ALL,				/* check for all specified privileges */
	ACLMASK_ANY					/* check for any specified privilege */
} AclMaskHow;

/* ---------------------------------------------------------------------
 * membership oracle: THE STATE SEAM (see file header)
 * --------------------------------------------------------------------- */
#define PG_ORACLE_N 6

Oid			pg_oracle_role[PG_ORACLE_N];
int			pg_oracle_ans[PG_ORACLE_N];
int			pg_oracle_default;

static bool
has_privs_of_role(Oid member, Oid role)
{
	int			i;

	(void) member;				/* always == the aclmask roleid; the model is
								 * the member-fixed restriction of the seam */
	for (i = 0; i < PG_ORACLE_N; i++)
		if (pg_oracle_role[i] == role)
			return pg_oracle_ans[i] != 0;
	return pg_oracle_default != 0;
}

/* ---------------------------------------------------------------------
 * aclmask (acl.c) — body verbatim modulo the container shim noted above
 * --------------------------------------------------------------------- */
AclMode
pg_aclmask(const AclItem *aidat_in, int num, Oid roleid, Oid ownerId,
		   AclMode mask, int how)
{
	AclMode		result;
	AclMode		remaining;
	const AclItem *aidat;
	int			i;

	/* Quick exit for mask == 0 */
	if (mask == 0)
		return 0;

	result = 0;

	/* Owner always implicitly has all grant options */
	if ((mask & ACLITEM_ALL_GOPTION_BITS) &&
		has_privs_of_role(roleid, ownerId))
	{
		result = mask & ACLITEM_ALL_GOPTION_BITS;
		if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
			return result;
	}

	aidat = aidat_in;			/* shim: num/aidat from params, not
								 * ACL_NUM()/ACL_DAT() */

	/*
	 * Check privileges granted directly to roleid or to public
	 */
	for (i = 0; i < num; i++)
	{
		const AclItem *aidata = &aidat[i];

		if (aidata->ai_grantee == ACL_ID_PUBLIC ||
			aidata->ai_grantee == roleid)
		{
			result |= aidata->ai_privs & mask;
			if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
				return result;
		}
	}

	/*
	 * Check privileges granted indirectly via role memberships. We do this in
	 * a separate pass to minimize expensive indirect membership tests.  In
	 * particular, it's worth testing whether a given ACL entry grants any
	 * privileges still of interest before we perform the has_privs_of_role
	 * test.
	 */
	remaining = mask & ~result;
	for (i = 0; i < num; i++)
	{
		const AclItem *aidata = &aidat[i];

		if (aidata->ai_grantee == ACL_ID_PUBLIC ||
			aidata->ai_grantee == roleid)
			continue;			/* already checked it */

		if ((aidata->ai_privs & remaining) &&
			has_privs_of_role(roleid, aidata->ai_grantee))
		{
			result |= aidata->ai_privs & mask;
			if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
				return result;
			remaining = mask & ~result;
		}
	}

	return result;
}

/* ---------------------------------------------------------------------
 * aclmask_direct (acl.c) — pure; no membership seam at all
 * --------------------------------------------------------------------- */
AclMode
pg_aclmask_direct(const AclItem *aidat_in, int num, Oid roleid, Oid ownerId,
				  AclMode mask, int how)
{
	AclMode		result;
	const AclItem *aidat;
	int			i;

	/* Quick exit for mask == 0 */
	if (mask == 0)
		return 0;

	result = 0;

	/* Owner always implicitly has all grant options */
	if ((mask & ACLITEM_ALL_GOPTION_BITS) &&
		roleid == ownerId)
	{
		result = mask & ACLITEM_ALL_GOPTION_BITS;
		if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
			return result;
	}

	aidat = aidat_in;			/* shim: num/aidat from params */

	/*
	 * Check privileges granted directly to roleid (and not to public)
	 */
	for (i = 0; i < num; i++)
	{
		const AclItem *aidata = &aidat[i];

		if (aidata->ai_grantee == roleid)
		{
			result |= aidata->ai_privs & mask;
			if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
				return result;
		}
	}

	return result;
}

/* ---------------------------------------------------------------------
 * aclitem_eq (acl.c)
 * --------------------------------------------------------------------- */
int
pg_aclitem_eq(const AclItem *a1, const AclItem *a2)
{
	bool		result;

	result = a1->ai_privs == a2->ai_privs &&
		a1->ai_grantee == a2->ai_grantee &&
		a1->ai_grantor == a2->ai_grantor;
	return result;
}

/* ---------------------------------------------------------------------
 * hash_aclitem / hash_aclitem_extended (acl.c)
 * --------------------------------------------------------------------- */
uint32
pg_hash_aclitem(const AclItem *a)
{
	/* not very bright, but avoids any issue of padding in struct */
	return (uint32) (a->ai_privs + a->ai_grantee + a->ai_grantor);
}

/* src/common/hashfn.c, verbatim: rot / mix / final / hash_bytes_uint32_extended */
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}

#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}

static uint64
hash_bytes_uint32_extended(uint32 k, uint64 seed)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;

	if (seed != 0)
	{
		a += (uint32) (seed >> 32);
		b += (uint32) seed;
		mix(a, b, c);
	}

	a += k;

	final(a, b, c);

	/* report the result */
	return ((uint64) b << 32) | c;
}

uint64
pg_hash_aclitem_extended(const AclItem *a, uint64 seed)
{
	uint32		sum = (uint32) (a->ai_privs + a->ai_grantee + a->ai_grantor);

	return (seed == 0) ? (uint64) sum : hash_bytes_uint32_extended(sum, seed);
}

/* ---------------------------------------------------------------------
 * aclcontains (acl.c) — container shim as in pg_aclmask
 * --------------------------------------------------------------------- */
int
pg_aclcontains(const AclItem *aidat, int num, const AclItem *aip)
{
	int			i;

	for (i = 0; i < num; ++i)
	{
		if (aip->ai_grantee == aidat[i].ai_grantee &&
			aip->ai_grantor == aidat[i].ai_grantor &&
			(ACLITEM_GET_RIGHTS(*aip) & ACLITEM_GET_RIGHTS(aidat[i])) == ACLITEM_GET_RIGHTS(*aip))
			return 1;
	}
	return 0;
}

/* ---------------------------------------------------------------------
 * makeaclitem (acl.c) — bit assembly only; text parsing + palloc are the
 * shimmed plumbing (see file header)
 * --------------------------------------------------------------------- */
int							/* int-return shim: Kani lowers Rust () as a
								 * struct goto-cc rejects against C void */
pg_makeaclitem_bits(Oid grantee, Oid grantor, AclMode priv, int goption,
					AclItem *result)
{
	result->ai_grantee = grantee;
	result->ai_grantor = grantor;

	ACLITEM_SET_PRIVS_GOPTIONS(*result, priv,
							   (goption ? priv : ACL_NO_RIGHTS));
	return 0;
}
