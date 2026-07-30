/*
 * Vendored PostgreSQL C for the aclcheck proof family — the SQL-visible ACL
 * privilege-inquiry layer (has_*_privilege family, aclitemin/out, aclinsert/
 * aclremove, acldefault_sql) and the object-aclcheck layer under it.
 *
 * Provenance:
 *   - src/backend/utils/adt/acl.c    (getid, putid, aclparse, aclitemin,
 *                                     aclitemout, aclinsert, aclremove,
 *                                     acldefault, acldefault_sql, aclmask,
 *                                     convert_any_priv_string and the
 *                                     per-class convert_*_priv_string maps,
 *                                     the has_table/sequence/database/
 *                                     function/language/schema_privilege_*
 *                                     cores, get_role_oid,
 *                                     get_role_oid_or_public)
 *   - src/backend/catalog/aclchk.c   (pg_class_aclmask_ext,
 *                                     pg_class_aclcheck[_ext],
 *                                     object_aclmask[_ext],
 *                                     object_aclcheck[_ext],
 *                                     pg_namespace_aclmask_ext)
 *   - src/include/utils/acl.h        (AclItem, ACL_* bits and chars,
 *                                     ACLITEM_* / ACL_GRANT_OPTION_FOR
 *                                     macros, ACL_ALL_RIGHTS_*, verbatim)
 *   - src/backend/catalog/catalog.c  (IsSystemClass, IsCatalogRelationOid,
 *                                     IsToastNamespace — reduced, see shims)
 *   - src/port/pgstrcasecmp.c        (pg_strcasecmp, verbatim)
 *   ref: postgres/postgres REL_18_STABLE
 *   fetched: 2026-07-28
 *
 * STATE SEAMS (each mirrored bit-for-bit by a kani::stub on the Rust side;
 * the harness arms both sides with the SAME universally-quantified symbolic
 * values; skew controls prove each seam is load-bearing):
 *
 *   1. membership oracle  — has_privs_of_role(member, role): first-match
 *      table pgq_memb_role[] -> pgq_memb_ans[], else pgq_memb_default.
 *      `member` is ignored: within one statement-level check it is the one
 *      fixed roleid, so the reachable seam surface is a boolean function of
 *      the queried role (state-seam-probe precedent).
 *   2. superuser oracle   — superuser_arg(roleid): same first-match shape
 *      over pgq_super_role[]/pgq_super_ans[], else pgq_super_default.
 *      Modeled independently of the membership oracle: quantifying over
 *      unconstrained (superuser, membership) pairs covers a superset of the
 *      reachable assignments, which is sound for equivalence.
 *   3. catalog-tuple seam — the single object row a harness call can read
 *      (SearchSysCache1 + SysCacheGetAttr[NotNull] on both sides):
 *      pgq_cat_found, pgq_cat_owner, pgq_cat_relkind, pgq_cat_relnamespace,
 *      pgq_cat_acl_isnull, pgq_cat_nacl, pgq_cat_acl[].  Exactly one object
 *      is inspected per has_*_privilege call, so one row model is total.
 *   4. role-name oracle   — get_role_oid's syscache lookup: answers by CALL
 *      INDEX (pgq_role_found[i]/pgq_role_oid[i] for the i-th lookup in the
 *      call; at most 2 lookups reachable: aclitemin grantee+grantor).
 *      Constant-per-call-index is total because both sides' lookup sequence
 *      is itself part of the proven behavior.
 *   5. object-name oracle — convert_table_name / get_database_oid /
 *      get_language_oid / get_namespace_oid: pgq_objname_oid, modeled TOTAL
 *      (always found).  Name->oid failure precedence is out of proof.
 *   6. current user       — GetUserId(): pgq_current_user.
 *   7. role-NAME-by-oid oracle (aclitemout): sequential one-deep register:
 *      SearchSysCache1(AUTHOID, oid) consults the 2-slot table
 *      pgq_rname_oid[]/pgq_rname_found[]/pgq_rname_name[][] and latches the
 *      hit for the immediately following rolname attribute read.
 *   8. pgq_temp_toast / pgq_is_temp_namespace — isTempToastNamespace /
 *      isTempNamespace backend-session state.  Harnesses PIN both to 0
 *      (fence: non-temp namespaces).  NOTE: the Rust port of IsSystemClass
 *      has NO temp-toast arm at all (aclchk/src/lib.rs pg_class_aclmask_ext
 *      hardwires relnamespace == PG_TOAST_NAMESPACE || oid <
 *      FirstUnpinnedObjectId); widening pgq_temp_toast to symbolic is a
 *      known-divergence probe, kept as an expected-fail witness harness.
 *
 * Function-local shims (plumbing only, never logic):
 *   - `pg_` prefix on every exported function; `pgq_` prefix on seam state.
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures; PG_RETURN_NULL
 *     -> *isnull = 1; PG_RETURN_BOOL -> int return.
 *   - text* arguments -> caller-provided MODIFIABLE char* (text_to_cstring
 *     done by the harness); the object-name text args are consumed by the
 *     object-name oracle and do not cross into C at all.
 *   - ereport(ERROR)/ereturn -> PROOF_EREPORT_FLAG variant: *err is set to a
 *     DISTINCT code per errcode (PGQ_ERR_* below) at the exact ereport
 *     program point, then early-return; the harness asserts code parity
 *     against the Rust Err sqlstate.  ereport(WARNING) (aclparse grantor
 *     defaulting) -> no-op (WARNING emission is out of proof on both sides).
 *   - palloc/pfree of Acl -> static struct; the Acl varlena container is a
 *     plain {num, items[]} struct with ACL_NUM/ACL_DAT mapped onto it, so
 *     aclmask/acldefault bodies stay verbatim.  check_acl() is dropped with
 *     the container (harness-constructed ACLs are well-formed by
 *     construction, which is exactly what check_acl certifies); the Rust
 *     side's shipped decoder (check_acl_payload + read_acl_item) stays
 *     IN-theorem via the catalog-seam varlena image.
 *   - strlen/strchr/strcmp -> local static loops (Kani has no libc model);
 *     isspace/isalpha/isalnum/isupper/tolower -> pg_proof_* C-locale
 *     helpers (shim header + local additions below).
 *   - sprintf(p, "%u", oid) in aclitemout's role-not-found arm ->
 *     pgq_sprintf_u32 (standard decimal rendering of a uint32; this arm's
 *     formatting is a shim MODEL of sprintf, documented in the harness).
 *   - get_object_catcache_oid / get_object_attnum_owner / _acl /
 *     get_object_type (objectaddress.c table lookups) collapse into the
 *     catalog-tuple seam + a classid->ObjectType switch that mirrors the
 *     ObjectProperty rows for the reachable classids.
 *   - Assert compiled out (production parity, shim header).
 *
 * Bodies are otherwise verbatim, comments included.
 */

#include "../../support/c/pg_proof_shim.h"

/* ---- local C-locale ctype additions (same contract as the header's) ---- */
static int pg_proof_isalpha_(int c) {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}
static int pg_proof_isalnum_(int c) {
	return pg_proof_isalpha_(c) || (c >= '0' && c <= '9');
}
static int pg_proof_isupper_(int c) {
	return c >= 'A' && c <= 'Z';
}
static int pg_proof_isspace_(int c) {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}
static int pg_proof_tolower_(int c) {
	return pg_proof_isupper_(c) ? c - 'A' + 'a' : c;
}
#define isspace(c) pg_proof_isspace_(c)
#define isalpha(c) pg_proof_isalpha_(c)
#define isalnum(c) pg_proof_isalnum_(c)
#define isupper(c) pg_proof_isupper_(c)
#define tolower(c) pg_proof_tolower_(c)

/* ---- local libc string replacements (plumbing) ---- */
static size_t pgq_strlen(const char *s) {
	size_t n = 0;
	while (s[n] != '\0')
		n++;
	return n;
}
static char *pgq_strchr(char *s, int c) {
	for (;; s++) {
		if (*s == (char) c)
			return s;
		if (*s == '\0')
			return (char *) 0;
	}
}
static int pgq_strcmp(const char *a, const char *b) {
	while (*a != '\0' && *a == *b) {
		a++;
		b++;
	}
	return (unsigned char) *a - (unsigned char) *b;
}
#define strlen(s) pgq_strlen(s)
#define strchr(s, c) pgq_strchr((char *) (s), (c))
#define strcmp(a, b) pgq_strcmp((a), (b))

/* ---- acl.h / parsenodes.h / pg_class.h constants, verbatim values ---- */
typedef uint64 AclMode;
typedef int AclResult;

#define ACLCHECK_OK 0
#define ACLCHECK_NO_PRIV 1

#define ACL_INSERT		(1<<0)
#define ACL_SELECT		(1<<1)
#define ACL_UPDATE		(1<<2)
#define ACL_DELETE		(1<<3)
#define ACL_TRUNCATE	(1<<4)
#define ACL_REFERENCES	(1<<5)
#define ACL_TRIGGER		(1<<6)
#define ACL_EXECUTE		(1<<7)
#define ACL_USAGE		(1<<8)
#define ACL_CREATE		(1<<9)
#define ACL_CREATE_TEMP (1<<10)
#define ACL_CONNECT		(1<<11)
#define ACL_SET			(1<<12)
#define ACL_ALTER_SYSTEM (1<<13)
#define ACL_MAINTAIN	(1<<14)
#define N_ACL_RIGHTS	15
#define ACL_NO_RIGHTS	0

#define ACL_INSERT_CHR		'a'
#define ACL_SELECT_CHR		'r'
#define ACL_UPDATE_CHR		'w'
#define ACL_DELETE_CHR		'd'
#define ACL_TRUNCATE_CHR	'D'
#define ACL_REFERENCES_CHR	'x'
#define ACL_TRIGGER_CHR		't'
#define ACL_EXECUTE_CHR		'X'
#define ACL_USAGE_CHR		'U'
#define ACL_CREATE_CHR		'C'
#define ACL_CREATE_TEMP_CHR 'T'
#define ACL_CONNECT_CHR		'c'
#define ACL_SET_CHR			's'
#define ACL_ALTER_SYSTEM_CHR 'A'
#define ACL_MAINTAIN_CHR	'm'
#define ACL_ALL_RIGHTS_STR	"arwdDxtXUCTcsAm"

#define ACL_ALL_RIGHTS_RELATION \
	(ACL_INSERT|ACL_SELECT|ACL_UPDATE|ACL_DELETE|ACL_TRUNCATE|ACL_REFERENCES|ACL_TRIGGER|ACL_MAINTAIN)
#define ACL_ALL_RIGHTS_SEQUENCE (ACL_USAGE|ACL_SELECT|ACL_UPDATE)
#define ACL_ALL_RIGHTS_DATABASE (ACL_CREATE|ACL_CREATE_TEMP|ACL_CONNECT)
#define ACL_ALL_RIGHTS_FDW		(ACL_USAGE)
#define ACL_ALL_RIGHTS_FOREIGN_SERVER (ACL_USAGE)
#define ACL_ALL_RIGHTS_FUNCTION (ACL_EXECUTE)
#define ACL_ALL_RIGHTS_LANGUAGE (ACL_USAGE)
#define ACL_ALL_RIGHTS_LARGEOBJECT (ACL_SELECT|ACL_UPDATE)
#define ACL_ALL_RIGHTS_PARAMETER_ACL (ACL_SET|ACL_ALTER_SYSTEM)
#define ACL_ALL_RIGHTS_SCHEMA	(ACL_USAGE|ACL_CREATE)
#define ACL_ALL_RIGHTS_TABLESPACE (ACL_CREATE)
#define ACL_ALL_RIGHTS_TYPE		(ACL_USAGE)

typedef struct AclItem
{
	Oid			ai_grantee;		/* ID that this item grants privs to */
	Oid			ai_grantor;		/* grantor of privs */
	AclMode		ai_privs;		/* privilege bits */
} AclItem;

#define ACLITEM_GET_PRIVS(item)    ((item).ai_privs & 0xFFFFFFFF)
#define ACLITEM_GET_GOPTIONS(item) (((item).ai_privs >> 32) & 0xFFFFFFFF)
#define ACLITEM_GET_RIGHTS(item)   ((item).ai_privs)

#define ACLITEM_SET_PRIVS_GOPTIONS(item,privs,goptions) \
  ((item).ai_privs = ((AclMode) (privs) & 0xFFFFFFFF) | \
					 (((AclMode) (goptions) & 0xFFFFFFFF) << 32))

#define ACL_GRANT_OPTION_FOR(privs) (((AclMode) (privs) & 0xFFFFFFFF) << 32)

#define ACLITEM_ALL_PRIV_BITS		((AclMode) 0xFFFFFFFF)
#define ACLITEM_ALL_GOPTION_BITS	((AclMode) 0xFFFFFFFF << 32)

#define ACL_ID_PUBLIC	0
#define InvalidOid		((Oid) 0)
#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))
#define NAMEDATALEN 64
#define BOOTSTRAP_SUPERUSERID 10

#define IS_HIGHBIT_SET(ch)	((unsigned char)(ch) & 0x80)

typedef enum
{
	ACLMASK_ALL,				/* check for all specified privileges */
	ACLMASK_ANY					/* check for any specified privilege */
} AclMaskHow;

/*
 * Acl container shim: {num, items[]} with ACL_NUM/ACL_DAT mapped onto it so
 * aclmask/acldefault bodies stay verbatim (see file header).
 */
#define PGQ_MAX_ACL 8
typedef struct Acl
{
	int			num;
	AclItem		items[PGQ_MAX_ACL];
} Acl;
#define ACL_NUM(ACL)  ((ACL)->num)
#define ACL_DAT(ACL)  ((ACL)->items)

/* parsenodes.h ObjectType — only the values acldefault switches on */
typedef enum ObjectType
{
	OBJECT_COLUMN = 1,
	OBJECT_TABLE,
	OBJECT_SEQUENCE,
	OBJECT_DATABASE,
	OBJECT_FUNCTION,
	OBJECT_LANGUAGE,
	OBJECT_LARGEOBJECT,
	OBJECT_SCHEMA,
	OBJECT_TABLESPACE,
	OBJECT_FDW,
	OBJECT_FOREIGN_SERVER,
	OBJECT_DOMAIN,
	OBJECT_TYPE,
	OBJECT_PARAMETER_ACL
} ObjectType;

/* pg_class.h relkinds used here */
#define RELKIND_SEQUENCE 'S'
#define RELKIND_VIEW	 'v'

/* catalog oids (pg_class.dat / pg_namespace.dat, verbatim values) */
#define RelationRelationId	 1259
#define DatabaseRelationId	 1262
#define ProcedureRelationId	 1255
#define LanguageRelationId	 2612
#define NamespaceRelationId	 2615
#define TypeRelationId		 1247
#define PG_TOAST_NAMESPACE	 99
#define FirstUnpinnedObjectId 12000

/* pg_authid.dat predefined roles */
#define ROLE_PG_READ_ALL_DATA  6181
#define ROLE_PG_WRITE_ALL_DATA 6182
#define ROLE_PG_MAINTAIN	   6337

/* ---- PGQ_ERR_* : distinct PROOF_EREPORT codes per errcode (sqlstate
 * parity; the Rust harness maps Err sqlstates onto the same codes) ---- */
#define PGQ_ERR_INVALID_PARAMETER_VALUE 1	/* unrecognized privilege type */
#define PGQ_ERR_UNDEFINED_OBJECT		2	/* role does not exist */
#define PGQ_ERR_UNDEFINED_TABLE			3	/* relation does not exist */
#define PGQ_ERR_WRONG_OBJECT_TYPE		4	/* not a sequence */
#define PGQ_ERR_NAME_TOO_LONG			5	/* identifier too long */
#define PGQ_ERR_INVALID_TEXT_REP		6	/* aclparse syntax */
#define PGQ_ERR_FEATURE_NOT_SUPPORTED	7	/* aclinsert/aclremove */
#define PGQ_ERR_INTERNAL				8	/* elog / cache lookup failed */
#define PGQ_ERR_UNDEFINED_SCHEMA		9	/* schema does not exist */

/* =====================================================================
 * SEAM STATE (extern; the Rust harness writes these AND the mirrored
 * Rust-side statics with the same symbolic values)
 * ===================================================================== */

/* 1. membership oracle */
#define PGQ_MEMB_N 8
Oid			pgq_memb_role[PGQ_MEMB_N];
int			pgq_memb_ans[PGQ_MEMB_N];
int			pgq_memb_default;

static bool
has_privs_of_role(Oid member, Oid role)
{
	int			i;

	(void) member;				/* member-fixed restriction of the seam */
	for (i = 0; i < PGQ_MEMB_N; i++)
		if (pgq_memb_role[i] == role)
			return pgq_memb_ans[i] != 0;
	return pgq_memb_default != 0;
}

/* 2. superuser oracle */
#define PGQ_SUPER_N 2
Oid			pgq_super_role[PGQ_SUPER_N];
int			pgq_super_ans[PGQ_SUPER_N];
int			pgq_super_default;

static bool
superuser_arg(Oid roleid)
{
	int			i;

	for (i = 0; i < PGQ_SUPER_N; i++)
		if (pgq_super_role[i] == roleid)
			return pgq_super_ans[i] != 0;
	return pgq_super_default != 0;
}

/* 3. catalog-tuple seam (single inspected object per call) */
int			pgq_cat_found;
Oid			pgq_cat_owner;
int			pgq_cat_relkind;	/* pg_class rows only */
Oid			pgq_cat_relnamespace;	/* pg_class rows only */
int			pgq_cat_acl_isnull;
int			pgq_cat_nacl;		/* <= PGQ_MAX_ACL */
AclItem		pgq_cat_acl[PGQ_MAX_ACL];

/*
 * Setter shim (plumbing only): a struct-typed global shared across the
 * Kani/C goto-link boundary trips a goto-cc linker invariant
 * (casting_replace_symbol: "front-ends should construct symbol expressions
 * with source locations" — struct tags differ between the two frontends,
 * tag-AclItem vs tag-adt_acl::AclItem).  The Rust harness writes
 * pgq_cat_acl through this scalar-args setter instead of declaring the
 * struct global extern.
 */
int
pgq_set_cat_acl(int i, Oid grantee, Oid grantor, AclMode privs)
{
	/* int (not void) return: Kani lowers Rust () as struct Unit, which
	 * goto-cc rejects against C void (skill void/Unit trap). */
	pgq_cat_acl[i].ai_grantee = grantee;
	pgq_cat_acl[i].ai_grantor = grantor;
	pgq_cat_acl[i].ai_privs = privs;
	return 0;
}

/* 4. role-name oracle (call-indexed; see file header) */
int			pgq_role_calls;
int			pgq_role_found[2];
Oid			pgq_role_oid[2];

/* 5. object-name oracle (total) */
Oid			pgq_objname_oid;

/* 6. current user */
Oid			pgq_current_user;
#define GetUserId() (pgq_current_user)

/* 7. role-name-by-oid oracle for aclitemout */
int			pgq_rname_found[2];
Oid			pgq_rname_oid[2];
char		pgq_rname_name[2][NAMEDATALEN];

/* 8. namespace session state (pinned 0 in equivalence harnesses) */
int			pgq_is_temp_namespace;
int			pgq_temp_toast;
Oid			pgq_my_database_id;
#define MyDatabaseId (pgq_my_database_id)

/* =====================================================================
 * src/port/pgstrcasecmp.c — pg_strcasecmp, verbatim (C locale)
 * ===================================================================== */
int
pg_strcasecmp(const char *s1, const char *s2)
{
	for (;;)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

/* =====================================================================
 * acl.c — get_role_oid / get_role_oid_or_public (role-name oracle seam)
 * ===================================================================== */

/* GetSysCacheOid1(AUTHNAME, ...) -> call-indexed oracle (seam 4) */
static Oid
pgq_role_lookup(const char *rolname)
{
	int			i = pgq_role_calls < 2 ? pgq_role_calls : 1;

	(void) rolname;				/* one symbolic name per call index */
	pgq_role_calls++;
	return pgq_role_found[i] ? pgq_role_oid[i] : InvalidOid;
}

static Oid
pg_get_role_oid(const char *rolname, bool missing_ok, int *err)
{
	Oid			oid;

	oid = pgq_role_lookup(rolname);
	if (!OidIsValid(oid) && !missing_ok)
	{
		/* ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT), ...) */
		*err = PGQ_ERR_UNDEFINED_OBJECT;
		return InvalidOid;
	}
	return oid;
}

static Oid
pg_get_role_oid_or_public(const char *rolname, int *err)
{
	if (strcmp(rolname, "public") == 0)
		return ACL_ID_PUBLIC;

	return pg_get_role_oid(rolname, false, err);
}

/* =====================================================================
 * acl.c — aclmask, body verbatim (Acl container shim; check_acl dropped
 * with the container, see file header)
 * ===================================================================== */
static AclMode
aclmask(const Acl *acl, Oid roleid, Oid ownerId,
		AclMode mask, AclMaskHow how)
{
	AclMode		result;
	AclMode		remaining;
	const AclItem *aidat;
	int			i,
				num;

	/* Quick exit for mask == 0 */
	if (mask == 0)
		return 0;

	num = ACL_NUM(acl);
	aidat = ACL_DAT(acl);

	result = 0;

	/* Owner always implicitly has all grant options */
	if ((mask & ACLITEM_ALL_GOPTION_BITS) &&
		has_privs_of_role(roleid, ownerId))
	{
		result = mask & ACLITEM_ALL_GOPTION_BITS;
		if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
			return result;
	}

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

/* =====================================================================
 * acl.c — acldefault, body verbatim (allocacl -> caller-provided Acl)
 * ===================================================================== */
static Acl *
acldefault(ObjectType objtype, Oid ownerId, Acl *acl_out, int *err)
{
	AclMode		world_default;
	AclMode		owner_default;
	int			nacl;
	Acl		   *acl;
	AclItem    *aip;

	switch (objtype)
	{
		case OBJECT_COLUMN:
			/* by default, columns have no extra privileges */
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_NO_RIGHTS;
			break;
		case OBJECT_TABLE:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_RELATION;
			break;
		case OBJECT_SEQUENCE:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_SEQUENCE;
			break;
		case OBJECT_DATABASE:
			/* for backwards compatibility, grant some rights by default */
			world_default = ACL_CREATE_TEMP | ACL_CONNECT;
			owner_default = ACL_ALL_RIGHTS_DATABASE;
			break;
		case OBJECT_FUNCTION:
			/* Grant EXECUTE by default, for now */
			world_default = ACL_EXECUTE;
			owner_default = ACL_ALL_RIGHTS_FUNCTION;
			break;
		case OBJECT_LANGUAGE:
			/* Grant USAGE by default, for now */
			world_default = ACL_USAGE;
			owner_default = ACL_ALL_RIGHTS_LANGUAGE;
			break;
		case OBJECT_LARGEOBJECT:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_LARGEOBJECT;
			break;
		case OBJECT_SCHEMA:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_SCHEMA;
			break;
		case OBJECT_TABLESPACE:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_TABLESPACE;
			break;
		case OBJECT_FDW:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_FDW;
			break;
		case OBJECT_FOREIGN_SERVER:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_FOREIGN_SERVER;
			break;
		case OBJECT_DOMAIN:
		case OBJECT_TYPE:
			world_default = ACL_USAGE;
			owner_default = ACL_ALL_RIGHTS_TYPE;
			break;
		case OBJECT_PARAMETER_ACL:
			world_default = ACL_NO_RIGHTS;
			owner_default = ACL_ALL_RIGHTS_PARAMETER_ACL;
			break;
		default:
			/* elog(ERROR, "unrecognized object type: %d", objtype) */
			*err = PGQ_ERR_INTERNAL;
			world_default = ACL_NO_RIGHTS;	/* keep compiler quiet */
			owner_default = ACL_NO_RIGHTS;
			return acl_out;
	}

	nacl = 0;
	if (world_default != ACL_NO_RIGHTS)
		nacl++;
	if (owner_default != ACL_NO_RIGHTS)
		nacl++;

	acl = acl_out;				/* shim: allocacl(nacl) -> caller storage */
	acl->num = nacl;
	aip = ACL_DAT(acl);

	if (world_default != ACL_NO_RIGHTS)
	{
		aip->ai_grantee = ACL_ID_PUBLIC;
		aip->ai_grantor = ownerId;
		ACLITEM_SET_PRIVS_GOPTIONS(*aip, world_default, ACL_NO_RIGHTS);
		aip++;
	}

	/*
	 * Note that the owner's entry shows all ordinary privileges but no grant
	 * options.  This is because his grant options come "from the system" and
	 * not from his own efforts.  (The SQL spec says that the owner's rights
	 * come from a "_SYSTEM" authid.)  However, we do consider that the
	 * owner's ordinary privileges are self-granted; this lets him revoke
	 * them.  We implement the owner's grant options without any explicit
	 * "_SYSTEM"-like ACL entry, by internally special-casing the owner
	 * wherever we are testing grant options.
	 */
	if (owner_default != ACL_NO_RIGHTS)
	{
		aip->ai_grantee = ownerId;
		aip->ai_grantor = ownerId;
		ACLITEM_SET_PRIVS_GOPTIONS(*aip, owner_default, ACL_NO_RIGHTS);
	}

	return acl;
}

/* =====================================================================
 * catalog.c — IsSystemClass, reduced to the reachable form:
 * IsCatalogRelationOid(relid) || IsToastNamespace(relnamespace), with
 * isTempToastNamespace -> seam 8 (session state).
 * ===================================================================== */
static bool
IsSystemClass(Oid relid, Oid relnamespace)
{
	/* IsCatalogRelationOid: relid < FirstUnpinnedObjectId */
	/* IsToastNamespace: PG_TOAST_NAMESPACE || isTempToastNamespace */
	return relid < FirstUnpinnedObjectId ||
		relnamespace == PG_TOAST_NAMESPACE ||
		pgq_temp_toast != 0;
}

/* =====================================================================
 * aclchk.c — pg_class_aclmask_ext, body verbatim modulo the catalog-tuple
 * seam (SearchSysCache1/GETSTRUCT/SysCacheGetAttr -> pgq_cat_*) and the
 * ereport -> err-flag shim.
 * ===================================================================== */
static AclMode
pg_class_aclmask_ext_(Oid table_oid, Oid roleid, AclMode mask,
					  AclMaskHow how, bool *is_missing, int *err)
{
	AclMode		result;
	static Acl	default_acl;
	Acl		   *acl;
	Oid			ownerId;

	/*
	 * Must get the relation's tuple from pg_class
	 */
	if (!pgq_cat_found)			/* !HeapTupleIsValid(tuple) */
	{
		if (is_missing != NULL)
		{
			/* return "no privileges" instead of throwing an error */
			*is_missing = true;
			return 0;
		}
		else
		{
			/* ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE), ...) */
			*err = PGQ_ERR_UNDEFINED_TABLE;
			return 0;
		}
	}

	/*
	 * Deny anyone permission to update a system catalog unless
	 * pg_authid.rolsuper is set.
	 *
	 * As of 7.4 we have some updatable system views; those shouldn't be
	 * protected in this way.  Assume the view rules can take care of
	 * themselves.  ACL_USAGE is if we ever have system sequences.
	 */
	if ((mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE)) &&
		IsSystemClass(table_oid, pgq_cat_relnamespace) &&
		pgq_cat_relkind != RELKIND_VIEW &&
		!superuser_arg(roleid))
		mask &= ~(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE);

	/*
	 * Otherwise, superusers bypass all permission-checking.
	 */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Normal case: get the relation's ACL from pg_class
	 */
	ownerId = pgq_cat_owner;

	if (pgq_cat_acl_isnull)
	{
		/* No ACL, so build default ACL */
		switch (pgq_cat_relkind)
		{
			case RELKIND_SEQUENCE:
				acl = acldefault(OBJECT_SEQUENCE, ownerId, &default_acl, err);
				break;
			default:
				acl = acldefault(OBJECT_TABLE, ownerId, &default_acl, err);
				break;
		}
	}
	else
	{
		/* detoast rel's ACL if necessary */
		static Acl	stored_acl;
		int			i;

		stored_acl.num = pgq_cat_nacl;
		for (i = 0; i < PGQ_MAX_ACL; i++)
			stored_acl.items[i] = pgq_cat_acl[i];
		acl = &stored_acl;
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/*
	 * Check if ACL_SELECT is being checked and, if so, and not set already as
	 * part of the result, then check if the user is a member of the
	 * pg_read_all_data role, which allows read access to all relations.
	 */
	if (mask & ACL_SELECT && !(result & ACL_SELECT) &&
		has_privs_of_role(roleid, ROLE_PG_READ_ALL_DATA))
		result |= ACL_SELECT;

	/*
	 * Check if ACL_INSERT, ACL_UPDATE, or ACL_DELETE is being checked and, if
	 * so, and not set already as part of the result, then check if the user
	 * is a member of the pg_write_all_data role, which allows
	 * INSERT/UPDATE/DELETE access to all relations (except system catalogs,
	 * which requires superuser, see above).
	 */
	if (mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE) &&
		!(result & (ACL_INSERT | ACL_UPDATE | ACL_DELETE)) &&
		has_privs_of_role(roleid, ROLE_PG_WRITE_ALL_DATA))
		result |= (mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE));

	/*
	 * Check if ACL_MAINTAIN is being checked and, if so, and not already set
	 * as part of the result, then check if the user is a member of the
	 * pg_maintain role, which allows VACUUM, ANALYZE, CLUSTER, REFRESH
	 * MATERIALIZED VIEW, REINDEX, and LOCK TABLE on all relations.
	 */
	if (mask & ACL_MAINTAIN &&
		!(result & ACL_MAINTAIN) &&
		has_privs_of_role(roleid, ROLE_PG_MAINTAIN))
		result |= ACL_MAINTAIN;

	return result;
}

/* aclchk.c — pg_class_aclcheck_ext / pg_class_aclcheck, verbatim */
static AclResult
pg_class_aclcheck_ext_(Oid table_oid, Oid roleid,
					   AclMode mode, bool *is_missing, int *err)
{
	if (pg_class_aclmask_ext_(table_oid, roleid, mode,
							  ACLMASK_ANY, is_missing, err) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

static AclResult
pg_class_aclcheck_(Oid table_oid, Oid roleid, AclMode mode, int *err)
{
	return pg_class_aclcheck_ext_(table_oid, roleid, mode, NULL, err);
}

/* =====================================================================
 * aclchk.c — pg_namespace_aclmask_ext, body verbatim modulo the seams
 * (isTempNamespace -> seam 8; catalog tuple -> seam 3).
 * ===================================================================== */
static AclResult object_aclcheck_ext_(Oid classid, Oid objectid, Oid roleid,
									  AclMode mode, bool *is_missing, int *err);

static AclMode
pg_namespace_aclmask_ext_(Oid nsp_oid, Oid roleid,
						  AclMode mask, AclMaskHow how,
						  bool *is_missing, int *err)
{
	AclMode		result;
	static Acl	default_acl;
	Acl		   *acl;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * If we have been assigned this namespace as a temp namespace, check to
	 * make sure we have CREATE TEMP permission on the database, and if so act
	 * as though we have all standard (but not GRANT OPTION) permissions on
	 * the namespace.  If we don't have CREATE TEMP, act as though we have
	 * only USAGE (and not CREATE) rights.
	 */
	if (pgq_is_temp_namespace)	/* isTempNamespace(nsp_oid) */
	{
		if (object_aclcheck_ext_(DatabaseRelationId, MyDatabaseId, roleid,
								 ACL_CREATE_TEMP, is_missing, err) == ACLCHECK_OK)
			return mask & ACL_ALL_RIGHTS_SCHEMA;
		else
			return mask & ACL_USAGE;
	}

	/*
	 * Get the schema's ACL from pg_namespace
	 */
	if (!pgq_cat_found)
	{
		if (is_missing != NULL)
		{
			/* return "no privileges" instead of throwing an error */
			*is_missing = true;
			return 0;
		}
		else
		{
			/* ereport(ERROR, errcode(ERRCODE_UNDEFINED_SCHEMA), ...) */
			*err = PGQ_ERR_UNDEFINED_SCHEMA;
			return 0;
		}
	}

	ownerId = pgq_cat_owner;

	if (pgq_cat_acl_isnull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(OBJECT_SCHEMA, ownerId, &default_acl, err);
	}
	else
	{
		static Acl	stored_acl;
		int			i;

		stored_acl.num = pgq_cat_nacl;
		for (i = 0; i < PGQ_MAX_ACL; i++)
			stored_acl.items[i] = pgq_cat_acl[i];
		acl = &stored_acl;
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/*
	 * Check if ACL_USAGE is being checked and, if so, and not set already as
	 * part of the result, then check if the user is a member of the
	 * pg_read_all_data or pg_write_all_data roles, which allow usage access
	 * to all schemas.
	 */
	if (mask & ACL_USAGE && !(result & ACL_USAGE) &&
		(has_privs_of_role(roleid, ROLE_PG_READ_ALL_DATA) ||
		 has_privs_of_role(roleid, ROLE_PG_WRITE_ALL_DATA)))
		result |= ACL_USAGE;
	return result;
}

/* =====================================================================
 * aclchk.c — object_aclmask_ext, body verbatim modulo the seams; the
 * objectaddress.c property lookups collapse into the catalog-tuple seam
 * plus this classid -> ObjectType switch (mirrors the ObjectProperty rows
 * for the classids reachable from the vendored SQL functions).
 * ===================================================================== */
static ObjectType
get_object_type_(Oid classid)
{
	switch (classid)
	{
		case DatabaseRelationId:
			return OBJECT_DATABASE;
		case ProcedureRelationId:
			return OBJECT_FUNCTION;
		case LanguageRelationId:
			return OBJECT_LANGUAGE;
		default:
			return OBJECT_TABLE;	/* unreachable in this family */
	}
}

static AclMode
object_aclmask_ext_(Oid classid, Oid objectid, Oid roleid,
					AclMode mask, AclMaskHow how,
					bool *is_missing, int *err)
{
	AclMode		result;
	static Acl	default_acl;
	Acl		   *acl;
	Oid			ownerId;

	/* Special cases */
	switch (classid)
	{
		case NamespaceRelationId:
			return pg_namespace_aclmask_ext_(objectid, roleid, mask, how,
											 is_missing, err);
		case TypeRelationId:
			/* pg_type_aclmask_ext: not vendored (unreachable here) */
			*err = PGQ_ERR_INTERNAL;
			return 0;
	}

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Get the object's ACL from its catalog
	 */
	if (!pgq_cat_found)			/* !HeapTupleIsValid(tuple) */
	{
		if (is_missing != NULL)
		{
			/* return "no privileges" instead of throwing an error */
			*is_missing = true;
			return 0;
		}
		else
		{
			/* elog(ERROR, "cache lookup failed for %s %u", ...) */
			*err = PGQ_ERR_INTERNAL;
			return 0;
		}
	}

	ownerId = pgq_cat_owner;

	if (pgq_cat_acl_isnull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(get_object_type_(classid), ownerId, &default_acl, err);
	}
	else
	{
		/* detoast ACL if necessary */
		static Acl	stored_acl;
		int			i;

		stored_acl.num = pgq_cat_nacl;
		for (i = 0; i < PGQ_MAX_ACL; i++)
			stored_acl.items[i] = pgq_cat_acl[i];
		acl = &stored_acl;
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	return result;
}

/* aclchk.c — object_aclcheck_ext / object_aclcheck, verbatim */
static AclResult
object_aclcheck_ext_(Oid classid, Oid objectid,
					 Oid roleid, AclMode mode,
					 bool *is_missing, int *err)
{
	if (object_aclmask_ext_(classid, objectid, roleid, mode, ACLMASK_ANY,
							is_missing, err) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

static AclResult
object_aclcheck_(Oid classid, Oid objectid, Oid roleid, AclMode mode, int *err)
{
	return object_aclcheck_ext_(classid, objectid, roleid, mode, NULL, err);
}

/* =====================================================================
 * acl.c — convert_any_priv_string + the per-class priv maps, bodies
 * verbatim (text_to_cstring -> caller-provided modifiable buffer;
 * ereport -> err flag)
 * ===================================================================== */
typedef struct
{
	const char *name;
	AclMode		value;
} priv_map;

static AclMode
convert_any_priv_string(char *priv_type /* modifiable */ ,
						const priv_map *privileges, int *err)
{
	AclMode		result = 0;
	char	   *chunk;
	char	   *next_chunk;

	/* We rely on priv_type being a private, modifiable string */
	for (chunk = priv_type; chunk; chunk = next_chunk)
	{
		int			chunk_len;
		const priv_map *this_priv;

		/* Split string at commas */
		next_chunk = strchr(chunk, ',');
		if (next_chunk)
			*next_chunk++ = '\0';

		/* Drop leading/trailing whitespace in this chunk */
		while (*chunk && isspace((unsigned char) *chunk))
			chunk++;
		chunk_len = strlen(chunk);
		while (chunk_len > 0 && isspace((unsigned char) chunk[chunk_len - 1]))
			chunk_len--;
		chunk[chunk_len] = '\0';

		/* Match to the privileges list */
		for (this_priv = privileges; this_priv->name; this_priv++)
		{
			if (pg_strcasecmp(this_priv->name, chunk) == 0)
			{
				result |= this_priv->value;
				break;
			}
		}
		if (!this_priv->name)
		{
			/* ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE), ...) */
			*err = PGQ_ERR_INVALID_PARAMETER_VALUE;
			return 0;
		}
	}

	return result;
}

static AclMode
convert_table_priv_string(char *priv_type_text, int *err)
{
	static const priv_map table_priv_map[] = {
		{"SELECT", ACL_SELECT},
		{"SELECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_SELECT)},
		{"INSERT", ACL_INSERT},
		{"INSERT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_INSERT)},
		{"UPDATE", ACL_UPDATE},
		{"UPDATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_UPDATE)},
		{"DELETE", ACL_DELETE},
		{"DELETE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_DELETE)},
		{"TRUNCATE", ACL_TRUNCATE},
		{"TRUNCATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_TRUNCATE)},
		{"REFERENCES", ACL_REFERENCES},
		{"REFERENCES WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_REFERENCES)},
		{"TRIGGER", ACL_TRIGGER},
		{"TRIGGER WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_TRIGGER)},
		{"MAINTAIN", ACL_MAINTAIN},
		{"MAINTAIN WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_MAINTAIN)},
		{NULL, 0}
	};

	return convert_any_priv_string(priv_type_text, table_priv_map, err);
}

static AclMode
convert_sequence_priv_string(char *priv_type_text, int *err)
{
	static const priv_map sequence_priv_map[] = {
		{"USAGE", ACL_USAGE},
		{"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
		{"SELECT", ACL_SELECT},
		{"SELECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_SELECT)},
		{"UPDATE", ACL_UPDATE},
		{"UPDATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_UPDATE)},
		{NULL, 0}
	};

	return convert_any_priv_string(priv_type_text, sequence_priv_map, err);
}

static AclMode
convert_database_priv_string(char *priv_type_text, int *err)
{
	static const priv_map database_priv_map[] = {
		{"CREATE", ACL_CREATE},
		{"CREATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
		{"TEMPORARY", ACL_CREATE_TEMP},
		{"TEMPORARY WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE_TEMP)},
		{"TEMP", ACL_CREATE_TEMP},
		{"TEMP WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE_TEMP)},
		{"CONNECT", ACL_CONNECT},
		{"CONNECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CONNECT)},
		{NULL, 0}
	};

	return convert_any_priv_string(priv_type_text, database_priv_map, err);
}

static AclMode
convert_function_priv_string(char *priv_type_text, int *err)
{
	static const priv_map function_priv_map[] = {
		{"EXECUTE", ACL_EXECUTE},
		{"EXECUTE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_EXECUTE)},
		{NULL, 0}
	};

	return convert_any_priv_string(priv_type_text, function_priv_map, err);
}

static AclMode
convert_language_priv_string(char *priv_type_text, int *err)
{
	static const priv_map language_priv_map[] = {
		{"USAGE", ACL_USAGE},
		{"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
		{NULL, 0}
	};

	return convert_any_priv_string(priv_type_text, language_priv_map, err);
}

static AclMode
convert_schema_priv_string(char *priv_type_text, int *err)
{
	static const priv_map schema_priv_map[] = {
		{"CREATE", ACL_CREATE},
		{"CREATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
		{"USAGE", ACL_USAGE},
		{"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
		{NULL, 0}
	};

	return convert_any_priv_string(priv_type_text, schema_priv_map, err);
}

/* object-name oracle (seam 5): convert_table_name / convert_database_name /
 * convert_language_name / convert_schema_name; total, name bytes opaque */
static Oid
pgq_convert_object_name(void)
{
	return pgq_objname_oid;
}

/* lsyscache get_rel_relkind against the catalog-tuple seam: '\0' if the
 * object row is absent, else the seam relkind (same one-row model as the
 * aclcheck read; both reads inspect the same pg_class row) */
static char
pgq_get_rel_relkind(Oid relid)
{
	(void) relid;
	return pgq_cat_found ? (char) pgq_cat_relkind : '\0';
}

/* =====================================================================
 * acl.c — has_table_privilege_* cores (fmgr unwrap shims; bodies verbatim)
 * ===================================================================== */
int
pg_has_table_privilege_name_name(const char *rolename, char *priv,
								 int *isnull, int *err)
{
	Oid			roleid;
	Oid			tableoid;
	AclMode		mode;
	AclResult	aclresult;

	(void) isnull;
	roleid = pg_get_role_oid_or_public(rolename, err);
	if (*err)
		return 0;
	tableoid = pgq_convert_object_name();
	mode = convert_table_priv_string(priv, err);
	if (*err)
		return 0;

	aclresult = pg_class_aclcheck_(tableoid, roleid, mode, err);
	if (*err)
		return 0;

	return aclresult == ACLCHECK_OK;
}

int
pg_has_table_privilege_name(char *priv, int *isnull, int *err)
{
	Oid			roleid;
	Oid			tableoid;
	AclMode		mode;
	AclResult	aclresult;

	(void) isnull;
	roleid = GetUserId();
	tableoid = pgq_convert_object_name();
	mode = convert_table_priv_string(priv, err);
	if (*err)
		return 0;

	aclresult = pg_class_aclcheck_(tableoid, roleid, mode, err);
	if (*err)
		return 0;

	return aclresult == ACLCHECK_OK;
}

int
pg_has_table_privilege_name_id(const char *username, Oid tableoid,
							   char *priv, int *isnull, int *err)
{
	Oid			roleid;
	AclMode		mode;
	AclResult	aclresult;
	bool		is_missing = false;

	roleid = pg_get_role_oid_or_public(username, err);
	if (*err)
		return 0;
	mode = convert_table_priv_string(priv, err);
	if (*err)
		return 0;

	aclresult = pg_class_aclcheck_ext_(tableoid, roleid, mode, &is_missing, err);
	if (*err)
		return 0;

	if (is_missing)
	{
		*isnull = 1;
		return 0;
	}

	return aclresult == ACLCHECK_OK;
}

int
pg_has_table_privilege_id(Oid tableoid, char *priv, int *isnull, int *err)
{
	Oid			roleid;
	AclMode		mode;
	AclResult	aclresult;
	bool		is_missing = false;

	roleid = GetUserId();
	mode = convert_table_priv_string(priv, err);
	if (*err)
		return 0;

	aclresult = pg_class_aclcheck_ext_(tableoid, roleid, mode, &is_missing, err);
	if (*err)
		return 0;

	if (is_missing)
	{
		*isnull = 1;
		return 0;
	}

	return aclresult == ACLCHECK_OK;
}

int
pg_has_table_privilege_id_name(Oid roleid, char *priv, int *isnull, int *err)
{
	Oid			tableoid;
	AclMode		mode;
	AclResult	aclresult;

	(void) isnull;
	tableoid = pgq_convert_object_name();
	mode = convert_table_priv_string(priv, err);
	if (*err)
		return 0;

	aclresult = pg_class_aclcheck_(tableoid, roleid, mode, err);
	if (*err)
		return 0;

	return aclresult == ACLCHECK_OK;
}

int
pg_has_table_privilege_id_id(Oid roleid, Oid tableoid, char *priv,
							 int *isnull, int *err)
{
	AclMode		mode;
	AclResult	aclresult;
	bool		is_missing = false;

	mode = convert_table_priv_string(priv, err);
	if (*err)
		return 0;

	aclresult = pg_class_aclcheck_ext_(tableoid, roleid, mode, &is_missing, err);
	if (*err)
		return 0;

	if (is_missing)
	{
		*isnull = 1;
		return 0;
	}

	return aclresult == ACLCHECK_OK;
}

/* =====================================================================
 * acl.c — has_sequence_privilege_* cores (verbatim; get_rel_relkind ->
 * catalog-tuple seam; get_rel_name in the error message is message text
 * and does not cross the seam)
 * ===================================================================== */
int
pg_has_sequence_privilege_name_name(const char *rolename, char *priv,
									int *isnull, int *err)
{
	Oid			roleid;
	Oid			sequenceoid;
	AclMode		mode;
	AclResult	aclresult;

	(void) isnull;
	roleid = pg_get_role_oid_or_public(rolename, err);
	if (*err)
		return 0;
	mode = convert_sequence_priv_string(priv, err);
	if (*err)
		return 0;
	sequenceoid = pgq_convert_object_name();
	if (pgq_get_rel_relkind(sequenceoid) != RELKIND_SEQUENCE)
	{
		/* ereport(ERROR, errcode(ERRCODE_WRONG_OBJECT_TYPE), ...) */
		*err = PGQ_ERR_WRONG_OBJECT_TYPE;
		return 0;
	}

	aclresult = pg_class_aclcheck_(sequenceoid, roleid, mode, err);
	if (*err)
		return 0;

	return aclresult == ACLCHECK_OK;
}

int
pg_has_sequence_privilege_name(char *priv, int *isnull, int *err)
{
	Oid			roleid;
	Oid			sequenceoid;
	AclMode		mode;
	AclResult	aclresult;

	(void) isnull;
	roleid = GetUserId();
	mode = convert_sequence_priv_string(priv, err);
	if (*err)
		return 0;
	sequenceoid = pgq_convert_object_name();
	if (pgq_get_rel_relkind(sequenceoid) != RELKIND_SEQUENCE)
	{
		*err = PGQ_ERR_WRONG_OBJECT_TYPE;
		return 0;
	}

	aclresult = pg_class_aclcheck_(sequenceoid, roleid, mode, err);
	if (*err)
		return 0;

	return aclresult == ACLCHECK_OK;
}

int
pg_has_sequence_privilege_name_id(const char *username, Oid sequenceoid,
								  char *priv, int *isnull, int *err)
{
	Oid			roleid;
	AclMode		mode;
	AclResult	aclresult;
	char		relkind;
	bool		is_missing = false;

	roleid = pg_get_role_oid_or_public(username, err);
	if (*err)
		return 0;
	mode = convert_sequence_priv_string(priv, err);
	if (*err)
		return 0;
	relkind = pgq_get_rel_relkind(sequenceoid);
	if (relkind == '\0')
	{
		*isnull = 1;
		return 0;
	}
	else if (relkind != RELKIND_SEQUENCE)
	{
		*err = PGQ_ERR_WRONG_OBJECT_TYPE;
		return 0;
	}

	aclresult = pg_class_aclcheck_ext_(sequenceoid, roleid, mode, &is_missing, err);
	if (*err)
		return 0;

	if (is_missing)
	{
		*isnull = 1;
		return 0;
	}

	return aclresult == ACLCHECK_OK;
}

int
pg_has_sequence_privilege_id(Oid sequenceoid, char *priv, int *isnull, int *err)
{
	Oid			roleid;
	AclMode		mode;
	AclResult	aclresult;
	char		relkind;
	bool		is_missing = false;

	roleid = GetUserId();
	mode = convert_sequence_priv_string(priv, err);
	if (*err)
		return 0;
	relkind = pgq_get_rel_relkind(sequenceoid);
	if (relkind == '\0')
	{
		*isnull = 1;
		return 0;
	}
	else if (relkind != RELKIND_SEQUENCE)
	{
		*err = PGQ_ERR_WRONG_OBJECT_TYPE;
		return 0;
	}

	aclresult = pg_class_aclcheck_ext_(sequenceoid, roleid, mode, &is_missing, err);
	if (*err)
		return 0;

	if (is_missing)
	{
		*isnull = 1;
		return 0;
	}

	return aclresult == ACLCHECK_OK;
}

int
pg_has_sequence_privilege_id_name(Oid roleid, char *priv, int *isnull, int *err)
{
	Oid			sequenceoid;
	AclMode		mode;
	AclResult	aclresult;

	(void) isnull;
	mode = convert_sequence_priv_string(priv, err);
	if (*err)
		return 0;
	sequenceoid = pgq_convert_object_name();
	if (pgq_get_rel_relkind(sequenceoid) != RELKIND_SEQUENCE)
	{
		*err = PGQ_ERR_WRONG_OBJECT_TYPE;
		return 0;
	}

	aclresult = pg_class_aclcheck_(sequenceoid, roleid, mode, err);
	if (*err)
		return 0;

	return aclresult == ACLCHECK_OK;
}

int
pg_has_sequence_privilege_id_id(Oid roleid, Oid sequenceoid, char *priv,
								int *isnull, int *err)
{
	AclMode		mode;
	AclResult	aclresult;
	char		relkind;
	bool		is_missing = false;

	mode = convert_sequence_priv_string(priv, err);
	if (*err)
		return 0;
	relkind = pgq_get_rel_relkind(sequenceoid);
	if (relkind == '\0')
	{
		*isnull = 1;
		return 0;
	}
	else if (relkind != RELKIND_SEQUENCE)
	{
		*err = PGQ_ERR_WRONG_OBJECT_TYPE;
		return 0;
	}

	aclresult = pg_class_aclcheck_ext_(sequenceoid, roleid, mode, &is_missing, err);
	if (*err)
		return 0;

	if (is_missing)
	{
		*isnull = 1;
		return 0;
	}

	return aclresult == ACLCHECK_OK;
}

/* =====================================================================
 * acl.c — generic object-class has_*_privilege cores (database/function/
 * language/schema), bodies verbatim; one macro block per the six argument
 * forms, mirroring the acl.c function bodies exactly.
 * ===================================================================== */
#define PGQ_DEFINE_OBJECT_FAMILY(fam, CLASSID, CONVERT_PRIV)				\
int																			\
pg_has_##fam##_privilege_name_name(const char *rolename, char *priv,		\
								   int *isnull, int *err)					\
{																			\
	Oid			roleid;														\
	Oid			objoid;														\
	AclMode		mode;														\
	AclResult	aclresult;													\
																			\
	(void) isnull;															\
	roleid = pg_get_role_oid_or_public(rolename, err);						\
	if (*err)																\
		return 0;															\
	objoid = pgq_convert_object_name();										\
	mode = CONVERT_PRIV(priv, err);											\
	if (*err)																\
		return 0;															\
	aclresult = object_aclcheck_(CLASSID, objoid, roleid, mode, err);		\
	if (*err)																\
		return 0;															\
	return aclresult == ACLCHECK_OK;										\
}																			\
int																			\
pg_has_##fam##_privilege_name_id(const char *rolename, Oid objoid,			\
								 char *priv, int *isnull, int *err)			\
{																			\
	Oid			roleid;														\
	AclMode		mode;														\
	AclResult	aclresult;													\
	bool		is_missing = false;											\
																			\
	roleid = pg_get_role_oid_or_public(rolename, err);						\
	if (*err)																\
		return 0;															\
	mode = CONVERT_PRIV(priv, err);											\
	if (*err)																\
		return 0;															\
	aclresult = object_aclcheck_ext_(CLASSID, objoid, roleid, mode,			\
									 &is_missing, err);						\
	if (*err)																\
		return 0;															\
	if (is_missing)															\
	{																		\
		*isnull = 1;														\
		return 0;															\
	}																		\
	return aclresult == ACLCHECK_OK;										\
}																			\
int																			\
pg_has_##fam##_privilege_id_name(Oid roleid, char *priv,					\
								 int *isnull, int *err)						\
{																			\
	Oid			objoid;														\
	AclMode		mode;														\
	AclResult	aclresult;													\
																			\
	(void) isnull;															\
	objoid = pgq_convert_object_name();										\
	mode = CONVERT_PRIV(priv, err);											\
	if (*err)																\
		return 0;															\
	aclresult = object_aclcheck_(CLASSID, objoid, roleid, mode, err);		\
	if (*err)																\
		return 0;															\
	return aclresult == ACLCHECK_OK;										\
}																			\
int																			\
pg_has_##fam##_privilege_id_id(Oid roleid, Oid objoid, char *priv,			\
							   int *isnull, int *err)						\
{																			\
	AclMode		mode;														\
	AclResult	aclresult;													\
	bool		is_missing = false;											\
																			\
	mode = CONVERT_PRIV(priv, err);											\
	if (*err)																\
		return 0;															\
	aclresult = object_aclcheck_ext_(CLASSID, objoid, roleid, mode,			\
									 &is_missing, err);						\
	if (*err)																\
		return 0;															\
	if (is_missing)															\
	{																		\
		*isnull = 1;														\
		return 0;															\
	}																		\
	return aclresult == ACLCHECK_OK;										\
}																			\
int																			\
pg_has_##fam##_privilege_name(char *priv, int *isnull, int *err)			\
{																			\
	Oid			roleid;														\
	Oid			objoid;														\
	AclMode		mode;														\
	AclResult	aclresult;													\
																			\
	(void) isnull;															\
	roleid = GetUserId();													\
	objoid = pgq_convert_object_name();										\
	mode = CONVERT_PRIV(priv, err);											\
	if (*err)																\
		return 0;															\
	aclresult = object_aclcheck_(CLASSID, objoid, roleid, mode, err);		\
	if (*err)																\
		return 0;															\
	return aclresult == ACLCHECK_OK;										\
}																			\
int																			\
pg_has_##fam##_privilege_id(Oid objoid, char *priv, int *isnull, int *err)	\
{																			\
	Oid			roleid;														\
	AclMode		mode;														\
	AclResult	aclresult;													\
	bool		is_missing = false;											\
																			\
	roleid = GetUserId();													\
	mode = CONVERT_PRIV(priv, err);											\
	if (*err)																\
		return 0;															\
	aclresult = object_aclcheck_ext_(CLASSID, objoid, roleid, mode,			\
									 &is_missing, err);						\
	if (*err)																\
		return 0;															\
	if (is_missing)															\
	{																		\
		*isnull = 1;														\
		return 0;															\
	}																		\
	return aclresult == ACLCHECK_OK;										\
}

PGQ_DEFINE_OBJECT_FAMILY(database, DatabaseRelationId, convert_database_priv_string)
PGQ_DEFINE_OBJECT_FAMILY(function, ProcedureRelationId, convert_function_priv_string)
PGQ_DEFINE_OBJECT_FAMILY(language, LanguageRelationId, convert_language_priv_string)
PGQ_DEFINE_OBJECT_FAMILY(schema, NamespaceRelationId, convert_schema_priv_string)

/* =====================================================================
 * acl.c — aclinsert / aclremove (verbatim: unconditional ereport)
 * ===================================================================== */
int
pg_aclinsert(int *err)
{
	/* ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	 *		   errmsg("aclinsert is no longer supported")) */
	*err = PGQ_ERR_FEATURE_NOT_SUPPORTED;
	return 0;
}

int
pg_aclremove(int *err)
{
	*err = PGQ_ERR_FEATURE_NOT_SUPPORTED;
	return 0;
}

/* =====================================================================
 * acl.c — acldefault_sql (verbatim switch; result Acl returned by items)
 * ===================================================================== */
int
pg_acldefault_sql(char objtypec, Oid owner, AclItem *items_out, int *nout,
				  int *err)
{
	ObjectType	objtype = (ObjectType) 0;
	static Acl	acl;
	int			i;

	switch (objtypec)
	{
		case 'c':
			objtype = OBJECT_COLUMN;
			break;
		case 'r':
			objtype = OBJECT_TABLE;
			break;
		case 's':
			objtype = OBJECT_SEQUENCE;
			break;
		case 'd':
			objtype = OBJECT_DATABASE;
			break;
		case 'f':
			objtype = OBJECT_FUNCTION;
			break;
		case 'l':
			objtype = OBJECT_LANGUAGE;
			break;
		case 'L':
			objtype = OBJECT_LARGEOBJECT;
			break;
		case 'n':
			objtype = OBJECT_SCHEMA;
			break;
		case 'p':
			objtype = OBJECT_PARAMETER_ACL;
			break;
		case 't':
			objtype = OBJECT_TABLESPACE;
			break;
		case 'F':
			objtype = OBJECT_FDW;
			break;
		case 'S':
			objtype = OBJECT_FOREIGN_SERVER;
			break;
		case 'T':
			objtype = OBJECT_TYPE;
			break;
		default:
			/* elog(ERROR, "unrecognized object type abbreviation: %c", ...) */
			*err = PGQ_ERR_INTERNAL;
			return 0;
	}

	acldefault(objtype, owner, &acl, err);
	if (*err)
		return 0;
	*nout = acl.num;
	for (i = 0; i < PGQ_MAX_ACL; i++)
		items_out[i] = acl.items[i];
	return 1;
}

/* =====================================================================
 * acl.c — getid / putid / aclparse / aclitemin / aclitemout, bodies
 * verbatim (ereturn -> err codes; escontext NULL i.e. hard-error path;
 * WARNING -> no-op; palloc -> caller buffers)
 * ===================================================================== */

static bool
is_safe_acl_char(unsigned char c, bool is_getid)
{
	if (IS_HIGHBIT_SET(c))
		return is_getid;
	return isalnum(c) || c == '_';
}

static const char *
getid(const char *s, char *n, int *err)
{
	int			len = 0;
	bool		in_quotes = false;

	while (isspace((unsigned char) *s))
		s++;
	for (;
		 *s != '\0' &&
		 (in_quotes || *s == '"' || is_safe_acl_char(*s, true));
		 s++)
	{
		if (*s == '"')
		{
			if (!in_quotes)
			{
				in_quotes = true;
				continue;
			}
			/* safe to look at next char (could be '\0' though) */
			if (*(s + 1) != '"')
			{
				in_quotes = false;
				continue;
			}
			/* it's an escaped double quote; skip the escaping char */
			s++;
		}

		/* Add the character to the string */
		if (len >= NAMEDATALEN - 1)
		{
			/* ereturn(escontext, NULL, errcode(ERRCODE_NAME_TOO_LONG), ...) */
			*err = PGQ_ERR_NAME_TOO_LONG;
			return NULL;
		}

		n[len++] = *s;
	}
	n[len] = '\0';
	while (isspace((unsigned char) *s))
		s++;
	return s;
}

static void
putid(char *p, const char *s)
{
	const char *src;
	bool		safe = true;

	/* Detect whether we need to use double quotes */
	for (src = s; *src; src++)
	{
		if (!is_safe_acl_char(*src, false))
		{
			safe = false;
			break;
		}
	}
	if (!safe)
		*p++ = '"';
	for (src = s; *src; src++)
	{
		/* A double quote character in a username is encoded as "" */
		if (*src == '"')
			*p++ = '"';
		*p++ = *src;
	}
	if (!safe)
		*p++ = '"';
	*p = '\0';
}

static const char *
aclparse(const char *s, AclItem *aip, int *err)
{
	AclMode		privs,
				goption,
				read;
	char		name[NAMEDATALEN];
	char		name2[NAMEDATALEN];

	s = getid(s, name, err);
	if (s == NULL)
		return NULL;
	if (*s != '=')
	{
		/* we just read a keyword, not a name */
		if (strcmp(name, "group") != 0 && strcmp(name, "user") != 0)
		{
			/* ereturn(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), ...) */
			*err = PGQ_ERR_INVALID_TEXT_REP;
			return NULL;
		}
		/* move s to the name beyond the keyword */
		s = getid(s, name, err);
		if (s == NULL)
			return NULL;
		if (name[0] == '\0')
		{
			/* errmsg("missing name") */
			*err = PGQ_ERR_INVALID_TEXT_REP;
			return NULL;
		}
	}

	if (*s != '=')
	{
		/* errmsg("missing \"=\" sign") */
		*err = PGQ_ERR_INVALID_TEXT_REP;
		return NULL;
	}

	privs = goption = ACL_NO_RIGHTS;

	for (++s, read = 0; isalpha((unsigned char) *s) || *s == '*'; s++)
	{
		switch (*s)
		{
			case '*':
				goption |= read;
				break;
			case ACL_INSERT_CHR:
				read = ACL_INSERT;
				break;
			case ACL_SELECT_CHR:
				read = ACL_SELECT;
				break;
			case ACL_UPDATE_CHR:
				read = ACL_UPDATE;
				break;
			case ACL_DELETE_CHR:
				read = ACL_DELETE;
				break;
			case ACL_TRUNCATE_CHR:
				read = ACL_TRUNCATE;
				break;
			case ACL_REFERENCES_CHR:
				read = ACL_REFERENCES;
				break;
			case ACL_TRIGGER_CHR:
				read = ACL_TRIGGER;
				break;
			case ACL_EXECUTE_CHR:
				read = ACL_EXECUTE;
				break;
			case ACL_USAGE_CHR:
				read = ACL_USAGE;
				break;
			case ACL_CREATE_CHR:
				read = ACL_CREATE;
				break;
			case ACL_CREATE_TEMP_CHR:
				read = ACL_CREATE_TEMP;
				break;
			case ACL_CONNECT_CHR:
				read = ACL_CONNECT;
				break;
			case ACL_SET_CHR:
				read = ACL_SET;
				break;
			case ACL_ALTER_SYSTEM_CHR:
				read = ACL_ALTER_SYSTEM;
				break;
			case ACL_MAINTAIN_CHR:
				read = ACL_MAINTAIN;
				break;
			default:
				/* errmsg("invalid mode character...") */
				*err = PGQ_ERR_INVALID_TEXT_REP;
				return NULL;
		}

		privs |= read;
	}

	if (name[0] == '\0')
		aip->ai_grantee = ACL_ID_PUBLIC;
	else
	{
		aip->ai_grantee = pg_get_role_oid(name, true, err);
		if (!OidIsValid(aip->ai_grantee))
		{
			/* errcode(ERRCODE_UNDEFINED_OBJECT), role does not exist */
			*err = PGQ_ERR_UNDEFINED_OBJECT;
			return NULL;
		}
	}

	/*
	 * XXX Allow a degree of backward compatibility by defaulting the grantor
	 * to the superuser.
	 */
	if (*s == '/')
	{
		s = getid(s + 1, name2, err);
		if (s == NULL)
			return NULL;
		if (name2[0] == '\0')
		{
			/* errmsg("a name must follow the \"/\" sign") */
			*err = PGQ_ERR_INVALID_TEXT_REP;
			return NULL;
		}
		aip->ai_grantor = pg_get_role_oid(name2, true, err);
		if (!OidIsValid(aip->ai_grantor))
		{
			*err = PGQ_ERR_UNDEFINED_OBJECT;
			return NULL;
		}
	}
	else
	{
		aip->ai_grantor = BOOTSTRAP_SUPERUSERID;
		/* ereport(WARNING, "defaulting grantor to user ID %u") -> no-op
		 * (WARNING emission out of proof on both sides) */
	}

	ACLITEM_SET_PRIVS_GOPTIONS(*aip, privs, goption);

	return s;
}

int
pg_aclitemin(const char *s, AclItem *out, int *err)
{
	AclItem    *aip = out;		/* shim: palloc -> caller storage */

	s = aclparse(s, aip, err);
	if (s == NULL)
		return 0;

	while (isspace((unsigned char) *s))
		++s;
	if (*s)
	{
		/* ereturn: "extra garbage at the end of the ACL specification" */
		*err = PGQ_ERR_INVALID_TEXT_REP;
		return 0;
	}

	return 1;
}

/* sprintf(p, "%u", oid) model for the role-not-found arm (see file header) */
static void
pgq_sprintf_u32(char *p, uint32 v)
{
	char		tmp[10];
	int			n = 0;
	int			i;

	do
	{
		tmp[n++] = (char) ('0' + v % 10);
		v /= 10;
	} while (v != 0);
	for (i = 0; i < n; i++)
		p[i] = tmp[n - 1 - i];
	p[n] = '\0';
}

/* SearchSysCache1(AUTHOID, oid) against seam 7 */
static int
pgq_rname_lookup(Oid roleid)
{
	int			i;

	for (i = 0; i < 2; i++)
		if (pgq_rname_oid[i] == roleid && pgq_rname_found[i])
			return i;
	return -1;
}

int
pg_aclitemout(const AclItem *aip, char *out, int *err)
{
	char	   *p;
	int			slot;
	unsigned	i;

	(void) err;
	p = out;
	*p = '\0';

	if (aip->ai_grantee != ACL_ID_PUBLIC)
	{
		slot = pgq_rname_lookup(aip->ai_grantee);
		if (slot >= 0)			/* HeapTupleIsValid */
		{
			putid(p, pgq_rname_name[slot]);
		}
		else
		{
			/* Generate numeric OID if we don't find an entry */
			pgq_sprintf_u32(p, aip->ai_grantee);
		}
	}
	while (*p)
		++p;

	*p++ = '=';

	for (i = 0; i < N_ACL_RIGHTS; ++i)
	{
		if (ACLITEM_GET_PRIVS(*aip) & (((uint64) 1) << i))
			*p++ = ACL_ALL_RIGHTS_STR[i];
		if (ACLITEM_GET_GOPTIONS(*aip) & (((uint64) 1) << i))
			*p++ = '*';
	}

	*p++ = '/';
	*p = '\0';

	slot = pgq_rname_lookup(aip->ai_grantor);
	if (slot >= 0)
	{
		putid(p, pgq_rname_name[slot]);
	}
	else
	{
		/* Generate numeric OID if we don't find an entry */
		pgq_sprintf_u32(p, aip->ai_grantor);
	}

	while (*p)
		++p;
	return (int) (p - out);
}
