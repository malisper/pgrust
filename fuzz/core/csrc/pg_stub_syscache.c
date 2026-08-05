/*
 * stub:syscache-row — C-oracle side of the shared supplied-catalog-row
 * facility (fuzz/core/src/stub_syscache.rs is the Rust side; the wire format
 * documented there is the contract, and the two decoders are transcriptions
 * of each other — asymmetry is a harness bug, never a divergence).
 *
 * WHAT THIS IS: PostgreSQL's lsyscache layer is hundreds of tiny helpers
 * that each read exactly ONE catalog row.  Supply the row as input and each
 * helper becomes pure over (its arguments, that row).  This TU holds a
 * thread-local row store (loaded from the same wire bytes the Rust side
 * derives), a SearchSysCacheN / GetSysCacheOidN interception layer that
 * answers from the store, and VERBATIM 18.3 lsyscache consumer bodies
 * compiled under that interception — C bodies stay verbatim, the
 * interception is pure preprocessor/shim plumbing.
 *
 * Provenance: FormData_pg_* fixed prefixes and the SECTION-V consumer
 * bodies vendored VERBATIM (comments/BKI annotations elided; pg_proc's
 * trailing oidvector member elided — no consumer here reads past
 * prorettype) from postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (REL_18 "Stamp 18.3", ../pgrust-fabled/vendor/postgres-src)
 * src/include/catalog/pg_{amop,amproc,operator,opclass,type,attribute,
 * proc}.h and src/backend/utils/cache/lsyscache.c.
 *
 * SYMBOL DISCIPLINE: every export carries the pg_stub_syscache_ prefix
 * (verbatim bodies are renamed by #define ahead of the paste, the
 * established stubshims pattern).  nm-census before pushing.
 *
 * CACHE-ID NOTE: 18.3 generates its SysCacheIdentifier enum values at build
 * time (MAKE_SYSCACHE); the NAMES are what the verbatim bodies reference,
 * so the names are load-bearing and the numeric values here are shim-
 * internal dispatch tags.
 *
 * SEMANTICS (identical to the Rust store; part of the compared contract):
 *   - at most PG_STUB_SYSCACHE_MAX_ROWS (16) rows per cache, enforced by
 *     the loader (-3 = clamp violation: the Rust encoder can never produce
 *     it, harness bug, not fuzz surface);
 *   - FIRST matching row wins (duplicate keys are legal fuzz input; both
 *     sides scan in wire order);
 *   - fields NOT carried by the wire (row oids, names other than attname,
 *     pg_proc cost columns, ...) are ZERO in the constructed Form structs
 *     on this side and absent from the Rust shapes — they are NOT covered
 *     by the facility and a consumer reading them needs the row shape
 *     extended BY TABLE (see fuzz/STUBS.md).
 *
 * UNREACHABLE-STATE HAZARD (band-2; documented in fuzz/STUBS.md): a
 * supplied row can be inconsistent with the catalog it was NOT supplied
 * alongside (an amproc row whose amproc oid names no pg_proc row, a type
 * row whose typelem names no type).  Real PostgreSQL can only reach
 * catalog-consistent states, so verdicts over invented rows must be read
 * with care.  Mitigation: the seed corpus / derivation menu is HARVESTED
 * from a live catalog (stub_syscache_harvest.rs) and the constructor is
 * injection-swept.
 */

#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <setjmp.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ---- plumbing typedefs (LP64, same as the other csrc shims) ---- */
typedef uint32_t Oid;
typedef Oid regproc;
typedef regproc RegProcedure;
typedef int16_t int16;
typedef int32_t int32;
typedef int16_t AttrNumber;
typedef uintptr_t Datum;
typedef float float4;

#define InvalidOid ((Oid) 0)
#define NAMEDATALEN 64
typedef struct nameData
{
	char		data[NAMEDATALEN];
} NameData;

#define ObjectIdGetDatum(x) ((Datum) (x))
#define Int16GetDatum(x) ((Datum) (uint16_t) (x))
#define DatumGetObjectId(d) ((Oid) (d))
#define DatumGetInt16(d) ((int16) (uint16_t) (d))
#define CharGetDatum(x) ((Datum) (uint8_t) (x))
#define DatumGetChar(d) ((char) (uint8_t) (d))

/* ---- FormData fixed prefixes, VERBATIM layout (annotations elided) ---- */

typedef struct FormData_pg_amop
{
	Oid			oid;
	Oid			amopfamily;
	Oid			amoplefttype;
	Oid			amoprighttype;
	int16		amopstrategy;
	char		amoppurpose;
	Oid			amopopr;
	Oid			amopmethod;
	Oid			amopsortfamily;
} FormData_pg_amop;
typedef FormData_pg_amop *Form_pg_amop;

typedef struct FormData_pg_amproc
{
	Oid			oid;
	Oid			amprocfamily;
	Oid			amproclefttype;
	Oid			amprocrighttype;
	int16		amprocnum;
	regproc		amproc;
} FormData_pg_amproc;
typedef FormData_pg_amproc *Form_pg_amproc;

typedef struct FormData_pg_operator
{
	Oid			oid;
	NameData	oprname;
	Oid			oprnamespace;
	Oid			oprowner;
	char		oprkind;
	bool		oprcanmerge;
	bool		oprcanhash;
	Oid			oprleft;
	Oid			oprright;
	Oid			oprresult;
	Oid			oprcom;
	Oid			oprnegate;
	regproc		oprcode;
	regproc		oprrest;
	regproc		oprjoin;
} FormData_pg_operator;
typedef FormData_pg_operator *Form_pg_operator;

typedef struct FormData_pg_opclass
{
	Oid			oid;
	Oid			opcmethod;
	NameData	opcname;
	Oid			opcnamespace;
	Oid			opcowner;
	Oid			opcfamily;
	Oid			opcintype;
	bool		opcdefault;
	Oid			opckeytype;
} FormData_pg_opclass;
typedef FormData_pg_opclass *Form_pg_opclass;

typedef struct FormData_pg_type
{
	Oid			oid;
	NameData	typname;
	Oid			typnamespace;
	Oid			typowner;
	int16		typlen;
	bool		typbyval;
	char		typtype;
	char		typcategory;
	bool		typispreferred;
	bool		typisdefined;
	char		typdelim;
	Oid			typrelid;
	regproc		typsubscript;
	Oid			typelem;
	Oid			typarray;
	regproc		typinput;
	regproc		typoutput;
	regproc		typreceive;
	regproc		typsend;
	regproc		typmodin;
	regproc		typmodout;
	regproc		typanalyze;
	char		typalign;
	char		typstorage;
	bool		typnotnull;
	Oid			typbasetype;
	int32		typtypmod;
	int32		typndims;
	Oid			typcollation;
} FormData_pg_type;
typedef FormData_pg_type *Form_pg_type;

typedef struct FormData_pg_attribute
{
	Oid			attrelid;
	NameData	attname;
	Oid			atttypid;
	int16		attlen;
	int16		attnum;
	int32		atttypmod;
	int16		attndims;
	bool		attbyval;
	char		attalign;
	char		attstorage;
	char		attcompression;
	bool		attnotnull;
	bool		atthasdef;
	bool		atthasmissing;
	char		attidentity;
	char		attgenerated;
	bool		attisdropped;
	bool		attislocal;
	int16		attinhcount;
	Oid			attcollation;
} FormData_pg_attribute;
typedef FormData_pg_attribute *Form_pg_attribute;

/* fixed prefix through prorettype; trailing oidvector proargtypes elided
 * (no consumer in this TU reads past prorettype) */
typedef struct FormData_pg_proc
{
	Oid			oid;
	NameData	proname;
	Oid			pronamespace;
	Oid			proowner;
	Oid			prolang;
	float4		procost;
	float4		prorows;
	Oid			provariadic;
	regproc		prosupport;
	char		prokind;
	bool		prosecdef;
	bool		proleakproof;
	bool		proisstrict;
	bool		proretset;
	char		provolatile;
	char		proparallel;
	int16		pronargs;
	int16		pronargdefaults;
	Oid			prorettype;
} FormData_pg_proc;
typedef FormData_pg_proc *Form_pg_proc;

/* ---- the thread-local row store (mirrors SysCacheRows in Rust) ---- */

#define PG_STUB_SYSCACHE_MAX_ROWS 16

typedef struct
{
	Oid			amopfamily;
	Oid			amoplefttype;
	Oid			amoprighttype;
	int16		amopstrategy;
	uint8_t		amoppurpose;
	Oid			amopopr;
	Oid			amopmethod;
	Oid			amopsortfamily;
} StubAmopRow;

typedef struct
{
	Oid			amprocfamily;
	Oid			amproclefttype;
	Oid			amprocrighttype;
	int16		amprocnum;
	Oid			amproc;
} StubAmprocRow;

typedef struct
{
	Oid			oid;
	Oid			oprnamespace;
	Oid			oprleft;
	Oid			oprright;
	Oid			oprresult;
	Oid			oprcom;
	Oid			oprnegate;
	Oid			oprcode;
	Oid			oprrest;
	Oid			oprjoin;
	uint8_t		oprcanmerge;
	uint8_t		oprcanhash;
} StubOperatorRow;

typedef struct
{
	Oid			oid;
	Oid			opcmethod;
	Oid			opcfamily;
	Oid			opcintype;
	Oid			opckeytype;
} StubOpclassRow;

typedef struct
{
	Oid			oid;
	int16		typlen;
	uint8_t		typbyval;
	uint8_t		typalign;
	uint8_t		typstorage;
	Oid			typcollation;
	Oid			typinput;
	Oid			typoutput;
	Oid			typreceive;
	Oid			typsend;
	Oid			typmodin;
	Oid			typmodout;
	Oid			typelem;
	uint8_t		typdelim;
	uint8_t		typisdefined;
} StubTypeRow;

typedef struct
{
	Oid			attrelid;
	int16		attnum;
	uint8_t		attname[NAMEDATALEN];
	Oid			atttypid;
	int32		atttypmod;
	Oid			attcollation;
	uint8_t		attgenerated;
} StubAttributeRow;

typedef struct
{
	Oid			oid;
	Oid			pronamespace;
	Oid			prorettype;
	Oid			provariadic;
	Oid			prosupport;
	Oid			prolang;
	int16		pronargs;
	uint8_t		prokind;
	uint8_t		provolatile;
	uint8_t		proparallel;
	uint8_t		proretset;
	uint8_t		proisstrict;
	uint8_t		proleakproof;
	uint8_t		prosecdef;
	uint8_t		proconfig_isnull;
} StubProcRow;

typedef struct
{
	int			n_amop;
	int			n_amproc;
	int			n_operator;
	int			n_opclass;
	int			n_type;
	int			n_attribute;
	int			n_proc;
	StubAmopRow amop[PG_STUB_SYSCACHE_MAX_ROWS];
	StubAmprocRow amproc[PG_STUB_SYSCACHE_MAX_ROWS];
	StubOperatorRow operator_[PG_STUB_SYSCACHE_MAX_ROWS];
	StubOpclassRow opclass[PG_STUB_SYSCACHE_MAX_ROWS];
	StubTypeRow type_[PG_STUB_SYSCACHE_MAX_ROWS];
	StubAttributeRow attribute[PG_STUB_SYSCACHE_MAX_ROWS];
	StubProcRow proc[PG_STUB_SYSCACHE_MAX_ROWS];
} StubSysCacheStore;

static _Thread_local StubSysCacheStore store;

/* ---- little-endian wire readers/writers (== pg_stub_snapshot.c) ---- */

typedef struct
{
	const uint8_t *b;
	int			len;
	int			i;
	int			short_read;
} StubRd;

static uint8_t
rd_u8(StubRd *r)
{
	if (r->i >= r->len)
	{
		r->short_read = 1;
		return 0;
	}
	return r->b[r->i++];
}

static uint32_t
rd_u32(StubRd *r)
{
	uint32_t	v = 0;

	for (int k = 0; k < 4; k++)
		v |= ((uint32_t) rd_u8(r)) << (8 * k);
	return v;
}

static int16_t
rd_i16(StubRd *r)
{
	uint16_t	v = 0;

	for (int k = 0; k < 2; k++)
		v |= ((uint16_t) rd_u8(r)) << (8 * k);
	return (int16_t) v;
}

typedef struct
{
	uint8_t    *b;
	int			cap;
	int			i;
	int			overflow;
} StubWr;

static void
wr_u8(StubWr *w, uint8_t v)
{
	if (w->i >= w->cap)
	{
		w->overflow = 1;
		return;
	}
	w->b[w->i++] = v;
}

static void
wr_u32(StubWr *w, uint32_t v)
{
	for (int k = 0; k < 4; k++)
		wr_u8(w, (uint8_t) (v >> (8 * k)));
}

static void
wr_i16(StubWr *w, int16_t v)
{
	uint16_t	u = (uint16_t) v;

	for (int k = 0; k < 2; k++)
		wr_u8(w, (uint8_t) (u >> (8 * k)));
}

/* ---- loader: wire -> store (see stub_syscache.rs WIRE; lockstep) ---- */

void
pg_stub_syscache_reset(void)
{
	memset(&store, 0, sizeof(store));
}

static int
rd_count(StubRd *r, int *out)
{
	uint8_t		n = rd_u8(r);

	if (n > PG_STUB_SYSCACHE_MAX_ROWS)
		return -3;
	*out = n;
	return 0;
}

int
pg_stub_syscache_load(const uint8_t *wire, int wirelen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StubRd		rd = {wire, wirelen, 0, 0};

	pg_stub_syscache_reset();

	if (rd_count(&rd, &store.n_amop))
		return -3;
	for (int k = 0; k < store.n_amop; k++)
	{
		StubAmopRow *r = &store.amop[k];

		r->amopfamily = rd_u32(&rd);
		r->amoplefttype = rd_u32(&rd);
		r->amoprighttype = rd_u32(&rd);
		r->amopstrategy = rd_i16(&rd);
		r->amoppurpose = rd_u8(&rd);
		r->amopopr = rd_u32(&rd);
		r->amopmethod = rd_u32(&rd);
		r->amopsortfamily = rd_u32(&rd);
	}
	if (rd_count(&rd, &store.n_amproc))
		return -3;
	for (int k = 0; k < store.n_amproc; k++)
	{
		StubAmprocRow *r = &store.amproc[k];

		r->amprocfamily = rd_u32(&rd);
		r->amproclefttype = rd_u32(&rd);
		r->amprocrighttype = rd_u32(&rd);
		r->amprocnum = rd_i16(&rd);
		r->amproc = rd_u32(&rd);
	}
	if (rd_count(&rd, &store.n_operator))
		return -3;
	for (int k = 0; k < store.n_operator; k++)
	{
		StubOperatorRow *r = &store.operator_[k];

		r->oid = rd_u32(&rd);
		r->oprnamespace = rd_u32(&rd);
		r->oprleft = rd_u32(&rd);
		r->oprright = rd_u32(&rd);
		r->oprresult = rd_u32(&rd);
		r->oprcom = rd_u32(&rd);
		r->oprnegate = rd_u32(&rd);
		r->oprcode = rd_u32(&rd);
		r->oprrest = rd_u32(&rd);
		r->oprjoin = rd_u32(&rd);
		r->oprcanmerge = rd_u8(&rd);
		r->oprcanhash = rd_u8(&rd);
	}
	if (rd_count(&rd, &store.n_opclass))
		return -3;
	for (int k = 0; k < store.n_opclass; k++)
	{
		StubOpclassRow *r = &store.opclass[k];

		r->oid = rd_u32(&rd);
		r->opcmethod = rd_u32(&rd);
		r->opcfamily = rd_u32(&rd);
		r->opcintype = rd_u32(&rd);
		r->opckeytype = rd_u32(&rd);
	}
	if (rd_count(&rd, &store.n_type))
		return -3;
	for (int k = 0; k < store.n_type; k++)
	{
		StubTypeRow *r = &store.type_[k];

		r->oid = rd_u32(&rd);
		r->typlen = rd_i16(&rd);
		r->typbyval = rd_u8(&rd);
		r->typalign = rd_u8(&rd);
		r->typstorage = rd_u8(&rd);
		r->typcollation = rd_u32(&rd);
		r->typinput = rd_u32(&rd);
		r->typoutput = rd_u32(&rd);
		r->typreceive = rd_u32(&rd);
		r->typsend = rd_u32(&rd);
		r->typmodin = rd_u32(&rd);
		r->typmodout = rd_u32(&rd);
		r->typelem = rd_u32(&rd);
		r->typdelim = rd_u8(&rd);
		r->typisdefined = rd_u8(&rd);
	}
	if (rd_count(&rd, &store.n_attribute))
		return -3;
	for (int k = 0; k < store.n_attribute; k++)
	{
		StubAttributeRow *r = &store.attribute[k];

		r->attrelid = rd_u32(&rd);
		r->attnum = rd_i16(&rd);
		for (int j = 0; j < NAMEDATALEN; j++)
			r->attname[j] = rd_u8(&rd);
		r->atttypid = rd_u32(&rd);
		r->atttypmod = (int32) rd_u32(&rd);
		r->attcollation = rd_u32(&rd);
		r->attgenerated = rd_u8(&rd);
	}
	if (rd_count(&rd, &store.n_proc))
		return -3;
	for (int k = 0; k < store.n_proc; k++)
	{
		StubProcRow *r = &store.proc[k];

		r->oid = rd_u32(&rd);
		r->pronamespace = rd_u32(&rd);
		r->prorettype = rd_u32(&rd);
		r->provariadic = rd_u32(&rd);
		r->prosupport = rd_u32(&rd);
		r->prolang = rd_u32(&rd);
		r->pronargs = rd_i16(&rd);
		r->prokind = rd_u8(&rd);
		r->provolatile = rd_u8(&rd);
		r->proparallel = rd_u8(&rd);
		r->proretset = rd_u8(&rd);
		r->proisstrict = rd_u8(&rd);
		r->proleakproof = rd_u8(&rd);
		r->prosecdef = rd_u8(&rd);
		r->proconfig_isnull = rd_u8(&rd);
	}

	if (rd.short_read)
		return -1;
	return 0;
}

/* ---- SECTION-Y: plane writer — serializes the CONSTRUCTED store (never
 * the wire bytes) in the exact order of the Rust ser_syscache_plane, so a
 * construction difference on either side is a caught divergence. ---- */

int
pg_stub_syscache_plane(uint8_t *out, int outcap, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StubWr		wr = {out, outcap, 0, 0};

	wr_u8(&wr, (uint8_t) store.n_amop);
	for (int k = 0; k < store.n_amop; k++)
	{
		const StubAmopRow *r = &store.amop[k];

		wr_u32(&wr, r->amopfamily);
		wr_u32(&wr, r->amoplefttype);
		wr_u32(&wr, r->amoprighttype);
		wr_i16(&wr, r->amopstrategy);
		wr_u8(&wr, r->amoppurpose);
		wr_u32(&wr, r->amopopr);
		wr_u32(&wr, r->amopmethod);
		wr_u32(&wr, r->amopsortfamily);
	}
	wr_u8(&wr, (uint8_t) store.n_amproc);
	for (int k = 0; k < store.n_amproc; k++)
	{
		const StubAmprocRow *r = &store.amproc[k];

		wr_u32(&wr, r->amprocfamily);
		wr_u32(&wr, r->amproclefttype);
		wr_u32(&wr, r->amprocrighttype);
		wr_i16(&wr, r->amprocnum);
		wr_u32(&wr, r->amproc);
	}
	wr_u8(&wr, (uint8_t) store.n_operator);
	for (int k = 0; k < store.n_operator; k++)
	{
		const StubOperatorRow *r = &store.operator_[k];

		wr_u32(&wr, r->oid);
		wr_u32(&wr, r->oprnamespace);
		wr_u32(&wr, r->oprleft);
		wr_u32(&wr, r->oprright);
		wr_u32(&wr, r->oprresult);
		wr_u32(&wr, r->oprcom);
		wr_u32(&wr, r->oprnegate);
		wr_u32(&wr, r->oprcode);
		wr_u32(&wr, r->oprrest);
		wr_u32(&wr, r->oprjoin);
		wr_u8(&wr, r->oprcanmerge);
		wr_u8(&wr, r->oprcanhash);
	}
	wr_u8(&wr, (uint8_t) store.n_opclass);
	for (int k = 0; k < store.n_opclass; k++)
	{
		const StubOpclassRow *r = &store.opclass[k];

		wr_u32(&wr, r->oid);
		wr_u32(&wr, r->opcmethod);
		wr_u32(&wr, r->opcfamily);
		wr_u32(&wr, r->opcintype);
		wr_u32(&wr, r->opckeytype);
	}
	wr_u8(&wr, (uint8_t) store.n_type);
	for (int k = 0; k < store.n_type; k++)
	{
		const StubTypeRow *r = &store.type_[k];

		wr_u32(&wr, r->oid);
		wr_i16(&wr, r->typlen);
		wr_u8(&wr, r->typbyval);
		wr_u8(&wr, r->typalign);
		wr_u8(&wr, r->typstorage);
		wr_u32(&wr, r->typcollation);
		wr_u32(&wr, r->typinput);
		wr_u32(&wr, r->typoutput);
		wr_u32(&wr, r->typreceive);
		wr_u32(&wr, r->typsend);
		wr_u32(&wr, r->typmodin);
		wr_u32(&wr, r->typmodout);
		wr_u32(&wr, r->typelem);
		wr_u8(&wr, r->typdelim);
		wr_u8(&wr, r->typisdefined);
	}
	wr_u8(&wr, (uint8_t) store.n_attribute);
	for (int k = 0; k < store.n_attribute; k++)
	{
		const StubAttributeRow *r = &store.attribute[k];

		wr_u32(&wr, r->attrelid);
		wr_i16(&wr, r->attnum);
		for (int j = 0; j < NAMEDATALEN; j++)
			wr_u8(&wr, r->attname[j]);
		wr_u32(&wr, r->atttypid);
		wr_u32(&wr, (uint32_t) r->atttypmod);
		wr_u32(&wr, r->attcollation);
		wr_u8(&wr, r->attgenerated);
	}
	wr_u8(&wr, (uint8_t) store.n_proc);
	for (int k = 0; k < store.n_proc; k++)
	{
		const StubProcRow *r = &store.proc[k];

		wr_u32(&wr, r->oid);
		wr_u32(&wr, r->pronamespace);
		wr_u32(&wr, r->prorettype);
		wr_u32(&wr, r->provariadic);
		wr_u32(&wr, r->prosupport);
		wr_u32(&wr, r->prolang);
		wr_i16(&wr, r->pronargs);
		wr_u8(&wr, r->prokind);
		wr_u8(&wr, r->provolatile);
		wr_u8(&wr, r->proparallel);
		wr_u8(&wr, r->proretset);
		wr_u8(&wr, r->proisstrict);
		wr_u8(&wr, r->proleakproof);
		wr_u8(&wr, r->prosecdef);
		wr_u8(&wr, r->proconfig_isnull);
	}

	if (wr.overflow)
		return -2;
	*outlen = wr.i;
	return 0;
}

/* ---- SearchSysCacheN / GetSysCacheOidN interception ----
 *
 * A "tuple" here is a StubTuple whose form pointer targets a thread-local
 * FormData_pg_* slot filled from the FIRST matching store row (uncovered
 * Form fields zeroed).  One live probe per cache at a time (per-cache
 * slots; the lsyscache single-row-probe family never nests within one
 * cache).  ReleaseSysCache is a no-op.
 */

typedef struct StubTuple
{
	void	   *form;
} StubTuple;

typedef StubTuple *HeapTuple;

#define HeapTupleIsValid(t) ((t) != NULL)
#define GETSTRUCT(t) (((StubTuple *) (t))->form)
#define ReleaseSysCache(t) pg_stub_syscache_release(t)

void
pg_stub_syscache_release(HeapTuple t)
{
	(void) t;
}

/* shim-internal dispatch tags for the verbatim bodies' cache-id NAMES */
enum
{
	AMOPSTRATEGY = 1,
	AMOPOPID,
	AMPROCNUM,
	OPEROID,
	CLAOID,
	TYPEOID,
	ATTNUM,
	PROCOID,
};

static _Thread_local FormData_pg_amop amop_slot;
static _Thread_local FormData_pg_amproc amproc_slot;
static _Thread_local FormData_pg_operator operator_slot;
static _Thread_local FormData_pg_opclass opclass_slot;
static _Thread_local FormData_pg_type type_slot;
static _Thread_local FormData_pg_attribute attribute_slot;
static _Thread_local FormData_pg_proc proc_slot;
static _Thread_local StubTuple tuple_slot;

static HeapTuple
mk_tuple(void *form)
{
	tuple_slot.form = form;
	return &tuple_slot;
}

HeapTuple
pg_stub_syscache_search(int cacheId, Datum k1, Datum k2, Datum k3, Datum k4)
{
	switch (cacheId)
	{
		case AMOPSTRATEGY:
			for (int k = 0; k < store.n_amop; k++)
			{
				const StubAmopRow *r = &store.amop[k];

				if (r->amopfamily == DatumGetObjectId(k1) &&
					r->amoplefttype == DatumGetObjectId(k2) &&
					r->amoprighttype == DatumGetObjectId(k3) &&
					r->amopstrategy == DatumGetInt16(k4))
				{
					memset(&amop_slot, 0, sizeof(amop_slot));
					amop_slot.amopfamily = r->amopfamily;
					amop_slot.amoplefttype = r->amoplefttype;
					amop_slot.amoprighttype = r->amoprighttype;
					amop_slot.amopstrategy = r->amopstrategy;
					amop_slot.amoppurpose = (char) r->amoppurpose;
					amop_slot.amopopr = r->amopopr;
					amop_slot.amopmethod = r->amopmethod;
					amop_slot.amopsortfamily = r->amopsortfamily;
					return mk_tuple(&amop_slot);
				}
			}
			return NULL;
		case AMOPOPID:
			/* keys: (amopopr, amoppurpose, amopfamily) */
			for (int k = 0; k < store.n_amop; k++)
			{
				const StubAmopRow *r = &store.amop[k];

				if (r->amopopr == DatumGetObjectId(k1) &&
					(char) r->amoppurpose == DatumGetChar(k2) &&
					r->amopfamily == DatumGetObjectId(k3))
				{
					memset(&amop_slot, 0, sizeof(amop_slot));
					amop_slot.amopfamily = r->amopfamily;
					amop_slot.amoplefttype = r->amoplefttype;
					amop_slot.amoprighttype = r->amoprighttype;
					amop_slot.amopstrategy = r->amopstrategy;
					amop_slot.amoppurpose = (char) r->amoppurpose;
					amop_slot.amopopr = r->amopopr;
					amop_slot.amopmethod = r->amopmethod;
					amop_slot.amopsortfamily = r->amopsortfamily;
					return mk_tuple(&amop_slot);
				}
			}
			return NULL;
		case AMPROCNUM:
			for (int k = 0; k < store.n_amproc; k++)
			{
				const StubAmprocRow *r = &store.amproc[k];

				if (r->amprocfamily == DatumGetObjectId(k1) &&
					r->amproclefttype == DatumGetObjectId(k2) &&
					r->amprocrighttype == DatumGetObjectId(k3) &&
					r->amprocnum == DatumGetInt16(k4))
				{
					memset(&amproc_slot, 0, sizeof(amproc_slot));
					amproc_slot.amprocfamily = r->amprocfamily;
					amproc_slot.amproclefttype = r->amproclefttype;
					amproc_slot.amprocrighttype = r->amprocrighttype;
					amproc_slot.amprocnum = r->amprocnum;
					amproc_slot.amproc = r->amproc;
					return mk_tuple(&amproc_slot);
				}
			}
			return NULL;
		case OPEROID:
			for (int k = 0; k < store.n_operator; k++)
			{
				const StubOperatorRow *r = &store.operator_[k];

				if (r->oid == DatumGetObjectId(k1))
				{
					memset(&operator_slot, 0, sizeof(operator_slot));
					operator_slot.oid = r->oid;
					operator_slot.oprnamespace = r->oprnamespace;
					operator_slot.oprcanmerge = r->oprcanmerge != 0;
					operator_slot.oprcanhash = r->oprcanhash != 0;
					operator_slot.oprleft = r->oprleft;
					operator_slot.oprright = r->oprright;
					operator_slot.oprresult = r->oprresult;
					operator_slot.oprcom = r->oprcom;
					operator_slot.oprnegate = r->oprnegate;
					operator_slot.oprcode = r->oprcode;
					operator_slot.oprrest = r->oprrest;
					operator_slot.oprjoin = r->oprjoin;
					return mk_tuple(&operator_slot);
				}
			}
			return NULL;
		case CLAOID:
			for (int k = 0; k < store.n_opclass; k++)
			{
				const StubOpclassRow *r = &store.opclass[k];

				if (r->oid == DatumGetObjectId(k1))
				{
					memset(&opclass_slot, 0, sizeof(opclass_slot));
					opclass_slot.oid = r->oid;
					opclass_slot.opcmethod = r->opcmethod;
					opclass_slot.opcfamily = r->opcfamily;
					opclass_slot.opcintype = r->opcintype;
					opclass_slot.opckeytype = r->opckeytype;
					return mk_tuple(&opclass_slot);
				}
			}
			return NULL;
		case TYPEOID:
			for (int k = 0; k < store.n_type; k++)
			{
				const StubTypeRow *r = &store.type_[k];

				if (r->oid == DatumGetObjectId(k1))
				{
					memset(&type_slot, 0, sizeof(type_slot));
					type_slot.oid = r->oid;
					type_slot.typlen = r->typlen;
					type_slot.typbyval = r->typbyval != 0;
					type_slot.typisdefined = r->typisdefined != 0;
					type_slot.typdelim = (char) r->typdelim;
					type_slot.typelem = r->typelem;
					type_slot.typinput = r->typinput;
					type_slot.typoutput = r->typoutput;
					type_slot.typreceive = r->typreceive;
					type_slot.typsend = r->typsend;
					type_slot.typmodin = r->typmodin;
					type_slot.typmodout = r->typmodout;
					type_slot.typalign = (char) r->typalign;
					type_slot.typstorage = (char) r->typstorage;
					type_slot.typcollation = r->typcollation;
					return mk_tuple(&type_slot);
				}
			}
			return NULL;
		case ATTNUM:
			for (int k = 0; k < store.n_attribute; k++)
			{
				const StubAttributeRow *r = &store.attribute[k];

				if (r->attrelid == DatumGetObjectId(k1) &&
					r->attnum == DatumGetInt16(k2))
				{
					memset(&attribute_slot, 0, sizeof(attribute_slot));
					attribute_slot.attrelid = r->attrelid;
					memcpy(attribute_slot.attname.data, r->attname, NAMEDATALEN);
					attribute_slot.atttypid = r->atttypid;
					attribute_slot.attnum = r->attnum;
					attribute_slot.atttypmod = r->atttypmod;
					attribute_slot.attgenerated = (char) r->attgenerated;
					attribute_slot.attcollation = r->attcollation;
					return mk_tuple(&attribute_slot);
				}
			}
			return NULL;
		case PROCOID:
			for (int k = 0; k < store.n_proc; k++)
			{
				const StubProcRow *r = &store.proc[k];

				if (r->oid == DatumGetObjectId(k1))
				{
					memset(&proc_slot, 0, sizeof(proc_slot));
					proc_slot.oid = r->oid;
					proc_slot.pronamespace = r->pronamespace;
					proc_slot.prolang = r->prolang;
					proc_slot.provariadic = r->provariadic;
					proc_slot.prosupport = r->prosupport;
					proc_slot.prokind = (char) r->prokind;
					proc_slot.prosecdef = r->prosecdef != 0;
					proc_slot.proleakproof = r->proleakproof != 0;
					proc_slot.proisstrict = r->proisstrict != 0;
					proc_slot.proretset = r->proretset != 0;
					proc_slot.provolatile = (char) r->provolatile;
					proc_slot.proparallel = (char) r->proparallel;
					proc_slot.pronargs = r->pronargs;
					proc_slot.prorettype = r->prorettype;
					return mk_tuple(&proc_slot);
				}
			}
			return NULL;
		default:
			return NULL;
	}
}

#define SearchSysCache1(id, k1) pg_stub_syscache_search(id, k1, 0, 0, 0)
#define SearchSysCache2(id, k1, k2) pg_stub_syscache_search(id, k1, k2, 0, 0)
#define SearchSysCache3(id, k1, k2, k3) pg_stub_syscache_search(id, k1, k2, k3, 0)
#define SearchSysCache4(id, k1, k2, k3, k4) pg_stub_syscache_search(id, k1, k2, k3, k4)

/* GetSysCacheOidN leg: search, then project the row's oid column (only the
 * oid-bearing caches here answer non-zero; the oidcol argument of the real
 * macro is dropped — every covered cache stores its oid first). */
Oid
pg_stub_syscache_getoid(int cacheId, Datum k1, Datum k2, Datum k3, Datum k4)
{
	HeapTuple	tp = pg_stub_syscache_search(cacheId, k1, k2, k3, k4);

	if (!HeapTupleIsValid(tp))
		return InvalidOid;
	switch (cacheId)
	{
		case OPEROID:
			return ((Form_pg_operator) GETSTRUCT(tp))->oid;
		case CLAOID:
			return ((Form_pg_opclass) GETSTRUCT(tp))->oid;
		case TYPEOID:
			return ((Form_pg_type) GETSTRUCT(tp))->oid;
		case PROCOID:
			return ((Form_pg_proc) GETSTRUCT(tp))->oid;
		default:
			return InvalidOid;	/* rows without a stored oid: not covered */
	}
}

#define GetSysCacheOid1(id, oidcol, k1) pg_stub_syscache_getoid(id, k1, 0, 0, 0)
#define GetSysCacheOid2(id, oidcol, k1, k2) pg_stub_syscache_getoid(id, k1, k2, 0, 0)
#define GetSysCacheOid3(id, oidcol, k1, k2, k3) pg_stub_syscache_getoid(id, k1, k2, k3, 0)
#define GetSysCacheOid4(id, oidcol, k1, k2, k3, k4) pg_stub_syscache_getoid(id, k1, k2, k3, k4)

/* ---- elog shim: the miss paths of get_typlenbyval/get_func_rettype/
 * get_opclass_family elog(ERROR, "cache lookup failed for ...").  The try_
 * wrappers arm a recovery point and report the elog as status 1 (the same
 * "cache lookup failed" class the Rust PgError carries). ---- */

static _Thread_local jmp_buf pg_stub_syscache_jmp;
static _Thread_local int pg_stub_syscache_jmp_armed;

static void
pg_stub_syscache_elog_raise(void)
{
	if (pg_stub_syscache_jmp_armed)
		longjmp(pg_stub_syscache_jmp, 1);
	abort();					/* elog outside a try_ wrapper: harness bug */
}

#define elog(level, ...) pg_stub_syscache_elog_raise()
#define ERROR 21				/* consumed by the elog macro only */

/* ---- SECTION-V: VERBATIM 18.3 lsyscache.c consumer bodies, compiled
 * under the interception above.  Renamed to prefixed exports by #define
 * (the established stubshims pattern); bodies unedited. ---- */

#define get_opfamily_proc pg_stub_syscache_get_opfamily_proc
#define get_opfamily_member pg_stub_syscache_get_opfamily_member
#define get_opcode pg_stub_syscache_get_opcode
#define get_opclass_family pg_stub_syscache_get_opclass_family
#define get_typlenbyval pg_stub_syscache_get_typlenbyval
#define get_atttype pg_stub_syscache_get_atttype
#define get_func_rettype pg_stub_syscache_get_func_rettype

Oid
get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16 procnum)
{
	HeapTuple	tp;
	Form_pg_amproc amproc_tup;
	RegProcedure result;

	tp = SearchSysCache4(AMPROCNUM,
						 ObjectIdGetDatum(opfamily),
						 ObjectIdGetDatum(lefttype),
						 ObjectIdGetDatum(righttype),
						 Int16GetDatum(procnum));
	if (!HeapTupleIsValid(tp))
		return InvalidOid;
	amproc_tup = (Form_pg_amproc) GETSTRUCT(tp);
	result = amproc_tup->amproc;
	ReleaseSysCache(tp);
	return result;
}

Oid
get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype,
					int16 strategy)
{
	HeapTuple	tp;
	Form_pg_amop amop_tup;
	Oid			result;

	tp = SearchSysCache4(AMOPSTRATEGY,
						 ObjectIdGetDatum(opfamily),
						 ObjectIdGetDatum(lefttype),
						 ObjectIdGetDatum(righttype),
						 Int16GetDatum(strategy));
	if (!HeapTupleIsValid(tp))
		return InvalidOid;
	amop_tup = (Form_pg_amop) GETSTRUCT(tp);
	result = amop_tup->amopopr;
	ReleaseSysCache(tp);
	return result;
}

RegProcedure
get_opcode(Oid opno)
{
	HeapTuple	tp;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_operator optup = (Form_pg_operator) GETSTRUCT(tp);
		RegProcedure result;

		result = optup->oprcode;
		ReleaseSysCache(tp);
		return result;
	}
	else
		return (RegProcedure) InvalidOid;
}

Oid
get_opclass_family(Oid opclass)
{
	HeapTuple	tp;
	Form_pg_opclass cla_tup;
	Oid			result;

	tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);
	cla_tup = (Form_pg_opclass) GETSTRUCT(tp);

	result = cla_tup->opcfamily;
	ReleaseSysCache(tp);
	return result;
}

void
get_typlenbyval(Oid typid, int16 *typlen, bool *typbyval)
{
	HeapTuple	tp;
	Form_pg_type typtup;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for type %u", typid);
	typtup = (Form_pg_type) GETSTRUCT(tp);
	*typlen = typtup->typlen;
	*typbyval = typtup->typbyval;
	ReleaseSysCache(tp);
}

Oid
get_atttype(Oid relid, AttrNumber attnum)
{
	HeapTuple	tp;

	tp = SearchSysCache2(ATTNUM,
						 ObjectIdGetDatum(relid),
						 Int16GetDatum(attnum));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
		Oid			result;

		result = att_tup->atttypid;
		ReleaseSysCache(tp);
		return result;
	}
	else
		return InvalidOid;
}

Oid
get_func_rettype(Oid funcid)
{
	HeapTuple	tp;
	Oid			result;

	tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for function %u", funcid);

	result = ((Form_pg_proc) GETSTRUCT(tp))->prorettype;
	ReleaseSysCache(tp);
	return result;
}

/* ---- status wrappers for the elog-on-miss consumers (0 ok / 1 = the
 * "cache lookup failed" elog fired) ---- */

int
pg_stub_syscache_try_get_opclass_family(Oid opclass, Oid *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_syscache_jmp_armed = 1;
	if (setjmp(pg_stub_syscache_jmp) != 0)
	{
		pg_stub_syscache_jmp_armed = 0;
		return 1;
	}
	*out = pg_stub_syscache_get_opclass_family(opclass);
	pg_stub_syscache_jmp_armed = 0;
	return 0;
}

int
pg_stub_syscache_try_get_typlenbyval(Oid typid, int16 *typlen, uint8_t *typbyval)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		byval = false;

	pg_stub_syscache_jmp_armed = 1;
	if (setjmp(pg_stub_syscache_jmp) != 0)
	{
		pg_stub_syscache_jmp_armed = 0;
		return 1;
	}
	pg_stub_syscache_get_typlenbyval(typid, typlen, &byval);
	pg_stub_syscache_jmp_armed = 0;
	*typbyval = byval ? 1 : 0;
	return 0;
}

int
pg_stub_syscache_try_get_func_rettype(Oid funcid, Oid *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stub_syscache_jmp_armed = 1;
	if (setjmp(pg_stub_syscache_jmp) != 0)
	{
		pg_stub_syscache_jmp_armed = 0;
		return 1;
	}
	*out = pg_stub_syscache_get_func_rettype(funcid);
	pg_stub_syscache_jmp_armed = 0;
	return 0;
}
