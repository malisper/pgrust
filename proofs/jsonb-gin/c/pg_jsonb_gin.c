/*
 * Vendored PostgreSQL C for the jsonb GIN opclass proofs (proofs/jsonb-gin):
 * gin_extract_jsonb (3482), gin_extract_jsonb_query (3483),
 * gin_consistent_jsonb (3484), gin_extract_jsonb_path (3485),
 * gin_extract_jsonb_query_path (3486), gin_consistent_jsonb_path (3487),
 * gin_triconsistent_jsonb (3488), gin_triconsistent_jsonb_path (3489).
 *
 * Provenance (all REL_18_STABLE, fetched 2026-07-28 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/backend/utils/adt/jsonb_gin.c:
 *       GinEntries, init_gin_entries, add_gin_entry, JsonPathGinNodeType,
 *       JsonPathGinNode, make_jsp_entry_node, make_jsp_expr_node,
 *       make_jsp_expr_node_binary, execute_jsp_gin_node, make_text_key,
 *       make_scalar_key, gin_extract_jsonb, gin_extract_jsonb_path,
 *       gin_consistent_jsonb, gin_triconsistent_jsonb,
 *       gin_consistent_jsonb_path, gin_triconsistent_jsonb_path
 *   - src/backend/utils/adt/jsonb_util.c:
 *       getJsonbOffset, getJsonbLength, fillJsonbValue, JsonbIteratorInit,
 *       JsonbIteratorNext, iteratorFromContainer, freeAndGetParent,
 *       JsonbHashScalarValue
 *       (iterator block copied from proofs/jsonb-probe/c/pg_jsonb.c, which
 *       vendored it verbatim from the same ref — including its documented
 *       typed-iterator-slot shim)
 *   - src/include/utils/jsonb.h: JEntry/JsonbContainer/JsonbValue/
 *       JsonbIterator declarations, JB_/JBE_ macros, the jsonb GIN strategy
 *       numbers and JGINFLAG_/JGIN_MAXLENGTH defines, verbatim.
 *   - src/include/access/gin.h: GinTernaryValue + GIN_FALSE/TRUE/MAYBE,
 *       GIN_SEARCH_MODE_* values, verbatim.
 *   - src/include/port/pg_bitutils.h: pg_rotate_left32, verbatim.
 *
 * FORMAT INVARIANT / FENCE: container images come from the same trusted
 * builder as proofs/jsonb-probe (see that file's header): layout-valid
 * jsonb containers, scalar leaves fenced to null/bool/string (NUMERIC
 * IMAGES OUT OF FENCE — the make_scalar_key numeric arm aborts), string
 * lengths <= 3 (so the make_text_key JGINFLAG_HASHED arm, len > 125, is
 * dead and aborts if reached).
 *
 * SHIMS (everything else is verbatim; function names pg_/pgg_-prefixed):
 *   - typedefs/Assert via ../../support/c/pg_proof_shim.h.
 *   - palloc/palloc0/repalloc/pfree -> fixed static bump pool (pgg_reset()
 *     rewinds; budget fence via __CPROVER_assume, jsonb-probe precedent).
 *     Allocation strategy is harness plumbing, never part of the claim.
 *   - elog(ERROR, ...) -> pg_proof_abort = 1 and continue; every elog site
 *     must be unreachable under the harness fence and each harness asserts
 *     pgg_take_abort() == 0.
 *   - JsonbIterator allocation -> typed named-static slots (copied shim
 *     from jsonb-probe: raw-pool iterator bytes defeat CBMC field
 *     sensitivity and explode the formula).
 *   - hash_any (JsonbHashScalarValue jbvString arm) -> SHARED SEAM MODEL
 *     pg_seam_hash_bytes (FNV-1a 32): the Rust side of a hash-bearing
 *     harness stubs hashfn::hash_bytes with the IDENTICAL model, so hash
 *     internals leave the proof on both sides identically (hashfn itself
 *     is a separately-PROVED family; dt-minmax shared-seam precedent).
 *     pgg_set_hash_skew(1) offsets the C side for the skew control.
 *   - make_text_key hashed arm: hash_any -> seam model, snprintf -> abort
 *     macro (arm is dead under the len <= 3 fence).
 *   - make_scalar_key numeric arm: numeric_normalize -> abort + "" (numeric
 *     out of fence).
 *   - DirectFunctionCall1(hash_numeric,..) (JsonbHashScalarValue numeric
 *     arm) -> abort + 0 (numeric out of fence).
 *   - fmgr unwrapping: PG_GETARG_JSONB_P(0) -> `const JsonbContainer *`
 *     param (pre-detoasted container payload, bytea-cmp precedent;
 *     JB_ROOT_COUNT(jb) -> root->header & JB_CMASK, same field);
 *     PG_GETARG_POINTER/UINT16/INT32 -> plain params; extra_data[0] ->
 *     JsonPathGinNode * param wrapped in a local Pointer[1];
 *     PG_RETURN_BOOL/POINTER/GIN_TERNARY_VALUE -> int return + statics.
 *   - SET_VARSIZE/VARDATA over a minimal little-endian 4-byte-header text
 *     struct (matches types_tuple::varatt::set_varsize_4b_word on this
 *     target: len << 2).
 *   - node-tree builders pgg_mk_entry/pgg_mk_expr2/pgg_mk_expr3 (harness
 *     scaffolding): assemble JsonPathGinNode trees via the verbatim
 *     make_jsp_* constructors; entry nodes carry val.entryIndex directly
 *     (the post-emit_jsp_gin_entries state in which consistent runs).
 */

#include <stddef.h>
#include <string.h>
#include "../../support/c/pg_proof_shim.h"

/* ---------------- harness plumbing (not under proof) ---------------- */

static int pg_proof_abort = 0;

/* elog/ereport shim: record and continue (sites must be fenced out). */
#define elog(level, ...) (pg_proof_abort = 1)
#define ereport(level, rest) (pg_proof_abort = 1)

/* dead-arm snprintf (make_text_key hashed arm; len > 125 is out of fence) */
#define snprintf(buf, size, fmt, val) (pg_proof_abort = 1)

/* static bump pool standing in for palloc (harness plumbing only) */
#define PGG_POOL_CAP 4096
static unsigned char pgg_pool[PGG_POOL_CAP] __attribute__((aligned(8)));
static size_t pgg_pool_next = 0;

/* CBMC's assume primitive (available under kani -Z c-ffi). */
void		__CPROVER_assume(int cond);

static void *
palloc(Size size)
{
	size_t		start = (pgg_pool_next + 7) & ~(size_t) 7;

	/* Harness budget fence, NOT logic (jsonb-probe rationale: a merging
	 * overflow arm poisons downstream pointer value-sets). Sufficiency is
	 * witnessed by the harnesses' entry-count/content assertions. */
	__CPROVER_assume(start + size <= PGG_POOL_CAP);
	pgg_pool_next = start + size;
	return pgg_pool + start;
}

static void *
palloc0(Size size)
{
	void	   *p = palloc(size);

	memset(p, 0, size);
	return p;
}

static void *
repalloc(void *p, Size size)
{
	/* bump-pool grow: fresh block, copy is the caller's data (GinEntries
	 * doubling only; old size <= new size always holds there) */
	void	   *np = palloc(size);

	memcpy(np, p, size);
	return np;
}

#define pfree(p) ((void) 0)

typedef char *Pointer;
typedef uintptr_t Datum;
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetPointer(X) ((Pointer) (X))
#define UInt32GetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))

/* access/stratnum.h */
typedef uint16 StrategyNumber;

/* access/gin.h (verbatim values) */
typedef char GinTernaryValue;
#define GIN_FALSE		0
#define GIN_TRUE		1
#define GIN_MAYBE		2
#define GIN_SEARCH_MODE_DEFAULT			0
#define GIN_SEARCH_MODE_INCLUDE_EMPTY	1
#define GIN_SEARCH_MODE_ALL				2

/* utils/jsonb.h (verbatim values) */
#define JsonbContainsStrategyNumber		7
#define JsonbExistsStrategyNumber		9
#define JsonbExistsAnyStrategyNumber	10
#define JsonbExistsAllStrategyNumber	11
#define JsonbJsonpathExistsStrategyNumber		15
#define JsonbJsonpathPredicateStrategyNumber	16

#define JGINFLAG_KEY	0x01	/* key (or string array element) */
#define JGINFLAG_NULL	0x02	/* null value */
#define JGINFLAG_BOOL	0x03	/* boolean value */
#define JGINFLAG_NUM	0x04	/* numeric value */
#define JGINFLAG_STR	0x05	/* string value (if not an array element) */
#define JGINFLAG_HASHED 0x10	/* OR'd into flag if value was hashed */
#define JGIN_MAXLENGTH	125		/* max length of text part before hashing */

/* ---- minimal LE 4-byte-header text varlena (SET_VARSIZE/VARDATA shim) --- */
typedef struct text
{
	uint32		vl_len_;
	char		vl_dat[];
} text;

#define SET_VARSIZE(PTR, len) (((text *) (PTR))->vl_len_ = ((uint32) (len)) << 2)
#define VARSIZE(PTR) (((const text *) (PTR))->vl_len_ >> 2)
#define VARDATA(PTR) (((text *) (PTR))->vl_dat)

/* ---------------- jsonb.h declarations (verbatim) ---------------- */

typedef uint32 JEntry;

#define JENTRY_OFFLENMASK		0x0FFFFFFF
#define JENTRY_TYPEMASK			0x70000000
#define JENTRY_HAS_OFF			0x80000000

#define JENTRY_ISSTRING			0x00000000
#define JENTRY_ISNUMERIC		0x10000000
#define JENTRY_ISBOOL_FALSE		0x20000000
#define JENTRY_ISBOOL_TRUE		0x30000000
#define JENTRY_ISNULL			0x40000000
#define JENTRY_ISCONTAINER		0x50000000

#define JBE_OFFLENFLD(je_)		((je_) & JENTRY_OFFLENMASK)
#define JBE_HAS_OFF(je_)		(((je_) & JENTRY_HAS_OFF) != 0)
#define JBE_ISSTRING(je_)		(((je_) & JENTRY_TYPEMASK) == JENTRY_ISSTRING)
#define JBE_ISNUMERIC(je_)		(((je_) & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC)
#define JBE_ISCONTAINER(je_)	(((je_) & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER)
#define JBE_ISNULL(je_)			(((je_) & JENTRY_TYPEMASK) == JENTRY_ISNULL)
#define JBE_ISBOOL_TRUE(je_)	(((je_) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE)
#define JBE_ISBOOL_FALSE(je_)	(((je_) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE)

#define JBE_ADVANCE_OFFSET(offset, je) \
	do { \
		JEntry	je_ = (je); \
		if (JBE_HAS_OFF(je_)) \
			(offset) = JBE_OFFLENFLD(je_); \
		else \
			(offset) += JBE_OFFLENFLD(je_); \
	} while(0)

typedef struct JsonbContainer
{
	uint32		header;
	JEntry		children[];		/* FLEXIBLE_ARRAY_MEMBER */
} JsonbContainer;

#define JB_CMASK				0x0FFFFFFF
#define JB_FSCALAR				0x10000000
#define JB_FOBJECT				0x20000000
#define JB_FARRAY				0x40000000

#define JsonContainerSize(jc)		((jc)->header & JB_CMASK)
#define JsonContainerIsScalar(jc)	(((jc)->header & JB_FSCALAR) != 0)
#define JsonContainerIsObject(jc)	(((jc)->header & JB_FOBJECT) != 0)
#define JsonContainerIsArray(jc)	(((jc)->header & JB_FARRAY) != 0)

enum jbvType
{
	jbvNull = 0x0,
	jbvString,
	jbvNumeric,
	jbvBool,
	jbvArray = 0x10,
	jbvObject,
	jbvBinary,
	jbvDatetime = 0x20,
};

typedef void *Numeric;			/* shim: opaque; numeric values are fenced out */

typedef struct JsonbValue JsonbValue;

struct JsonbValue
{
	enum jbvType type;
	union
	{
		Numeric		numeric;
		bool		boolean;
		struct
		{
			int			len;
			char	   *val;	/* Not necessarily null-terminated */
		}			string;
		struct
		{
			int			nElems;
			JsonbValue *elems;
			bool		rawScalar;
		}			array;
		struct
		{
			int			nPairs;
			void	   *pairs;	/* shim: JsonbPair unused here */
		}			object;
		struct
		{
			int			len;
			JsonbContainer *data;
		}			binary;
	}			val;
};

#define IsAJsonbScalar(jsonbval)	((jsonbval)->type >= jbvNull && \
									 (jsonbval)->type <= jbvBool)

typedef enum
{
	JBI_ARRAY_START,
	JBI_ARRAY_ELEM,
	JBI_OBJECT_START,
	JBI_OBJECT_KEY,
	JBI_OBJECT_VALUE,
} JsonbIterState;

typedef struct JsonbIterator
{
	JsonbContainer *container;
	uint32		nElems;
	bool		isScalar;
	JEntry	   *children;
	char	   *dataProper;
	int			curIndex;
	uint32		curDataOffset;
	uint32		curValueOffset;
	JsonbIterState state;
	struct JsonbIterator *parent;
} JsonbIterator;

typedef enum
{
	WJB_DONE,
	WJB_KEY,
	WJB_VALUE,
	WJB_ELEM,
	WJB_BEGIN_ARRAY,
	WJB_END_ARRAY,
	WJB_BEGIN_OBJECT,
	WJB_END_OBJECT,
} JsonbIteratorToken;

#define INTALIGN(LEN) (((uintptr_t) (LEN) + 3) & ~(uintptr_t) 3)

/* forward decls */
static void pg_fillJsonbValue(JsonbContainer *container, int index,
							  char *base_addr, uint32 offset,
							  JsonbValue *result);
static JsonbIterator *pg_iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent);
static JsonbIterator *pg_freeAndGetParent(JsonbIterator *it);
static JsonbIterator *pg_JsonbIteratorInit(JsonbContainer *container);
static JsonbIteratorToken pg_JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested);

/* ---------------- jsonb_util.c (verbatim bodies) ---------------- */

static uint32
pg_getJsonbOffset(const JsonbContainer *jc, int index)
{
	uint32		offset = 0;
	int			i;

	for (i = index - 1; i >= 0; i--)
	{
		offset += JBE_OFFLENFLD(jc->children[i]);
		if (JBE_HAS_OFF(jc->children[i]))
			break;
	}

	return offset;
}

static uint32
pg_getJsonbLength(const JsonbContainer *jc, int index)
{
	uint32		off;
	uint32		len;

	if (JBE_HAS_OFF(jc->children[index]))
	{
		off = pg_getJsonbOffset(jc, index);
		len = JBE_OFFLENFLD(jc->children[index]) - off;
	}
	else
		len = JBE_OFFLENFLD(jc->children[index]);

	return len;
}

static void
pg_fillJsonbValue(JsonbContainer *container, int index,
				  char *base_addr, uint32 offset,
				  JsonbValue *result)
{
	JEntry		entry = container->children[index];

	if (JBE_ISNULL(entry))
	{
		result->type = jbvNull;
	}
	else if (JBE_ISSTRING(entry))
	{
		result->type = jbvString;
		result->val.string.val = base_addr + offset;
		result->val.string.len = pg_getJsonbLength(container, index);
		Assert(result->val.string.len >= 0);
	}
	else if (JBE_ISNUMERIC(entry))
	{
		result->type = jbvNumeric;
		result->val.numeric = (Numeric) (base_addr + INTALIGN(offset));
	}
	else if (JBE_ISBOOL_TRUE(entry))
	{
		result->type = jbvBool;
		result->val.boolean = true;
	}
	else if (JBE_ISBOOL_FALSE(entry))
	{
		result->type = jbvBool;
		result->val.boolean = false;
	}
	else
	{
		Assert(JBE_ISCONTAINER(entry));
		result->type = jbvBinary;
		/* Remove alignment padding from data pointer and length */
		result->val.binary.data = (JsonbContainer *) (base_addr + INTALIGN(offset));
		result->val.binary.len = pg_getJsonbLength(container, index) -
			(INTALIGN(offset) - offset);
	}
}

/*
 * SHIM (allocation plumbing only; copied verbatim from jsonb-probe):
 * iterators come from TYPED named-static slots — raw pool bytes defeat
 * CBMC's field sensitivity (measured there). pgg_reset() rewinds.
 */
#define PGG_ITER_SLOTS 6
static JsonbIterator pgg_it0, pgg_it1, pgg_it2, pgg_it3, pgg_it4, pgg_it5;
static int	pgg_iter_next_slot;

static JsonbIterator *
pgg_alloc_iter(void)
{
	JsonbIterator *p;

	__CPROVER_assume(pgg_iter_next_slot < PGG_ITER_SLOTS);
	switch (pgg_iter_next_slot++)
	{
		case 0:
			p = &pgg_it0;
			break;
		case 1:
			p = &pgg_it1;
			break;
		case 2:
			p = &pgg_it2;
			break;
		case 3:
			p = &pgg_it3;
			break;
		case 4:
			p = &pgg_it4;
			break;
		default:
			p = &pgg_it5;
			break;
	}
	memset(p, 0, sizeof(JsonbIterator));
	return p;
}

static JsonbIterator *
pg_iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent)
{
	JsonbIterator *it;

	it = pgg_alloc_iter();		/* shim: was palloc0(sizeof(JsonbIterator)) */
	it->container = container;
	it->parent = parent;
	it->nElems = JsonContainerSize(container);

	/* Array starts just after header */
	it->children = container->children;

	switch (container->header & (JB_FARRAY | JB_FOBJECT))
	{
		case JB_FARRAY:
			it->dataProper =
				(char *) it->children + it->nElems * sizeof(JEntry);
			it->isScalar = JsonContainerIsScalar(container);
			/* This is either a "raw scalar", or an array */
			Assert(!it->isScalar || it->nElems == 1);

			it->state = JBI_ARRAY_START;
			break;

		case JB_FOBJECT:
			it->dataProper =
				(char *) it->children + it->nElems * sizeof(JEntry) * 2;
			it->state = JBI_OBJECT_START;
			break;

		default:
			elog(ERROR, "unknown type of jsonb container");
	}

	return it;
}

static JsonbIterator *
pg_freeAndGetParent(JsonbIterator *it)
{
	JsonbIterator *v = it->parent;

	pfree(it);
	return v;
}

static JsonbIterator *
pg_JsonbIteratorInit(JsonbContainer *container)
{
	return pg_iteratorFromContainer(container, NULL);
}

static JsonbIteratorToken
pg_JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested)
{
	if (*it == NULL)
	{
		val->type = jbvNull;
		return WJB_DONE;
	}

recurse:
	switch ((*it)->state)
	{
		case JBI_ARRAY_START:
			/* Set v to array on first array call */
			val->type = jbvArray;
			val->val.array.nElems = (*it)->nElems;
			val->val.array.rawScalar = (*it)->isScalar;
			(*it)->curIndex = 0;
			(*it)->curDataOffset = 0;
			(*it)->curValueOffset = 0;	/* not actually used */
			/* Set state for next call */
			(*it)->state = JBI_ARRAY_ELEM;
			return WJB_BEGIN_ARRAY;

		case JBI_ARRAY_ELEM:
			if ((*it)->curIndex >= (*it)->nElems)
			{
				*it = pg_freeAndGetParent(*it);
				val->type = jbvNull;
				return WJB_END_ARRAY;
			}

			pg_fillJsonbValue((*it)->container, (*it)->curIndex,
							  (*it)->dataProper, (*it)->curDataOffset,
							  val);

			JBE_ADVANCE_OFFSET((*it)->curDataOffset,
							   (*it)->children[(*it)->curIndex]);
			(*it)->curIndex++;

			if (!IsAJsonbScalar(val) && !skipNested)
			{
				/* Recurse into container. */
				*it = pg_iteratorFromContainer(val->val.binary.data, *it);
				goto recurse;
			}
			else
			{
				return WJB_ELEM;
			}

		case JBI_OBJECT_START:
			/* Set v to object on first object call */
			val->type = jbvObject;
			val->val.object.nPairs = (*it)->nElems;
			(*it)->curIndex = 0;
			(*it)->curDataOffset = 0;
			(*it)->curValueOffset = pg_getJsonbOffset((*it)->container,
													  (*it)->nElems);
			/* Set state for next call */
			(*it)->state = JBI_OBJECT_KEY;
			return WJB_BEGIN_OBJECT;

		case JBI_OBJECT_KEY:
			if ((*it)->curIndex >= (*it)->nElems)
			{
				*it = pg_freeAndGetParent(*it);
				val->type = jbvNull;
				return WJB_END_OBJECT;
			}
			else
			{
				/* Return key of a key/value pair.  */
				pg_fillJsonbValue((*it)->container, (*it)->curIndex,
								  (*it)->dataProper, (*it)->curDataOffset,
								  val);
				if (val->type != jbvString)
					elog(ERROR, "unexpected jsonb type as object key");

				/* Set state for next call */
				(*it)->state = JBI_OBJECT_VALUE;
				return WJB_KEY;
			}

		case JBI_OBJECT_VALUE:
			/* Set state for next call */
			(*it)->state = JBI_OBJECT_KEY;

			pg_fillJsonbValue((*it)->container, (*it)->curIndex + (*it)->nElems,
							  (*it)->dataProper, (*it)->curValueOffset,
							  val);

			JBE_ADVANCE_OFFSET((*it)->curDataOffset,
							   (*it)->children[(*it)->curIndex]);
			JBE_ADVANCE_OFFSET((*it)->curValueOffset,
							   (*it)->children[(*it)->curIndex + (*it)->nElems]);
			(*it)->curIndex++;

			if (!IsAJsonbScalar(val) && !skipNested)
			{
				*it = pg_iteratorFromContainer(val->val.binary.data, *it);
				goto recurse;
			}
			else
				return WJB_VALUE;
	}

	elog(ERROR, "invalid jsonb iterator state");
	/* satisfy compilers that don't know that elog(ERROR) doesn't return */
	val->type = jbvNull;
	return WJB_DONE;
}

/* ---------------- hash seam (see file header) ---------------- */

static uint32 pgg_hash_seam_skew = 0;

int
pgg_set_hash_skew(int on)
{
	pgg_hash_seam_skew = on ? 1 : 0;
	return 0;
}

/*
 * SHARED SEAM MODEL for hash_any: FNV-1a 32. The Rust harness stubs
 * hashfn::hash_bytes with a literal re-statement of this body (minus the
 * skew term, which only the skew CONTROL enables).
 */
uint32
pg_seam_hash_bytes(const unsigned char *k, int keylen)
{
	uint32		h = 0x811c9dc5u;
	int			i;

	for (i = 0; i < keylen; i++)
	{
		h ^= k[i];
		h *= 0x01000193u;
	}
	return h + pgg_hash_seam_skew;
}

#define hash_any(p, len) pg_seam_hash_bytes((p), (len))

/* port/pg_bitutils.h (verbatim body) */
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

/* jsonb_util.c JsonbHashScalarValue (verbatim body; numeric arm shimmed
 * to abort — numeric is out of fence) */
static void
pg_JsonbHashScalarValue(const JsonbValue *scalarVal, uint32 *hash)
{
	uint32		tmp;

	/* Compute hash value for scalarVal */
	switch (scalarVal->type)
	{
		case jbvNull:
			tmp = 0x01;
			break;
		case jbvString:
			tmp = DatumGetUInt32(hash_any((const unsigned char *) scalarVal->val.string.val,
										  scalarVal->val.string.len));
			break;
		case jbvNumeric:
			/* shim: DirectFunctionCall1(hash_numeric, ..) — numeric is
			 * outside the harness fence */
			pg_proof_abort = 1;
			tmp = 0;
			break;
		case jbvBool:
			tmp = scalarVal->val.boolean ? 0x02 : 0x04;

			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
			tmp = 0;			/* keep compiler quiet */
			break;
	}

	/*
	 * Combine hash values of successive keys, values and elements by rotating
	 * the previous value left 1 bit, then XOR'ing in the new
	 * key/value/element's hash value.
	 */
	*hash = pg_rotate_left32(*hash, 1);
	*hash ^= tmp;
}

/* ---------------- jsonb_gin.c (verbatim bodies) ---------------- */

typedef struct PathHashStack
{
	uint32		hash;
	struct PathHashStack *parent;
} PathHashStack;

/* Buffer for GIN entries */
typedef struct GinEntries
{
	Datum	   *buf;
	int			count;
	int			allocated;
} GinEntries;

typedef enum JsonPathGinNodeType
{
	JSP_GIN_OR,
	JSP_GIN_AND,
	JSP_GIN_ENTRY,
} JsonPathGinNodeType;

typedef struct JsonPathGinNode JsonPathGinNode;

/* Node in jsonpath expression tree */
struct JsonPathGinNode
{
	JsonPathGinNodeType type;
	union
	{
		int			nargs;		/* valid for OR and AND nodes */
		int			entryIndex; /* index in GinEntries array, valid for ENTRY
								 * nodes after entries output */
		Datum		entryDatum; /* path hash or key name/scalar, valid for
								 * ENTRY nodes before entries output */
	}			val;
	JsonPathGinNode *args[];	/* FLEXIBLE_ARRAY_MEMBER; valid for OR and
								 * AND nodes */
};

/* Initialize GinEntries struct */
static void
init_gin_entries(GinEntries *entries, int preallocated)
{
	entries->allocated = preallocated;
	entries->buf = preallocated ? palloc(sizeof(Datum) * preallocated) : NULL;
	entries->count = 0;
}

/* Add new entry to GinEntries */
static int
add_gin_entry(GinEntries *entries, Datum entry)
{
	int			id = entries->count;

	if (entries->count >= entries->allocated)
	{
		if (entries->allocated)
		{
			entries->allocated *= 2;
			entries->buf = repalloc(entries->buf,
									sizeof(Datum) * entries->allocated);
		}
		else
		{
			entries->allocated = 8;
			entries->buf = palloc(sizeof(Datum) * entries->allocated);
		}
	}

	entries->buf[entries->count++] = entry;

	return id;
}

/*
 * Construct a jsonb_ops GIN key from a flag byte and a textual representation
 * (which need not be null-terminated).  This function is responsible
 * for hashing overlength text representations; it will add the
 * JGINFLAG_HASHED bit to the flag value if it does that.
 * (verbatim; the len > JGIN_MAXLENGTH arm is dead under the builder fence —
 * snprintf is an abort macro there)
 */
static Datum
make_text_key(char flag, const char *str, int len)
{
	text	   *item;
	char		hashbuf[10];

	if (len > JGIN_MAXLENGTH)
	{
		uint32		hashval;

		hashval = DatumGetUInt32(hash_any((const unsigned char *) str, len));
		snprintf(hashbuf, sizeof(hashbuf), "%08x", hashval);
		str = hashbuf;
		len = 8;
		flag |= JGINFLAG_HASHED;
	}

	/*
	 * Now build the text Datum.  For simplicity we build a 4-byte-header
	 * varlena text Datum here, but we expect it will get converted to short
	 * header format when stored in the index.
	 */
	item = (text *) palloc(VARHDRSZ + len + 1);
	SET_VARSIZE(item, VARHDRSZ + len + 1);

	*VARDATA(item) = flag;

	memcpy(VARDATA(item) + 1, str, len);

	return PointerGetDatum(item);
}

/* shim: numeric_normalize — numeric values are outside the harness fence */
static char *
numeric_normalize(Numeric num)
{
	static char pgg_num_dead[1];

	pg_proof_abort = 1;
	pgg_num_dead[0] = '\0';
	return pgg_num_dead;
}

/*
 * Create a textual representation of a JsonbValue that will serve as a GIN
 * key in a jsonb_ops index.  is_key is true if the JsonbValue is a key,
 * or if it is a string array element (since we pretend those are keys,
 * see jsonb.h).
 * (verbatim; numeric arm reaches the abort shim above)
 */
static Datum
make_scalar_key(const JsonbValue *scalarVal, bool is_key)
{
	Datum		item;
	char	   *cstr;

	switch (scalarVal->type)
	{
		case jbvNull:
			Assert(!is_key);
			item = make_text_key(JGINFLAG_NULL, "", 0);
			break;
		case jbvBool:
			Assert(!is_key);
			item = make_text_key(JGINFLAG_BOOL,
								 scalarVal->val.boolean ? "t" : "f", 1);
			break;
		case jbvNumeric:
			Assert(!is_key);

			/*
			 * A normalized textual representation, free of trailing zeroes,
			 * is required so that numerically equal values will produce equal
			 * strings.
			 */
			cstr = numeric_normalize(scalarVal->val.numeric);
			item = make_text_key(JGINFLAG_NUM, cstr, strlen(cstr));
			pfree(cstr);
			break;
		case jbvString:
			item = make_text_key(is_key ? JGINFLAG_KEY : JGINFLAG_STR,
								 scalarVal->val.string.val,
								 scalarVal->val.string.len);
			break;
		default:
			elog(ERROR, "unrecognized jsonb scalar type: %d", scalarVal->type);
			item = 0;			/* keep compiler quiet */
			break;
	}

	return item;
}

/* jsonb_gin.c node constructors (verbatim) */
static JsonPathGinNode *
make_jsp_entry_node(Datum entry)
{
	JsonPathGinNode *node = palloc(offsetof(JsonPathGinNode, args));

	node->type = JSP_GIN_ENTRY;
	node->val.entryDatum = entry;

	return node;
}

static JsonPathGinNode *
make_jsp_expr_node(JsonPathGinNodeType type, int nargs)
{
	JsonPathGinNode *node = palloc(offsetof(JsonPathGinNode, args) +
								   sizeof(node->args[0]) * nargs);

	node->type = type;
	node->val.nargs = nargs;

	return node;
}

static JsonPathGinNode *
make_jsp_expr_node_binary(JsonPathGinNodeType type,
						  JsonPathGinNode *arg1, JsonPathGinNode *arg2)
{
	JsonPathGinNode *node = make_jsp_expr_node(type, 2);

	node->args[0] = arg1;
	node->args[1] = arg2;

	return node;
}

/*
 * Recursively execute jsonpath expression.
 * 'check' is a bool[] or a GinTernaryValue[] depending on 'ternary' flag.
 * (verbatim)
 */
static GinTernaryValue
execute_jsp_gin_node(JsonPathGinNode *node, void *check, bool ternary)
{
	GinTernaryValue res;
	GinTernaryValue v;
	int			i;

	switch (node->type)
	{
		case JSP_GIN_AND:
			res = GIN_TRUE;
			for (i = 0; i < node->val.nargs; i++)
			{
				v = execute_jsp_gin_node(node->args[i], check, ternary);
				if (v == GIN_FALSE)
					return GIN_FALSE;
				else if (v == GIN_MAYBE)
					res = GIN_MAYBE;
			}
			return res;

		case JSP_GIN_OR:
			res = GIN_FALSE;
			for (i = 0; i < node->val.nargs; i++)
			{
				v = execute_jsp_gin_node(node->args[i], check, ternary);
				if (v == GIN_TRUE)
					return GIN_TRUE;
				else if (v == GIN_MAYBE)
					res = GIN_MAYBE;
			}
			return res;

		case JSP_GIN_ENTRY:
			{
				int			index = node->val.entryIndex;

				if (ternary)
					return ((GinTernaryValue *) check)[index];
				else
					return ((bool *) check)[index] ? GIN_TRUE : GIN_FALSE;
			}

		default:
			elog(ERROR, "invalid jsonpath gin node type: %d", node->type);
			return GIN_FALSE;	/* keep compiler quiet */
	}
}

/* ---------------- extract cores (fmgr unwrap shims) ---------------- */

/* extract results parked in statics for the entry accessors below */
static Datum *pgg_entries_buf = NULL;
static int	pgg_entries_count = 0;

/*
 * gin_extract_jsonb (verbatim body; fmgr unwrap: jb -> pre-detoasted
 * container payload `root`; JB_ROOT_COUNT(jb) reads the same header word;
 * *nentries / PG_RETURN_POINTER -> statics)
 */
int
pgg_extract_jsonb(const unsigned char *container)
{
	JsonbContainer *root = (JsonbContainer *) container;
	int			total = JsonContainerSize(root);	/* JB_ROOT_COUNT(jb) */
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken r;
	GinEntries	entries;

	/* If the root level is empty, we certainly have no keys */
	if (total == 0)
	{
		pgg_entries_count = 0;
		pgg_entries_buf = NULL;
		return 0;
	}

	/* Otherwise, use 2 * root count as initial estimate of result size */
	init_gin_entries(&entries, 2 * total);

	it = pg_JsonbIteratorInit(root);

	while ((r = pg_JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (r)
		{
			case WJB_KEY:
				add_gin_entry(&entries, make_scalar_key(&v, true));
				break;
			case WJB_ELEM:
				/* Pretend string array elements are keys, see jsonb.h */
				add_gin_entry(&entries, make_scalar_key(&v, v.type == jbvString));
				break;
			case WJB_VALUE:
				add_gin_entry(&entries, make_scalar_key(&v, false));
				break;
			default:
				/* we can ignore structural items */
				break;
		}
	}

	pgg_entries_count = entries.count;
	pgg_entries_buf = entries.buf;
	return entries.count;
}

/*
 * gin_extract_jsonb_path (verbatim body; same fmgr unwrap as above)
 */
int
pgg_extract_jsonb_path(const unsigned char *container)
{
	JsonbContainer *root = (JsonbContainer *) container;
	int			total = JsonContainerSize(root);	/* JB_ROOT_COUNT(jb) */
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken r;
	PathHashStack tail;
	PathHashStack *stack;
	GinEntries	entries;

	/* If the root level is empty, we certainly have no keys */
	if (total == 0)
	{
		pgg_entries_count = 0;
		pgg_entries_buf = NULL;
		return 0;
	}

	/* Otherwise, use 2 * root count as initial estimate of result size */
	init_gin_entries(&entries, 2 * total);

	/* We keep a stack of partial hashes corresponding to parent key levels */
	tail.parent = NULL;
	tail.hash = 0;
	stack = &tail;

	it = pg_JsonbIteratorInit(root);

	while ((r = pg_JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		PathHashStack *parent;

		switch (r)
		{
			case WJB_BEGIN_ARRAY:
			case WJB_BEGIN_OBJECT:
				/* Push a stack level for this object */
				parent = stack;
				stack = (PathHashStack *) palloc(sizeof(PathHashStack));

				/*
				 * We pass forward hashes from outer nesting levels so that
				 * the hashes for nested values will include outer keys as
				 * well as their own keys.
				 */
				stack->hash = parent->hash;
				stack->parent = parent;
				break;
			case WJB_KEY:
				/* mix this key into the current outer hash */
				pg_JsonbHashScalarValue(&v, &stack->hash);
				/* hash is now ready to incorporate the value */
				break;
			case WJB_ELEM:
			case WJB_VALUE:
				/* mix the element or value's hash into the prepared hash */
				pg_JsonbHashScalarValue(&v, &stack->hash);
				/* and emit an index entry */
				add_gin_entry(&entries, UInt32GetDatum(stack->hash));
				/* reset hash for next key, value, or sub-object */
				stack->hash = stack->parent->hash;
				break;
			case WJB_END_ARRAY:
			case WJB_END_OBJECT:
				/* Pop the stack */
				parent = stack->parent;
				pfree(stack);
				stack = parent;
				/* reset hash for next key, value, or sub-object */
				if (stack->parent)
					stack->hash = stack->parent->hash;
				else
					stack->hash = 0;
				break;
			default:
				elog(ERROR, "invalid JsonbIteratorNext rc: %d", (int) r);
		}
	}

	pgg_entries_count = entries.count;
	pgg_entries_buf = entries.buf;
	return entries.count;
}

/* entry accessors (harness plumbing: pointee reads stay on the C side —
 * brin-minmax provenance lesson) */
int
pgg_entry_len(int i)
{
	return (int) VARSIZE((const text *) DatumGetPointer(pgg_entries_buf[i]));
}

int
pgg_entry_byte(int i, int off)
{
	return (int) ((const unsigned char *) DatumGetPointer(pgg_entries_buf[i]))[off];
}

unsigned
pgg_entry_u32(int i)
{
	return (unsigned) DatumGetUInt32(pgg_entries_buf[i]);
}

/* ---------------- consistent / triconsistent (fmgr unwrap) ------------- */

/*
 * gin_consistent_jsonb (verbatim body; fmgr unwrap: params + int return;
 * extra_data[0] carries the jsonpath node tree exactly as in C)
 */
int
pgg_consistent_jsonb(const unsigned char *check_in, unsigned strategy_in,
					 int nkeys_in, JsonPathGinNode *node,
					 int *recheck_out, int *err)
{
	bool	   *check = (bool *) check_in;
	StrategyNumber strategy = (StrategyNumber) strategy_in;
	int32		nkeys = nkeys_in;
	Pointer		extra_data_local[1];
	Pointer    *extra_data = extra_data_local;
	bool		recheck_local = false;
	bool	   *recheck = &recheck_local;
	bool		res = true;
	int32		i;

	extra_data_local[0] = (Pointer) node;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * We must always recheck, since we can't tell from the index whether
		 * the positions of the matched items match the structure of the query
		 * object.  However, the tuple certainly doesn't match unless it
		 * contains all the query keys.
		 */
		*recheck = true;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbExistsStrategyNumber)
	{
		/*
		 * Although the key is certainly present in the index, we must recheck
		 * because (1) the key might be hashed, and (2) the index match might
		 * be for a key that's not at top level of the JSON object.
		 */
		*recheck = true;
		res = true;
	}
	else if (strategy == JsonbExistsAnyStrategyNumber)
	{
		/* As for plain exists, we must recheck */
		*recheck = true;
		res = true;
	}
	else if (strategy == JsonbExistsAllStrategyNumber)
	{
		/* As for plain exists, we must recheck */
		*recheck = true;
		/* ... but unless all the keys are present, we can say "false" */
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		*recheck = true;

		if (nkeys > 0)
		{
			Assert(extra_data && extra_data[0]);
			res = execute_jsp_gin_node((JsonPathGinNode *) extra_data[0], check,
									   false) != GIN_FALSE;
		}
	}
	else
	{
		/* shim: elog(ERROR, "unrecognized strategy number") -> err flag */
		PROOF_EREPORT_FLAG(err);
	}

	*recheck_out = recheck_local ? 1 : 0;
	return res ? 1 : 0;
}

/*
 * gin_triconsistent_jsonb (verbatim body; fmgr unwrap as above)
 */
int
pgg_triconsistent_jsonb(const signed char *check_in, unsigned strategy_in,
						int nkeys_in, JsonPathGinNode *node, int *err)
{
	GinTernaryValue *check = (GinTernaryValue *) check_in;
	StrategyNumber strategy = (StrategyNumber) strategy_in;
	int32		nkeys = nkeys_in;
	Pointer		extra_data_local[1];
	Pointer    *extra_data = extra_data_local;
	GinTernaryValue res = GIN_MAYBE;
	int32		i;

	extra_data_local[0] = (Pointer) node;

	/*
	 * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE; this
	 * corresponds to always forcing recheck in the regular consistent
	 * function, for the reasons listed there.
	 */
	if (strategy == JsonbContainsStrategyNumber ||
		strategy == JsonbExistsAllStrategyNumber)
	{
		/* All extracted keys must be present */
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_FALSE)
			{
				res = GIN_FALSE;
				break;
			}
		}
	}
	else if (strategy == JsonbExistsStrategyNumber ||
			 strategy == JsonbExistsAnyStrategyNumber)
	{
		/* At least one extracted key must be present */
		res = GIN_FALSE;
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_TRUE ||
				check[i] == GIN_MAYBE)
			{
				res = GIN_MAYBE;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		if (nkeys > 0)
		{
			Assert(extra_data && extra_data[0]);
			res = execute_jsp_gin_node((JsonPathGinNode *) extra_data[0], check,
									   true);

			/* Should always recheck the result */
			if (res == GIN_TRUE)
				res = GIN_MAYBE;
		}
	}
	else
	{
		/* shim: elog(ERROR, "unrecognized strategy number") -> err flag */
		PROOF_EREPORT_FLAG(err);
	}

	return (int) res;
}

/*
 * gin_consistent_jsonb_path (verbatim body; fmgr unwrap as above)
 */
int
pgg_consistent_jsonb_path(const unsigned char *check_in, unsigned strategy_in,
						  int nkeys_in, JsonPathGinNode *node,
						  int *recheck_out, int *err)
{
	bool	   *check = (bool *) check_in;
	StrategyNumber strategy = (StrategyNumber) strategy_in;
	int32		nkeys = nkeys_in;
	Pointer		extra_data_local[1];
	Pointer    *extra_data = extra_data_local;
	bool		recheck_local = false;
	bool	   *recheck = &recheck_local;
	bool		res = true;
	int32		i;

	extra_data_local[0] = (Pointer) node;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * jsonb_path_ops is necessarily lossy, not only because of hash
		 * collisions but also because it doesn't preserve complete
		 * information about the structure of the JSON object.  So we must
		 * always recheck a match.  However, if not all of the keys are
		 * present, the tuple certainly doesn't match.
		 */
		*recheck = true;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		*recheck = true;

		if (nkeys > 0)
		{
			Assert(extra_data && extra_data[0]);
			res = execute_jsp_gin_node((JsonPathGinNode *) extra_data[0], check,
									   false) != GIN_FALSE;
		}
	}
	else
	{
		/* shim: elog(ERROR, "unrecognized strategy number") -> err flag */
		PROOF_EREPORT_FLAG(err);
	}

	*recheck_out = recheck_local ? 1 : 0;
	return res ? 1 : 0;
}

/*
 * gin_triconsistent_jsonb_path (verbatim body; fmgr unwrap as above)
 */
int
pgg_triconsistent_jsonb_path(const signed char *check_in, unsigned strategy_in,
							 int nkeys_in, JsonPathGinNode *node, int *err)
{
	GinTernaryValue *check = (GinTernaryValue *) check_in;
	StrategyNumber strategy = (StrategyNumber) strategy_in;
	int32		nkeys = nkeys_in;
	Pointer		extra_data_local[1];
	Pointer    *extra_data = extra_data_local;
	GinTernaryValue res = GIN_MAYBE;
	int32		i;

	extra_data_local[0] = (Pointer) node;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE;
		 * this corresponds to always forcing recheck in the regular
		 * consistent function, for the reasons listed there.
		 */
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_FALSE)
			{
				res = GIN_FALSE;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		if (nkeys > 0)
		{
			Assert(extra_data && extra_data[0]);
			res = execute_jsp_gin_node((JsonPathGinNode *) extra_data[0], check,
									   true);

			/* Should always recheck the result */
			if (res == GIN_TRUE)
				res = GIN_MAYBE;
		}
	}
	else
	{
		/* shim: elog(ERROR, "unrecognized strategy number") -> err flag */
		PROOF_EREPORT_FLAG(err);
	}

	return (int) res;
}

/* ------------- node-tree builders (harness scaffolding) -------------- */

#define PGG_NODE_HANDLES 8
static JsonPathGinNode *pgg_nodes[PGG_NODE_HANDLES];
static int	pgg_node_next = 0;

/* ENTRY node carrying its post-emit entryIndex (the state in which the
 * consistent functions run; emit_jsp_gin_entries assigns preorder indices
 * in production — the harness passes the SAME index to the Rust ops). */
int
pgg_mk_entry(int entry_index)
{
	JsonPathGinNode *n = make_jsp_entry_node((Datum) 0);

	n->val.entryIndex = entry_index;
	__CPROVER_assume(pgg_node_next < PGG_NODE_HANDLES);
	pgg_nodes[pgg_node_next] = n;
	return pgg_node_next++;
}

int
pgg_mk_expr2(int type, int a, int b)
{
	JsonPathGinNode *n = make_jsp_expr_node_binary((JsonPathGinNodeType) type,
												   pgg_nodes[a], pgg_nodes[b]);

	__CPROVER_assume(pgg_node_next < PGG_NODE_HANDLES);
	pgg_nodes[pgg_node_next] = n;
	return pgg_node_next++;
}

int
pgg_mk_expr3(int type, int a, int b, int c)
{
	JsonPathGinNode *n = make_jsp_expr_node((JsonPathGinNodeType) type, 3);

	n->args[0] = pgg_nodes[a];
	n->args[1] = pgg_nodes[b];
	n->args[2] = pgg_nodes[c];
	__CPROVER_assume(pgg_node_next < PGG_NODE_HANDLES);
	pgg_nodes[pgg_node_next] = n;
	return pgg_node_next++;
}

JsonPathGinNode *
pgg_node(int handle)
{
	return handle < 0 ? NULL : pgg_nodes[handle];
}

/* direct core access for the per-shape ternary-logic cells */
int
pgg_execute_node(int handle, const signed char *check, int ternary)
{
	return (int) execute_jsp_gin_node(pgg_nodes[handle], (void *) check,
									  ternary != 0);
}

/* consistent wrappers taking a node HANDLE (FFI keeps handles, not raw
 * pointers, on the Rust side) */
int
pgg_consistent_jsonb_h(const unsigned char *check, unsigned strategy,
					   int nkeys, int handle, int *recheck_out, int *err)
{
	return pgg_consistent_jsonb(check, strategy, nkeys, pgg_node(handle),
								recheck_out, err);
}

int
pgg_triconsistent_jsonb_h(const signed char *check, unsigned strategy,
						  int nkeys, int handle, int *err)
{
	return pgg_triconsistent_jsonb(check, strategy, nkeys, pgg_node(handle), err);
}

int
pgg_consistent_jsonb_path_h(const unsigned char *check, unsigned strategy,
							int nkeys, int handle, int *recheck_out, int *err)
{
	return pgg_consistent_jsonb_path(check, strategy, nkeys, pgg_node(handle),
									 recheck_out, err);
}

int
pgg_triconsistent_jsonb_path_h(const signed char *check, unsigned strategy,
							   int nkeys, int handle, int *err)
{
	return pgg_triconsistent_jsonb_path(check, strategy, nkeys,
										pgg_node(handle), err);
}

/* harness entry: rewind pools + clear flags.
 * (int return: Kani lowers Rust () as `struct Unit`, which goto-cc
 * rejects against C void.) */
int
pgg_reset(void)
{
	pgg_pool_next = 0;
	pgg_iter_next_slot = 0;
	pgg_node_next = 0;
	pgg_entries_buf = NULL;
	pgg_entries_count = 0;
	pgg_hash_seam_skew = 0;
	pg_proof_abort = 0;
	return 0;
}

int
pgg_take_abort(void)
{
	int			a = pg_proof_abort;

	pg_proof_abort = 0;
	return a;
}
