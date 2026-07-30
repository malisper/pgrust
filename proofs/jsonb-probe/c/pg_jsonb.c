/*
 * Vendored PostgreSQL C for the jsonb BINARY-FORMAT proofs (jsonb-probe).
 *
 * Provenance (all REL_18_STABLE, fetched 2026-07-28 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/backend/utils/adt/jsonb_util.c:
 *       getJsonbOffset, getJsonbLength, compareJsonbContainers,
 *       findJsonbValueFromContainer, getKeyJsonValueFromContainer,
 *       getIthJsonbValueFromContainer, fillJsonbValue,
 *       JsonbIteratorInit, JsonbIteratorNext, iteratorFromContainer,
 *       freeAndGetParent, equalsJsonbScalarValue, compareJsonbScalarValue,
 *       lengthCompareJsonbStringValue, lengthCompareJsonbString
 *   - src/backend/utils/adt/jsonb.c:
 *       JsonbContainerTypeName, JsonbTypeName, JsonbExtractScalar
 *   - src/backend/utils/adt/jsonfuncs.c:
 *       jsonb_array_length, jsonb_object_field, jsonb_array_element
 *       (fmgr bodies, extracted per the shim rules below)
 *   - src/backend/utils/adt/jsonb_op.c: jsonb_exists (fmgr body)
 *   - src/include/utils/jsonb.h: JEntry/JsonbContainer/JsonbValue/
 *       JsonbIterator declarations + the JB_ and JBE_ macros, verbatim.
 *
 * FORMAT INVARIANT (the harness fence; jsonb.h "JEntry format" comment):
 * a container is a uint32 header (count | JB_FOBJECT/JB_FARRAY, optional
 * JB_FSCALAR on a 1-element raw-scalar array), then n (arrays) or 2n
 * (objects: keys then values) JEntrys, then the variable-length data.
 * Each JEntry's low 28 bits hold either the node's data LENGTH or (when
 * JENTRY_HAS_OFF) its end+1 OFFSET from the start of the variable-length
 * area; readers must accept EITHER form at any index (jsonb.h: "When
 * examining an existing value, pay attention to the HAS_OFF bits").
 * Object keys are unique and sorted by lengthCompareJsonbString (length
 * first, then memcmp) — the writer invariant getKeyJsonValueFromContainer's
 * binary search relies on. The harness builder constructs exactly such
 * images (with symbolic-but-consistent HAS_OFF choices, a superset of the
 * writer's every-JB_OFFSET_STRIDE'th placement).
 *
 * SHIMS (everything else is verbatim; function names pg_-prefixed):
 *   - typedefs/Assert/Min via ../../support/c/pg_proof_shim.h.
 *   - palloc/palloc0/pfree/repalloc -> fixed static bump pool (pgp_reset()
 *     rewinds it; exhaustion sets the abort flag). Allocation strategy is
 *     harness plumbing, never part of the claim.
 *   - elog(ERROR, ...)/ereport(ERROR, ...) -> pg_proof_abort = 1 (variadic
 *     macro; no libc). C's control flow would longjmp; the vendored bodies
 *     keep executing, so every elog site must be unreachable under the
 *     harness fence, and each harness asserts pgp_take_abort() == 0.
 *     Exception: the two ereports in jsonb_array_length are error VERDICTS
 *     under proof and use the PROOF_EREPORT_FLAG out-param convention.
 *   - varstr_cmp (compareJsonbScalarValue jbvString arm): replaced by the
 *     SHARED SEAM MODEL pg_seam_varstr_cmp — C-locale semantics: memcmp of
 *     the common prefix (CBMC model: difference of first mismatching
 *     unsigned chars), then -1/0/1 length tiebreak. The Rust side of a cmp
 *     harness stubs varlena::varstr_cmp with the IDENTICAL model, so
 *     everything around the seam is proven and only collation internals
 *     leave the proof (dt-minmax shared-seam precedent).
 *   - DirectFunctionCall2(numeric_eq/numeric_cmp): numeric values are
 *     OUTSIDE the fence (adt_numeric is its own proof family); the arms set
 *     the abort flag and return a sentinel. Same for jbvDatetime (an
 *     in-memory-only type that cannot occur in a binary container).
 *   - fmgr unwrapping: PG_GETARG_JSONB_P(0) -> `JsonbContainer *c` param
 *     (pre-detoasted payload = &jb->root; the varlena header and detoast
 *     are out of scope, bytea-cmp precedent). PG_GETARG_TEXT_PP ->
 *     (const char *key, int keylen). PG_RETURN_NULL/JSONB_P -> found flag +
 *     JsonbValue out-params (result-image materialization JsonbValueToJsonb
 *     is out of scope). PG_RETURN_BOOL/INT32 -> int.
 *   - pg_abs_s32 (common/int.h): verbatim semantics, |x| as uint32,
 *     defined here because common/int.h drags in the full header tree.
 */

#include <stddef.h>
#include <string.h>
#include "../../support/c/pg_proof_shim.h"

/* ---------------- harness plumbing (not under proof) ---------------- */

static int pg_proof_abort = 0;

/* elog/ereport shim: record and continue (sites must be fenced out). */
#define elog(level, ...) (pg_proof_abort = 1)
#define ereport(level, rest) (pg_proof_abort = 1)

/* static bump pool standing in for palloc (harness plumbing only) */
#define PGP_POOL_CAP 2048
static unsigned char pgp_pool[PGP_POOL_CAP] __attribute__((aligned(8)));
static size_t pgp_pool_next = 0;

/* CBMC's assume primitive (available under kani -Z c-ffi). */
void		__CPROVER_assume(int cond);

static void *
palloc(Size size)
{
	size_t		start = (pgp_pool_next + 7) & ~(size_t) 7;

	/* Harness budget fence, NOT logic: a merging overflow arm (garbage
	 * pointer + abort flag) poisons every downstream pointer value-set and
	 * exploded the cmp formulas; assume() diverges instead, like a Rust
	 * panic path. Budget sufficiency is cover-protected: the harnesses'
	 * kani::cover! witnesses need full walks, which need allocations, so a
	 * too-small pool turns the covers red. */
	__CPROVER_assume(start + size <= PGP_POOL_CAP);
	pgp_pool_next = start + size;
	return pgp_pool + start;
}

static void *
palloc0(Size size)
{
	void	   *p = palloc(size);

	memset(p, 0, size);
	return p;
}

#define pfree(p) ((void) 0)

/* typed iterator slot pool (see pgp_alloc_iter below) */
static int	pgp_iter_next_slot;

/* harness entry: rewind pool + clear abort flag.
 * (int return: Kani lowers Rust () as `struct Unit`, which goto-cc
 * rejects against C void.) */
int
pgp_reset(void)
{
	pgp_pool_next = 0;
	pgp_iter_next_slot = 0;
	pg_proof_abort = 0;
	return 0;
}

int
pgp_take_abort(void)
{
	int			a = pg_proof_abort;

	pg_proof_abort = 0;
	return a;
}

/*
 * SHARED SEAM MODEL for varstr_cmp (see file header). Exported so the Rust
 * harness can also call it directly if ever needed; the Rust-side stub is a
 * literal re-statement of this body.
 */
int
pg_seam_varstr_cmp(const char *val1, int len1, const char *val2, int len2)
{
	int			result = memcmp(val1, val2, Min(len1, len2));

	if (result == 0)
	{
		if (len1 < len2)
			return -1;
		if (len1 > len2)
			return 1;
		return 0;
	}
	return result;
}

/* common/int.h pg_abs_s32, verbatim semantics (works for INT32_MIN) */
static inline uint32
pg_abs_s32(int32 a)
{
	return a < 0 ? 0 - (uint32) a : (uint32) a;
}

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
typedef uintptr_t Datum;		/* shim: only appears in the (dead) datetime arm */

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
		struct
		{
			Datum		value;
			Oid			typid;
			int32		typmod;
			int			tz;
		}			datetime;
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
static bool pg_equalsJsonbScalarValue(JsonbValue *a, JsonbValue *b);
static int	pg_compareJsonbScalarValue(JsonbValue *a, JsonbValue *b);
static JsonbIterator *pg_iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent);
static JsonbIterator *pg_freeAndGetParent(JsonbIterator *it);
static int	pg_lengthCompareJsonbStringValue(const void *a, const void *b);
static int	pg_lengthCompareJsonbString(const char *val1, int len1,
										const char *val2, int len2);
JsonbValue *pg_getKeyJsonValueFromContainer(JsonbContainer *container,
											const char *keyVal, int keyLen, JsonbValue *res);
JsonbIterator *pg_JsonbIteratorInit(JsonbContainer *container);
JsonbIteratorToken pg_JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested);

/* ---------------- jsonb_util.c (verbatim bodies) ---------------- */

uint32
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

uint32
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

int
pg_compareJsonbContainers(JsonbContainer *a, JsonbContainer *b)
{
	JsonbIterator *ita,
			   *itb;
	int			res = 0;

	ita = pg_JsonbIteratorInit(a);
	itb = pg_JsonbIteratorInit(b);

	do
	{
		JsonbValue	va,
					vb;
		JsonbIteratorToken ra,
					rb;

		ra = pg_JsonbIteratorNext(&ita, &va, false);
		rb = pg_JsonbIteratorNext(&itb, &vb, false);

		if (ra == rb)
		{
			if (ra == WJB_DONE)
			{
				/* Decisively equal */
				break;
			}

			if (ra == WJB_END_ARRAY || ra == WJB_END_OBJECT)
			{
				continue;
			}

			if (va.type == vb.type)
			{
				switch (va.type)
				{
					case jbvString:
					case jbvNull:
					case jbvNumeric:
					case jbvBool:
						res = pg_compareJsonbScalarValue(&va, &vb);
						break;
					case jbvArray:
						if (va.val.array.rawScalar != vb.val.array.rawScalar)
							res = (va.val.array.rawScalar) ? -1 : 1;
						if (va.val.array.nElems != vb.val.array.nElems)
							res = (va.val.array.nElems > vb.val.array.nElems) ? 1 : -1;
						break;
					case jbvObject:
						if (va.val.object.nPairs != vb.val.object.nPairs)
							res = (va.val.object.nPairs > vb.val.object.nPairs) ? 1 : -1;
						break;
					case jbvBinary:
						elog(ERROR, "unexpected jbvBinary value");
						break;
					case jbvDatetime:
						elog(ERROR, "unexpected jbvDatetime value");
						break;
				}
			}
			else
			{
				/* Type-defined order */
				res = (va.type > vb.type) ? 1 : -1;
			}
		}
		else
		{
			Assert(ra != WJB_END_ARRAY && ra != WJB_END_OBJECT);
			Assert(rb != WJB_END_ARRAY && rb != WJB_END_OBJECT);

			Assert(va.type != vb.type);
			Assert(va.type != jbvBinary);
			Assert(vb.type != jbvBinary);
			/* Type-defined order */
			res = (va.type > vb.type) ? 1 : -1;
		}
	}
	while (res == 0);

	while (ita != NULL)
	{
		JsonbIterator *i = ita->parent;

		pfree(ita);
		ita = i;
	}
	while (itb != NULL)
	{
		JsonbIterator *i = itb->parent;

		pfree(itb);
		itb = i;
	}

	return res;
}

JsonbValue *
pg_findJsonbValueFromContainer(JsonbContainer *container, uint32 flags,
							   JsonbValue *key)
{
	JEntry	   *children = container->children;
	int			count = JsonContainerSize(container);

	Assert((flags & ~(JB_FARRAY | JB_FOBJECT)) == 0);

	/* Quick out without a palloc cycle if object/array is empty */
	if (count <= 0)
		return NULL;

	if ((flags & JB_FARRAY) && JsonContainerIsArray(container))
	{
		JsonbValue *result = palloc(sizeof(JsonbValue));
		char	   *base_addr = (char *) (children + count);
		uint32		offset = 0;
		int			i;

		for (i = 0; i < count; i++)
		{
			pg_fillJsonbValue(container, i, base_addr, offset, result);

			if (key->type == result->type)
			{
				if (pg_equalsJsonbScalarValue(key, result))
					return result;
			}

			JBE_ADVANCE_OFFSET(offset, children[i]);
		}

		pfree(result);
	}
	else if ((flags & JB_FOBJECT) && JsonContainerIsObject(container))
	{
		/* Object key passed by caller must be a string */
		Assert(key->type == jbvString);

		return pg_getKeyJsonValueFromContainer(container, key->val.string.val,
											   key->val.string.len, NULL);
	}

	/* Not found */
	return NULL;
}

JsonbValue *
pg_getKeyJsonValueFromContainer(JsonbContainer *container,
								const char *keyVal, int keyLen, JsonbValue *res)
{
	JEntry	   *children = container->children;
	int			count = JsonContainerSize(container);
	char	   *baseAddr;
	uint32		stopLow,
				stopHigh;

	Assert(JsonContainerIsObject(container));

	/* Quick out without a palloc cycle if object is empty */
	if (count <= 0)
		return NULL;

	baseAddr = (char *) (children + count * 2);
	stopLow = 0;
	stopHigh = count;
	while (stopLow < stopHigh)
	{
		uint32		stopMiddle;
		int			difference;
		const char *candidateVal;
		int			candidateLen;

		stopMiddle = stopLow + (stopHigh - stopLow) / 2;

		candidateVal = baseAddr + pg_getJsonbOffset(container, stopMiddle);
		candidateLen = pg_getJsonbLength(container, stopMiddle);

		difference = pg_lengthCompareJsonbString(candidateVal, candidateLen,
												 keyVal, keyLen);

		if (difference == 0)
		{
			/* Found our key, return corresponding value */
			int			index = stopMiddle + count;

			if (!res)
				res = palloc(sizeof(JsonbValue));

			pg_fillJsonbValue(container, index, baseAddr,
							  pg_getJsonbOffset(container, index),
							  res);

			return res;
		}
		else
		{
			if (difference < 0)
				stopLow = stopMiddle + 1;
			else
				stopHigh = stopMiddle;
		}
	}

	/* Not found */
	return NULL;
}

JsonbValue *
pg_getIthJsonbValueFromContainer(JsonbContainer *container, uint32 i)
{
	JsonbValue *result;
	char	   *base_addr;
	uint32		nelements;

	if (!JsonContainerIsArray(container))
		elog(ERROR, "not a jsonb array");

	nelements = JsonContainerSize(container);
	base_addr = (char *) &container->children[nelements];

	if (i >= nelements)
		return NULL;

	result = palloc(sizeof(JsonbValue));

	pg_fillJsonbValue(container, i, base_addr,
					  pg_getJsonbOffset(container, i),
					  result);

	return result;
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

JsonbIterator *
pg_JsonbIteratorInit(JsonbContainer *container)
{
	return pg_iteratorFromContainer(container, NULL);
}

JsonbIteratorToken
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

/*
 * SHIM (allocation plumbing only): iterators come from a TYPED static slot
 * array instead of the byte bump pool. Iterator structs read back out of
 * raw pool bytes defeat CBMC's field sensitivity, and every subsequent
 * JsonbIteratorNext call then explodes the formula (measured: a concrete
 * empty-array pgp_cmp exceeded 6GiB); typed slots keep the struct fields
 * trackable. Slot count 8 covers the probe's <= 2 nesting levels x 2
 * iterators with headroom; exhaustion sets the abort flag like pool
 * exhaustion. pgp_reset() rewinds it.
 */
/*
 * Individual NAMED statics, not an array: CBMC's field sensitivity tracks
 * scalar struct statics but not indexed array elements — with an array
 * pool the iterator's state field never constant-folds and even a fully
 * concrete first JsonbIteratorNext call keeps every switch arm alive
 * (measured via probe_c1's loop census).
 */
#define PGP_ITER_SLOTS 6
static JsonbIterator pgp_it0, pgp_it1, pgp_it2, pgp_it3, pgp_it4, pgp_it5;

static JsonbIterator *
pgp_alloc_iter(void)
{
	JsonbIterator *p;

	/* Budget fence via assume, same rationale as palloc above. */
	__CPROVER_assume(pgp_iter_next_slot < PGP_ITER_SLOTS);
	switch (pgp_iter_next_slot++)
	{
		case 0:
			p = &pgp_it0;
			break;
		case 1:
			p = &pgp_it1;
			break;
		case 2:
			p = &pgp_it2;
			break;
		case 3:
			p = &pgp_it3;
			break;
		case 4:
			p = &pgp_it4;
			break;
		default:
			p = &pgp_it5;
			break;
	}
	memset(p, 0, sizeof(JsonbIterator));
	return p;
}

static JsonbIterator *
pg_iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent)
{
	JsonbIterator *it;

	it = pgp_alloc_iter();		/* shim: was palloc0(sizeof(JsonbIterator)) */
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

static bool
pg_equalsJsonbScalarValue(JsonbValue *a, JsonbValue *b)
{
	if (a->type == b->type)
	{
		switch (a->type)
		{
			case jbvNull:
				return true;
			case jbvString:
				return pg_lengthCompareJsonbStringValue(a, b) == 0;
			case jbvNumeric:
				/* shim: DirectFunctionCall2(numeric_eq, ..) — numeric is
				 * outside the harness fence */
				pg_proof_abort = 1;
				return false;
			case jbvBool:
				return a->val.boolean == b->val.boolean;

			default:
				elog(ERROR, "invalid jsonb scalar type");
		}
	}
	elog(ERROR, "jsonb scalar type mismatch");
	return false;
}

static int
pg_compareJsonbScalarValue(JsonbValue *a, JsonbValue *b)
{
	if (a->type == b->type)
	{
		switch (a->type)
		{
			case jbvNull:
				return 0;
			case jbvString:
				/* shim: varstr_cmp(.., DEFAULT_COLLATION_OID) -> shared
				 * seam model (see file header) */
				return pg_seam_varstr_cmp(a->val.string.val,
										  a->val.string.len,
										  b->val.string.val,
										  b->val.string.len);
			case jbvNumeric:
				/* shim: DirectFunctionCall2(numeric_cmp, ..) — numeric is
				 * outside the harness fence */
				pg_proof_abort = 1;
				return 0;
			case jbvBool:
				if (a->val.boolean == b->val.boolean)
					return 0;
				else if (a->val.boolean > b->val.boolean)
					return 1;
				else
					return -1;
			default:
				elog(ERROR, "invalid jsonb scalar type");
		}
	}
	elog(ERROR, "jsonb scalar type mismatch");
	return -1;
}

static int
pg_lengthCompareJsonbStringValue(const void *a, const void *b)
{
	const JsonbValue *va = (const JsonbValue *) a;
	const JsonbValue *vb = (const JsonbValue *) b;

	Assert(va->type == jbvString);
	Assert(vb->type == jbvString);

	return pg_lengthCompareJsonbString(va->val.string.val, va->val.string.len,
									   vb->val.string.val, vb->val.string.len);
}

static int
pg_lengthCompareJsonbString(const char *val1, int len1, const char *val2, int len2)
{
	if (len1 == len2)
		return memcmp(val1, val2, len1);
	else
		return len1 > len2 ? 1 : -1;
}

/* ---------------- jsonb.c (verbatim bodies) ---------------- */

bool
pg_JsonbExtractScalar(JsonbContainer *jbc, JsonbValue *res)
{
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	tmp;

	if (!JsonContainerIsArray(jbc) || !JsonContainerIsScalar(jbc))
	{
		/* inform caller about actual type of container */
		res->type = (JsonContainerIsArray(jbc)) ? jbvArray : jbvObject;
		return false;
	}

	it = pg_JsonbIteratorInit(jbc);

	tok = pg_JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_BEGIN_ARRAY);
	Assert(tmp.val.array.nElems == 1 && tmp.val.array.rawScalar);

	tok = pg_JsonbIteratorNext(&it, res, true);
	Assert(tok == WJB_ELEM);
	Assert(IsAJsonbScalar(res));

	tok = pg_JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_END_ARRAY);

	tok = pg_JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_DONE);

	(void) tok;					/* shim: PG_USED_FOR_ASSERTS_ONLY */
	return true;
}

const char *
pg_JsonbTypeName(JsonbValue *val)
{
	switch (val->type)
	{
		case jbvBinary:
			/* shim: recursion into JsonbContainerTypeName unreachable —
			 * JsonbExtractScalar never yields jbvBinary */
			elog(ERROR, "unreachable jbvBinary in JsonbTypeName");
			return "unknown";
		case jbvObject:
			return "object";
		case jbvArray:
			return "array";
		case jbvNumeric:
			return "number";
		case jbvString:
			return "string";
		case jbvBool:
			return "boolean";
		case jbvNull:
			return "null";
		case jbvDatetime:
			/* shim: in-memory-only type, cannot occur in a container */
			elog(ERROR, "unreachable jbvDatetime in JsonbTypeName");
			return "unknown";
		default:
			elog(ERROR, "unrecognized jsonb value type: %d", val->type);
			return "unknown";
	}
}

const char *
pg_JsonbContainerTypeName(JsonbContainer *jbc)
{
	JsonbValue	scalar;

	if (pg_JsonbExtractScalar(jbc, &scalar))
		return pg_JsonbTypeName(&scalar);
	else if (JsonContainerIsArray(jbc))
		return "array";
	else if (JsonContainerIsObject(jbc))
		return "object";
	else
	{
		elog(ERROR, "invalid jsonb container type: 0x%08x", jbc->header);
		return "unknown";
	}
}

/* ---------------- fmgr bodies, extracted (see file header) ------------- */

/*
 * jsonfuncs.c jsonb_array_length: PG_GETARG_JSONB_P -> container param;
 * the two ereports use the PROOF_EREPORT_FLAG convention (distinct values
 * so the harness can assert WHICH verdict fired); PG_RETURN_INT32 -> int.
 * JB_ROOT_* over the datum == JsonContainer* macros over its root.
 */
int
pgp_array_length(const JsonbContainer *c, int *err)
{
	if (JsonContainerIsScalar(c))
	{
		*err = 1;				/* "cannot get array length of a scalar" */
		return -1;
	}
	else if (!JsonContainerIsArray(c))
	{
		*err = 2;				/* "cannot get array length of a non-array" */
		return -1;
	}

	return (int) JsonContainerSize(c);
}

/*
 * Out-param flattening of the JsonbValue a lookup returns (result-image
 * materialization is out of scope). vdata/vlen: string bytes, nested
 * container window, or numeric image start; vbool: boolean value.
 */
static int
pgp_emit_value(const JsonbValue *v, int *vtype, const unsigned char **vdata,
			   int *vlen, int *vbool)
{
	*vtype = (int) v->type;
	*vdata = NULL;
	*vlen = -1;
	*vbool = -1;
	switch (v->type)
	{
		case jbvNull:
			break;
		case jbvString:
			*vdata = (const unsigned char *) v->val.string.val;
			*vlen = v->val.string.len;
			break;
		case jbvNumeric:
			*vdata = (const unsigned char *) v->val.numeric;
			break;
		case jbvBool:
			*vbool = v->val.boolean ? 1 : 0;
			break;
		case jbvBinary:
			*vdata = (const unsigned char *) v->val.binary.data;
			*vlen = v->val.binary.len;
			break;
		default:
			pg_proof_abort = 1;
			break;
	}
	return 1;
}

/*
 * jsonfuncs.c jsonb_object_field body: PG_GETARG unwrap -> params;
 * PG_RETURN_NULL -> 0; found value emitted via out-params instead of
 * JsonbValueToJsonb (materialization out of scope).
 */
int
pgp_object_field(JsonbContainer *c, const char *key, int keylen,
				 int *vtype, const unsigned char **vdata, int *vlen, int *vbool)
{
	JsonbValue *v;
	JsonbValue	vbuf;

	if (!JsonContainerIsObject(c))
		return 0;

	v = pg_getKeyJsonValueFromContainer(c, key, keylen, &vbuf);

	if (v != NULL)
		return pgp_emit_value(v, vtype, vdata, vlen, vbool);

	return 0;
}

/*
 * jsonfuncs.c jsonb_array_element body (incl. the negative-subscript
 * adjustment, verbatim with pg_abs_s32).
 */
int
pgp_array_element(JsonbContainer *c, int element,
				  int *vtype, const unsigned char **vdata, int *vlen, int *vbool)
{
	JsonbValue *v;

	if (!JsonContainerIsArray(c))
		return 0;

	/* Handle negative subscript */
	if (element < 0)
	{
		uint32		nelements = JsonContainerSize(c);

		if (pg_abs_s32(element) > nelements)
			return 0;
		else
			element += nelements;
	}

	v = pg_getIthJsonbValueFromContainer(c, element);
	if (v != NULL)
		return pgp_emit_value(v, vtype, vdata, vlen, vbool);

	return 0;
}

/*
 * jsonb_op.c jsonb_exists body: text key -> (ptr,len); PG_RETURN_BOOL -> int.
 */
int
pgp_exists(JsonbContainer *c, const char *key, int keylen)
{
	JsonbValue	kval;
	JsonbValue *v = NULL;

	kval.type = jbvString;
	kval.val.string.val = (char *) key;
	kval.val.string.len = keylen;

	v = pg_findJsonbValueFromContainer(c,
									   JB_FOBJECT | JB_FARRAY,
									   &kval);

	return v != NULL;
}

/* jsonb_cmp core (jsonb_op.c routes to compareJsonbContainers) */
int
pgp_cmp(JsonbContainer *a, JsonbContainer *b)
{
	return pg_compareJsonbContainers(a, b);
}

/*
 * SHIM (harness plumbing): pgp_cmp over the raw Rust byte images makes
 * every header/JEntry access a byte-reinterpretation of a u8-typed object,
 * which CBMC cannot track (measured: a fully-concrete empty-array compare
 * forked >11k symex paths / 489k SSA steps and exceeded 6GiB). This entry
 * point copies the images ONCE into uint32-typed C statics — same bytes,
 * same verbatim compareJsonbContainers — so the container reads are typed
 * array accesses. len is in bytes (<= 96 = the builder CAP, uint32-aligned).
 */
static uint32 pgp_img_a[24];
static uint32 pgp_img_b[24];

int
pgp_cmp_staged(const unsigned char *a, int alen, const unsigned char *b, int blen)
{
	int			i;

	/* explicit word assembly (little-endian, the only target here): the
	 * builtin memcpy loop would silently truncate under the tight harness
	 * unwind bounds; these two loops carry their own --unwindset entries
	 * (pgp_cmp_staged.0/.1:25) in run-one invocations. len honored so
	 * narrower harness images (Img<CMPCAP>) are never over-read. */
	for (i = 0; i < alen / 4; i++)
		pgp_img_a[i] = (uint32) a[4 * i]
			| ((uint32) a[4 * i + 1] << 8)
			| ((uint32) a[4 * i + 2] << 16)
			| ((uint32) a[4 * i + 3] << 24);
	for (i = 0; i < blen / 4; i++)
		pgp_img_b[i] = (uint32) b[4 * i]
			| ((uint32) b[4 * i + 1] << 8)
			| ((uint32) b[4 * i + 2] << 16)
			| ((uint32) b[4 * i + 3] << 24);
	return pg_compareJsonbContainers((JsonbContainer *) pgp_img_a,
									 (JsonbContainer *) pgp_img_b);
}

/* iterator cost probe (harness diagnostics, not a parity entry) */
int
pgp_iter_probe(const unsigned char *a, int ncalls)
{
	JsonbIterator *it;
	JsonbValue	v;
	int			i;
	int			acc = 0;

	for (i = 0; i < 24; i++)
		pgp_img_a[i] = (uint32) a[4 * i]
			| ((uint32) a[4 * i + 1] << 8)
			| ((uint32) a[4 * i + 2] << 16)
			| ((uint32) a[4 * i + 3] << 24);
	it = pg_JsonbIteratorInit((JsonbContainer *) pgp_img_a);
	for (i = 0; i < ncalls; i++)
		acc = acc * 10 + (int) pg_JsonbIteratorNext(&it, &v, false);
	return acc;
}

/* typeof core: returns the C name string (harness compares bytes) */
const char *
pgp_typeof_name(JsonbContainer *c)
{
	return pg_JsonbContainerTypeName(c);
}
