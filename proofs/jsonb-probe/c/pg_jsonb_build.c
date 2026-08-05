/*
 * Vendored PostgreSQL C for the empty-builder rows (jsonb-probe wave 2):
 * oids 3272 jsonb_build_array_noargs / 3274 jsonb_build_object_noargs.
 * Self-contained TU (no deps on pg_jsonb.c).
 *
 * Provenance (REL_18_STABLE, fetched 2026-07-30 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/backend/utils/adt/jsonb.c: jsonb_build_array_noargs /
 *     jsonb_build_object_noargs (fmgr bodies; both are
 *     push(BEGIN)+push(END)+JsonbValueToJsonb sequences)
 *   - src/backend/utils/adt/jsonb_util.c: JsonbValueToJsonb,
 *     pushJsonbValue (scalar drop-through), pushJsonbValueScalar,
 *     pushState, uniqueifyJsonbObject, convertToJsonb, convertJsonbValue,
 *     convertJsonbArray, convertJsonbObject, reserveFromBuffer,
 *     copyToBuffer, appendToBuffer, padBufferToInt — bodies verbatim,
 *     PRUNED to the members reachable from the two noargs builders (the
 *     element/key/value appenders and convertJsonbScalar are unreachable
 *     with zero args and are rewired to the abort sentinel so any reach is
 *     a loud harness failure, not silent divergence).
 *
 * SHIMS (everything else verbatim):
 *  B1. fmgr: PG_RETURN_POINTER(JsonbValueToJsonb(res)) -> the varlena
 *      image is written into a static out-buffer; the entry returns its
 *      length (harness compares bytes).
 *  B2. StringInfo: initStringInfo/enlargeStringInfo -> fixed static
 *      256-byte buffer model (allocation plumbing; an overflow aborts).
 *      SET_VARSIZE transcribed as the little-endian len<<2 word
 *      (varattrib_4b target convention).
 *  B3. palloc/repalloc -> static bump pool (family convention);
 *      qsort_arg in uniqueifyJsonbObject is guarded by nPairs > 1 and
 *      unreachable here -> abort sentinel.
 *  B4. check_stack_depth() -> no-op; ereport limit checks unreachable at
 *      zero elements (kept verbatim where the surrounding code is kept).
 */

#include <stddef.h>
#include <string.h>
#include "../../support/c/pg_proof_shim.h"

typedef uint32 JEntry;

#define JENTRY_OFFLENMASK		0x0FFFFFFF
#define JENTRY_TYPEMASK			0x70000000
#define JENTRY_HAS_OFF			0x80000000
#define JENTRY_ISCONTAINER		0x50000000
#define JBE_OFFLENFLD(je_)		((je_) & JENTRY_OFFLENMASK)

#define JB_CMASK				0x0FFFFFFF
#define JB_FSCALAR				0x10000000
#define JB_FOBJECT				0x20000000
#define JB_FARRAY				0x40000000

#define JB_OFFSET_STRIDE		32

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

typedef struct JsonbValue JsonbValue;
typedef struct JsonbPair JsonbPair;

struct JsonbValue
{
	enum jbvType type;
	union
	{
		void	   *numeric;
		bool		boolean;
		struct
		{
			int			len;
			char	   *val;
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
			JsonbPair  *pairs;
		}			object;
		struct
		{
			int			len;
			void	   *data;
		}			binary;
	}			val;
};

struct JsonbPair
{
	JsonbValue	key;
	JsonbValue	value;
	uint32		order;
};

#define IsAJsonbScalar(jsonbval)	((jsonbval)->type >= jbvNull && \
									 (jsonbval)->type <= jbvBool)

typedef struct JsonbParseState
{
	JsonbValue	contVal;
	Size		size;
	struct JsonbParseState *next;
	bool		unique_keys;
	bool		skip_nulls;
} JsonbParseState;

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

/* ---- TU-local plumbing ---- */

static int	pg_build_abort = 0;

int
pgp_build_take_abort(void)
{
	int			a = pg_build_abort;

	pg_build_abort = 0;
	return a;
}

#define elog(level, ...) (pg_build_abort = 1)
#define ereport(level, rest) (pg_build_abort = 1)
#define check_stack_depth() ((void) 0)

#define BUILD_POOL_CAP 512
static unsigned char build_pool[BUILD_POOL_CAP] __attribute__((aligned(8)));
static size_t build_pool_next = 0;

static void *
palloc(Size size)
{
	size_t		start = (build_pool_next + 7) & ~(size_t) 7;

	if (start + size > BUILD_POOL_CAP)
	{
		pg_build_abort = 1;
		start = 0;
	}
	build_pool_next = start + size;
	return build_pool + start;
}

/* SHIM B2: StringInfo model — fixed buffer, len tracked; enlarge = cap
 * check.  data points at a static array; '\0' maintenance verbatim. */
#define BUILD_BUF_CAP 256
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
} StringInfoData;
typedef StringInfoData *StringInfo;

static char build_sibuf[BUILD_BUF_CAP];

static void
initStringInfo(StringInfo str)
{
	str->data = build_sibuf;
	str->len = 0;
	str->maxlen = BUILD_BUF_CAP;
}

static void
enlargeStringInfo(StringInfo str, int needed)
{
	if (str->len + needed + 1 > str->maxlen)
		pg_build_abort = 1;		/* model cap; fenced (empty containers) */
}

/* ---- jsonb_util.c writer subset (verbatim bodies) ---- */

static int
reserveFromBuffer(StringInfo buffer, int len)
{
	int			offset;

	/* Make more room if needed */
	enlargeStringInfo(buffer, len);

	/* remember current offset */
	offset = buffer->len;

	/* reserve the space */
	buffer->len += len;

	/*
	 * Keep a trailing null in place, even though it's not useful for us; it
	 * seems best to preserve the invariants of StringInfos.
	 */
	buffer->data[buffer->len] = '\0';

	return offset;
}

static void
copyToBuffer(StringInfo buffer, int offset, const void *data, int len)
{
	memcpy(buffer->data + offset, data, len);
}

static void
appendToBuffer(StringInfo buffer, const void *data, int len)
{
	int			offset;

	offset = reserveFromBuffer(buffer, len);
	copyToBuffer(buffer, offset, data, len);
}

#define INTALIGN(LEN) (((uintptr_t) (LEN) + 3) & ~(uintptr_t) 3)

static short
padBufferToInt(StringInfo buffer)
{
	int			padlen,
				p,
				offset;

	padlen = (int) (INTALIGN(buffer->len) - buffer->len);

	offset = reserveFromBuffer(buffer, padlen);

	/* padlen must be small, so this is probably faster than a memset */
	for (p = 0; p < padlen; p++)
		buffer->data[offset + p] = '\0';

	return padlen;
}

static void convertJsonbValue(StringInfo buffer, JEntry *header, JsonbValue *val, int level);

static void
convertJsonbArray(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
	int			base_offset;
	int			jentry_offset;
	int			i;
	int			totallen;
	uint32		containerhead;
	int			nElems = val->val.array.nElems;

	/* Remember where in the buffer this array starts. */
	base_offset = buffer->len;

	/* Align to 4-byte boundary (any padding counts as part of my data) */
	padBufferToInt(buffer);

	/*
	 * Construct the header Jentry and store it in the beginning of the
	 * variable-length payload.
	 */
	containerhead = nElems | JB_FARRAY;
	if (val->val.array.rawScalar)
	{
		Assert(nElems == 1);
		Assert(level == 0);
		containerhead |= JB_FSCALAR;
	}

	appendToBuffer(buffer, &containerhead, sizeof(uint32));

	/* Reserve space for the JEntries of the elements. */
	jentry_offset = reserveFromBuffer(buffer, sizeof(JEntry) * nElems);

	totallen = 0;
	for (i = 0; i < nElems; i++)
	{
		/* unreachable at nElems == 0 (noargs builders); see file header */
		pg_build_abort = 1;
		(void) jentry_offset;
	}

	/* Total data size is everything we've appended to buffer */
	totallen = buffer->len - base_offset;

	/* Check length again, since we didn't include the metadata above */
	if (totallen > (int) JENTRY_OFFLENMASK)
		ereport(ERROR, unreachable);

	/* Initialize the header of this node in the container's JEntry array */
	*header = JENTRY_ISCONTAINER | totallen;
}

static void
convertJsonbObject(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
	int			base_offset;
	int			jentry_offset;
	int			i;
	int			totallen;
	uint32		containerheader;
	int			nPairs = val->val.object.nPairs;

	/* Remember where in the buffer this object starts. */
	base_offset = buffer->len;

	/* Align to 4-byte boundary (any padding counts as part of my data) */
	padBufferToInt(buffer);

	/* Initialize pointer into conversion buffer at this level */
	containerheader = nPairs | JB_FOBJECT;
	appendToBuffer(buffer, &containerheader, sizeof(uint32));

	/* Reserve space for the JEntries of the keys and values. */
	jentry_offset = reserveFromBuffer(buffer, sizeof(JEntry) * nPairs * 2);

	totallen = 0;
	for (i = 0; i < nPairs; i++)
	{
		/* unreachable at nPairs == 0 (noargs builders); see file header */
		pg_build_abort = 1;
		(void) jentry_offset;
	}

	/* Total data size is everything we've appended to buffer */
	totallen = buffer->len - base_offset;

	/* Check length again, since we didn't include the metadata above */
	if (totallen > (int) JENTRY_OFFLENMASK)
		ereport(ERROR, unreachable);

	/* Initialize the header of this node in the container's JEntry array */
	*header = JENTRY_ISCONTAINER | totallen;
}

static void
convertJsonbValue(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
	check_stack_depth();

	if (!val)
		return;

	if (IsAJsonbScalar(val))
		pg_build_abort = 1;		/* convertJsonbScalar unreachable (noargs) */
	else if (val->type == jbvArray)
		convertJsonbArray(buffer, header, val, level);
	else if (val->type == jbvObject)
		convertJsonbObject(buffer, header, val, level);
	else
		elog(ERROR, "unknown type of jsonb container to convert");
}

/* convertToJsonb (SHIM B1/B2): image into the model buffer; length
 * returned by the entry points. SET_VARSIZE = LE len<<2 word. */
static int
convertToJsonb(JsonbValue *val)
{
	StringInfoData buffer;
	JEntry		jentry;

	/* Should not already have binary representation */
	Assert(val->type != jbvBinary);

	/* Allocate an output buffer. It will be enlarged as needed */
	initStringInfo(&buffer);

	/* Make room for the varlena header */
	reserveFromBuffer(&buffer, VARHDRSZ);

	convertJsonbValue(&buffer, &jentry, val, 0);

	/* SET_VARSIZE(res, buffer.len) */
	{
		uint32		w = ((uint32) buffer.len) << 2;

		memcpy(buffer.data, &w, sizeof(uint32));
	}

	return buffer.len;
}

/* ---- pushJsonbValue subset (verbatim; scalar drop-through only) ---- */

static JsonbParseState *
pushState(JsonbParseState **pstate)
{
	JsonbParseState *ns = palloc(sizeof(JsonbParseState));

	ns->next = *pstate;
	ns->unique_keys = false;
	ns->skip_nulls = false;

	return ns;
}

static void
uniqueifyJsonbObject(JsonbValue *object, bool unique_keys, bool skip_nulls)
{
	bool		hasNonUniq = false;

	Assert(object->type == jbvObject);

	if (object->val.object.nPairs > 1)
		pg_build_abort = 1;		/* qsort_arg unreachable (0 pairs) */

	if (hasNonUniq && unique_keys)
		ereport(ERROR, unreachable);

	if (hasNonUniq || skip_nulls)
		pg_build_abort = 1;		/* dedup walk unreachable here */
}

static JsonbValue *
pushJsonbValueScalar(JsonbParseState **pstate, JsonbIteratorToken seq,
					 JsonbValue *scalarVal)
{
	JsonbValue *result = NULL;

	switch (seq)
	{
		case WJB_BEGIN_ARRAY:
			Assert(!scalarVal || scalarVal->val.array.rawScalar);
			*pstate = pushState(pstate);
			result = &(*pstate)->contVal;
			(*pstate)->contVal.type = jbvArray;
			(*pstate)->contVal.val.array.nElems = 0;
			(*pstate)->contVal.val.array.rawScalar = (scalarVal &&
													  scalarVal->val.array.rawScalar);
			if (scalarVal && scalarVal->val.array.nElems > 0)
			{
				/* Assume that this array is still really a scalar */
				Assert(scalarVal->type == jbvArray);
				(*pstate)->size = scalarVal->val.array.nElems;
			}
			else
			{
				(*pstate)->size = 4;
			}
			(*pstate)->contVal.val.array.elems = palloc(sizeof(JsonbValue) *
														(*pstate)->size);
			break;
		case WJB_BEGIN_OBJECT:
			Assert(!scalarVal);
			*pstate = pushState(pstate);
			result = &(*pstate)->contVal;
			(*pstate)->contVal.type = jbvObject;
			(*pstate)->contVal.val.object.nPairs = 0;
			(*pstate)->size = 4;
			(*pstate)->contVal.val.object.pairs = palloc(sizeof(JsonbPair) *
														 (*pstate)->size);
			break;
		case WJB_KEY:
		case WJB_VALUE:
		case WJB_ELEM:
			/* append* unreachable from the noargs builders */
			pg_build_abort = 1;
			break;
		case WJB_END_OBJECT:
			uniqueifyJsonbObject(&(*pstate)->contVal,
								 (*pstate)->unique_keys,
								 (*pstate)->skip_nulls);
			/* fall through! */
		case WJB_END_ARRAY:
			/* Steps here common to WJB_END_OBJECT case */
			Assert(!scalarVal);
			result = &(*pstate)->contVal;

			/*
			 * Pop stack and push current array/object as value in parent
			 * array/object
			 */
			*pstate = (*pstate)->next;
			if (*pstate)
				pg_build_abort = 1;	/* nested parents unreachable (depth 1) */
			break;
		default:
			elog(ERROR, "unrecognized jsonb sequential processing token");
	}

	return result;
}

static JsonbValue *
pushJsonbValue(JsonbParseState **pstate, JsonbIteratorToken seq,
			   JsonbValue *jbval)
{
	/* jsonb_util.c pushJsonbValue: with jbval == NULL or non-jbvBinary
	 * scalars this is the drop-through to pushJsonbValueScalar (the
	 * object/array/binary unpack arms are unreachable from the noargs
	 * builders). */
	if (jbval && jbval->type == jbvBinary)
		pg_build_abort = 1;
	return pushJsonbValueScalar(pstate, seq, jbval);
}

/* ---- jsonb.c fmgr bodies (SHIM B1) ---- */

int
pgp_build_array_noargs(unsigned char *out, int outcap)
{
	JsonbParseState *pstate = NULL;
	JsonbValue *result;
	int			len;

	build_pool_next = 0;

	(void) pushJsonbValue(&pstate, WJB_BEGIN_ARRAY, NULL);
	result = pushJsonbValue(&pstate, WJB_END_ARRAY, NULL);

	len = convertToJsonb(result);
	if (len > outcap)
	{
		pg_build_abort = 1;
		return -1;
	}
	memcpy(out, build_sibuf, len);
	return len;
}

int
pgp_build_object_noargs(unsigned char *out, int outcap)
{
	JsonbParseState *pstate = NULL;
	JsonbValue *result;
	int			len;

	build_pool_next = 0;

	(void) pushJsonbValue(&pstate, WJB_BEGIN_OBJECT, NULL);
	result = pushJsonbValue(&pstate, WJB_END_OBJECT, NULL);

	len = convertToJsonb(result);
	if (len > outcap)
	{
		pg_build_abort = 1;
		return -1;
	}
	memcpy(out, build_sibuf, len);
	return len;
}
