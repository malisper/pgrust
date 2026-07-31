/*
 * Vendored PostgreSQL C for the jsonb path-extraction row (oid 3217
 * jsonb_extract_path / #>; jsonb-probe wave 2).  Own TU, linked together
 * with c/pg_jsonb.c (container lookups + JsonbValue types live there).
 *
 * Provenance (REL_18_STABLE, fetched 2026-07-30 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/backend/utils/adt/jsonfuncs.c: jsonb_get_element (the
 *     path-resolution walk; fmgr body extracted per the shim rules)
 *   - src/common/string.c: strtoint (verbatim; errno as a TU-local model)
 *
 * SHIMS (everything else verbatim):
 *  P1. fmgr/datum unwrapping: Jsonb *jb -> JsonbContainer *c (pre-detoasted
 *      payload fence); Datum *path (text datums) -> parallel
 *      (const unsigned char **paths, const int *plens) arrays;
 *      TextDatumGetCString -> NUL-terminated staging copy of the element
 *      bytes (SQL text cannot contain NUL — harness fence).
 *      JB_ROOT_IS_* over the datum == JsonContainer* macros over its root.
 *  P2. RESULT MATERIALIZATION OUT OF SCOPE (as_text=false lane;
 *      JsonbValueToJsonb / PG_RETURN_JSONB_P replaced): the walk's verdict
 *      is returned instead — 0 = *isnull lane, 1 = final JsonbValue
 *      (flattened out-params, same convention as pgp_emit_value),
 *      2 = the empty-path hand-back-the-input lane.
 *  P3. strtol: Kani/CBMC has NO libc model, so strtoint's strtol call is
 *      rewired to pg_proof_strtol below — a TOTAL C-locale/glibc-semantics
 *      base-10 model (leading isspace() skip incl. '\v', optional sign,
 *      digit run, endptr = start when no conversion, ERANGE clamp to
 *      LONG_MIN/LONG_MAX) with errno as the TU-local pg_path_errno.  This
 *      is the same libc-model class as the shim header's pg_proof_isspace
 *      (documented seam: strtol INTERNALS leave the proof; its contract
 *      behavior is fully in-model and is exactly what the parity claim
 *      exercises).  The harness carries a skew control proving the model
 *      is load-bearing.
 *  P4. elog(ERROR, "not a jsonb array") -> TU-local abort flag
 *      (pgp_path_take_abort); fenced out (the walk only reaches it with
 *      have_array true, where the container IS an array).
 *  Assert -> no-op (shim header).
 */

#include <stddef.h>
#include <string.h>
#include "../../support/c/pg_proof_shim.h"

/* ---- jsonb.h declarations (byte-identical to pg_jsonb.c's block) ---- */

typedef uint32 JEntry;

typedef struct JsonbContainer
{
	uint32		header;
	JEntry		children[];
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

typedef void *Numeric;
typedef uintptr_t Datum;

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
			void	   *pairs;
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

/* provided by pg_jsonb.c */
extern JsonbValue *pg_getKeyJsonValueFromContainer(JsonbContainer *container,
												   const char *keyVal, int keyLen,
												   JsonbValue *res);
extern JsonbValue *pg_getIthJsonbValueFromContainer(JsonbContainer *container,
													uint32 i);

/* ---- TU-local plumbing ---- */

static int	pg_path_abort = 0;
static int	pg_path_errno = 0;	/* errno model (SHIM P3) */

#define PATH_ERANGE 34

int
pgp_path_take_abort(void)
{
	int			a = pg_path_abort;

	pg_path_abort = 0;
	return a;
}

/*
 * SHIM P3: total C-locale/glibc base-10 strtol model.  Contract per
 * C99/POSIX: skip isspace() prefix (C locale: " \t\n\v\f\r"), optional
 * sign, longest digit run; *endptr = first unconsumed char, or the
 * ORIGINAL str when no digits were consumed; on overflow clamp to
 * LONG_MIN/LONG_MAX and set errno = ERANGE.
 */
static long
pg_proof_strtol10(const char *str, char **endptr)
{
	const char *p = str;
	int			neg = 0;
	int			any = 0;
	unsigned long acc = 0;
	int			over = 0;

	while (pg_proof_isspace((unsigned char) *p))
		p++;
	if (*p == '-')
	{
		neg = 1;
		p++;
	}
	else if (*p == '+')
		p++;
	while (pg_proof_isdigit((unsigned char) *p))
	{
		unsigned long d = (unsigned long) (*p - '0');
		unsigned long lim = neg ? 0x8000000000000000UL : 0x7FFFFFFFFFFFFFFFUL;

		any = 1;
		if (!over)
		{
			if (acc > lim / 10UL || (acc == lim / 10UL && d > lim % 10UL))
				over = 1;
			else
				acc = acc * 10UL + d;
		}
		p++;
	}
	if (!any)
	{
		*endptr = (char *) str;
		return 0;
	}
	*endptr = (char *) p;
	if (over)
	{
		pg_path_errno = PATH_ERANGE;
		return neg ? (long) 0x8000000000000000UL : (long) 0x7FFFFFFFFFFFFFFFUL;
	}
	return neg ? -(long) acc : (long) acc;
}

/* src/common/string.c strtoint, verbatim body (strtol -> model, errno ->
 * pg_path_errno) */
static int
strtoint(const char *str, char **endptr, int base)
{
	long		val;

	val = pg_proof_strtol10(str, endptr);
	(void) base;				/* call sites pass 10 */
	if (val != (int) val)
		pg_path_errno = PATH_ERANGE;
	return (int) val;
}

/* out-param flattening of the final JsonbValue (same convention as
 * pg_jsonb.c pgp_emit_value; duplicated here — that one is TU-static) */
static int
path_emit_value(const JsonbValue *v, int *vtype, const unsigned char **vdata,
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
			pg_path_abort = 1;
			break;
	}
	return 1;
}

#define PATH_MAX_ELEMS 2
#define PATH_ELEM_CAP 12

/*
 * jsonfuncs.c jsonb_get_element, path-resolution walk (as_text = false
 * lane; SHIM P1/P2/P3/P4).  Returns 0 = *isnull, 1 = value found,
 * 2 = empty-path input hand-back.
 */
int
pgp_get_element(JsonbContainer *container,
				const unsigned char **paths, const int *plens, int npath,
				int *vtype, const unsigned char **vdata, int *vlen, int *vbool)
{
	JsonbValue *jbvp = NULL;
	int			i;
	bool		have_object = false,
				have_array = false;
	static char indexbuf[PATH_ELEM_CAP + 1];	/* TextDatumGetCString staging
												 * (SHIM P1) */

	/* Identify whether we have object, array, or scalar at top-level */
	if (JsonContainerIsObject(container))
		have_object = true;
	else if (JsonContainerIsArray(container) && !JsonContainerIsScalar(container))
		have_array = true;
	else
	{
		Assert(JsonContainerIsArray(container) && JsonContainerIsScalar(container));
		/* Extract the scalar value, if it is what we'll return */
		if (npath <= 0)
			jbvp = pg_getIthJsonbValueFromContainer(container, 0);
	}

	/*
	 * If the array is empty, return the entire LHS object, on the grounds
	 * that we should do zero field or element extractions.
	 */
	if (npath <= 0 && jbvp == NULL)
		return 2;				/* SHIM P2: the hand-back-the-input lane */

	for (i = 0; i < npath; i++)
	{
		if (have_object)
		{
			jbvp = pg_getKeyJsonValueFromContainer(container,
												   (const char *) paths[i],
												   plens[i],
												   NULL);
		}
		else if (have_array)
		{
			int			lindex;
			uint32		index;
			char	   *indextext = indexbuf;
			char	   *endptr;
			int			j;

			/* SHIM P1: TextDatumGetCString staging copy */
			for (j = 0; j < plens[i]; j++)
				indexbuf[j] = (char) paths[i][j];
			indexbuf[plens[i]] = '\0';

			pg_path_errno = 0;	/* errno = 0 */
			lindex = strtoint(indextext, &endptr, 10);
			if (endptr == indextext || *endptr != '\0' || pg_path_errno != 0)
				return 0;

			if (lindex >= 0)
			{
				index = (uint32) lindex;
			}
			else
			{
				/* Handle negative subscript */
				uint32		nelements;

				/* Container must be array, but make sure */
				if (!JsonContainerIsArray(container))
				{
					pg_path_abort = 1;	/* elog(ERROR, "not a jsonb array") */
					return 0;
				}

				nelements = JsonContainerSize(container);

				if (lindex == (-0x7FFFFFFF - 1) || -lindex > nelements)
					return 0;
				else
					index = nelements + lindex;
			}

			jbvp = pg_getIthJsonbValueFromContainer(container, index);
		}
		else
		{
			/* scalar, extraction yields a null */
			return 0;
		}

		if (jbvp == NULL)
			return 0;
		else if (i == npath - 1)
			break;

		if (jbvp->type == jbvBinary)
		{
			container = jbvp->val.binary.data;
			have_object = JsonContainerIsObject(container);
			have_array = JsonContainerIsArray(container);
			Assert(!JsonContainerIsScalar(container));
		}
		else
		{
			Assert(IsAJsonbScalar(jbvp));
			have_object = false;
			have_array = false;
		}
	}

	return path_emit_value(jbvp, vtype, vdata, vlen, vbool);
}
