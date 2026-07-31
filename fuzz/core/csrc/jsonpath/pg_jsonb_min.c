/*
 * pg_jsonb_min.c — VERBATIM extracts for the jsonpathexec_diff oracle
 * (lane p1-laneaa, crate adt/jsonpath_exec): exactly the call graph that
 * jsonpath_exec.c + jsonb_util.c need beyond the jsonpath_diff family's
 * existing extracts. Every extract carries a "<file>:A-B VERBATIM" marker
 * (mechanically emitted by extract_verbatim.py). Sources @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3):
 *   - jsonb.c: JsonbExtractScalar
 *   - varlena.c: cstring_to_text(_with_len), text_to_cstring, varstr_cmp,
 *     check_collation_set
 *   - regexp.c: the RE_compile_and_execute chain incl. the compiled-regex
 *     cache (cache statics declared HERE as _Thread_local, reset by
 *     pg_jsonpath_regex_cache_reset() at every pg_diff entry — the cache
 *     memory lives in the per-entry TLS arena; thread-locality + per-entry
 *     reset are the ONLY deviations, both environment not logic)
 *   - int.c/int8.c: int4in, int8in; bool.c: parse_bool(_with_len)
 *   - numutils.c: pg_strtoint64(_safe), pg_ltoa
 *   - float.c: float8in_internal
 * SHIM (non-verbatim, environment only): pg_strncoll is unreachable under
 * the pinned C-ctype default collation (varstr_cmp's collate_is_c arm wins)
 * and is a LOUD ABORT stub in pg_jsonpath_exec_env.c.
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "nodes/miscnodes.h"
#include "regex/regex.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/jsonb.h"
#include "utils/pg_locale.h"
#include "utils/varlena.h"

#include <ctype.h>
#include <errno.h>
#include <math.h>

/* varlena.c fwd decl (declared at varlena.c:150) */
static void check_collation_set(Oid collid);

/* unreachable under the C-ctype pin (see header): loud stub lives in
 * pg_jsonpath_exec_env.c */
extern int	pg_strncoll(const char *arg1, ssize_t len1,
						const char *arg2, ssize_t len2, pg_locale_t locale);

/* ---- jsonb.c:155-174 VERBATIM (JsonbContainerTypeName) ---- */
/*
 * Get the type name of a jsonb container.
 */
static const char *
JsonbContainerTypeName(JsonbContainer *jbc)
{
	JsonbValue	scalar;

	if (JsonbExtractScalar(jbc, &scalar))
		return JsonbTypeName(&scalar);
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

/* ---- jsonb.c:1964-2002 VERBATIM (JsonbExtractScalar) ---- */
/*
 * Extract scalar value from raw-scalar pseudo-array jsonb.
 */
bool
JsonbExtractScalar(JsonbContainer *jbc, JsonbValue *res)
{
	JsonbIterator *it;
	JsonbIteratorToken tok PG_USED_FOR_ASSERTS_ONLY;
	JsonbValue	tmp;

	if (!JsonContainerIsArray(jbc) || !JsonContainerIsScalar(jbc))
	{
		/* inform caller about actual type of container */
		res->type = (JsonContainerIsArray(jbc)) ? jbvArray : jbvObject;
		return false;
	}

	/*
	 * A root scalar is stored as an array of one element, so we get the array
	 * and then its first (and only) member.
	 */
	it = JsonbIteratorInit(jbc);

	tok = JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_BEGIN_ARRAY);
	Assert(tmp.val.array.nElems == 1 && tmp.val.array.rawScalar);

	tok = JsonbIteratorNext(&it, res, true);
	Assert(tok == WJB_ELEM);
	Assert(IsAJsonbScalar(res));

	tok = JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_END_ARRAY);

	tok = JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_DONE);

	return true;
}
/* ---- jsonb.c:176-220 VERBATIM (JsonbTypeName) ---- */
/*
 * Get the type name of a jsonb value.
 */
const char *
JsonbTypeName(JsonbValue *val)
{
	switch (val->type)
	{
		case jbvBinary:
			return JsonbContainerTypeName(val->val.binary.data);
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
			switch (val->val.datetime.typid)
			{
				case DATEOID:
					return "date";
				case TIMEOID:
					return "time without time zone";
				case TIMETZOID:
					return "time with time zone";
				case TIMESTAMPOID:
					return "timestamp without time zone";
				case TIMESTAMPTZOID:
					return "timestamp with time zone";
				default:
					elog(ERROR, "unrecognized jsonb value datetime type: %d",
						 val->val.datetime.typid);
			}
			return "unknown";
		default:
			elog(ERROR, "unrecognized jsonb value type: %d", val->type);
			return "unknown";
	}
}

/* ---- varlena.c:185-196 VERBATIM (cstring_to_text) ---- */
/*
 * cstring_to_text
 *
 * Create a text value from a null-terminated C string.
 *
 * The new text value is freshly palloc'd with a full-size VARHDR.
 */
text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, strlen(s));
}

/* ---- varlena.c:198-213 VERBATIM (cstring_to_text_with_len) ---- */
/*
 * cstring_to_text_with_len
 *
 * Same as cstring_to_text except the caller specifies the string length;
 * the string need not be null_terminated.
 */
text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}

/* ---- varlena.c:215-241 VERBATIM (text_to_cstring) ---- */
/*
 * text_to_cstring
 *
 * Create a palloc'd, null-terminated C string from a text value.
 *
 * We support being passed a compressed or toasted text value.
 * This is a bit bogus since such values shouldn't really be referred to as
 * "text *", but it seems useful for robustness.  If we didn't handle that
 * case here, we'd need another routine that did, anyway.
 */
char *
text_to_cstring(const text *t)
{
	/* must cast away the const, unfortunately */
	text	   *tunpacked = pg_detoast_datum_packed(unconstify(text *, t));
	int			len = VARSIZE_ANY_EXHDR(tunpacked);
	char	   *result;

	result = (char *) palloc(len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';

	if (tunpacked != t)
		pfree(tunpacked);

	return result;
}

/* ---- varlena.c:1636-1650 VERBATIM (check_collation_set) ---- */
static void
check_collation_set(Oid collid)
{
	if (!OidIsValid(collid))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string comparison"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}
}

/* ---- varlena.c:1652-1707 VERBATIM (varstr_cmp) ---- */
/*
 * varstr_cmp()
 *
 * Comparison function for text strings with given lengths, using the
 * appropriate locale. Returns an integer less than, equal to, or greater than
 * zero, indicating whether arg1 is less than, equal to, or greater than arg2.
 *
 * Note: many functions that depend on this are marked leakproof; therefore,
 * avoid reporting the actual contents of the input when throwing errors.
 * All errors herein should be things that can't happen except on corrupt
 * data, anyway; otherwise we will have trouble with indexing strings that
 * would cause them.
 */
int
varstr_cmp(const char *arg1, int len1, const char *arg2, int len2, Oid collid)
{
	int			result;
	pg_locale_t mylocale;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->collate_is_c)
	{
		result = memcmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
	}
	else
	{
		/*
		 * memcmp() can't tell us which of two unequal strings sorts first,
		 * but it's a cheap way to tell if they're equal.  Testing shows that
		 * memcmp() followed by strcoll() is only trivially slower than
		 * strcoll() by itself, so we don't lose much if this doesn't work out
		 * very often, and if it does - for example, because there are many
		 * equal strings in the input - then we win big by avoiding expensive
		 * collation-aware comparisons.
		 */
		if (len1 == len2 && memcmp(arg1, arg2, len1) == 0)
			return 0;

		result = pg_strncoll(arg1, len1, arg2, len2, mylocale);

		/* Break tie if necessary. */
		if (result == 0 && mylocale->deterministic)
		{
			result = memcmp(arg1, arg2, Min(len1, len2));
			if ((result == 0) && (len1 != len2))
				result = (len1 < len2) ? -1 : 1;
		}
	}

	return result;
}
/* ---- bool.c:24-34 VERBATIM (parse_bool) ---- */
/*
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
bool
parse_bool(const char *value, bool *result)
{
	return parse_bool_with_len(value, strlen(value), result);
}

/* ---- bool.c:36-117 VERBATIM (parse_bool_with_len) ---- */
bool
parse_bool_with_len(const char *value, size_t len, bool *result)
{
	/* Check the most-used possibilities first. */
	switch (*value)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'y':
		case 'Y':
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'n':
		case 'N':
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = false;		/* suppress compiler warning */
	return false;
}
/* ---- int.c:312-321 VERBATIM (int4in) ---- */
/*
 *		int4in			- converts "num" to int4
 */
Datum
int4in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);

	PG_RETURN_INT32(pg_strtoint32_safe(num, fcinfo->context));
}
/* ---- int8.c:47-55 VERBATIM (int8in) ---- */
/* int8in()
 */
Datum
int8in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);

	PG_RETURN_INT64(pg_strtoint64_safe(num, fcinfo->context));
}
/* ---- float.c:372-514 VERBATIM (float8in_internal) ---- */
/*
 * float8in_internal - guts of float8in()
 *
 * This is exposed for use by functions that want a reasonably
 * platform-independent way of inputting doubles.  The behavior is
 * essentially like strtod + ereturn on error, but note the following
 * differences:
 * 1. Both leading and trailing whitespace are skipped.
 * 2. If endptr_p is NULL, we report error if there's trailing junk.
 * Otherwise, it's up to the caller to complain about trailing junk.
 * 3. In event of a syntax error, the report mentions the given type_name
 * and prints orig_string as the input; this is meant to support use of
 * this function with types such as "box" and "point", where what we are
 * parsing here is just a substring of orig_string.
 *
 * If escontext points to an ErrorSaveContext node, that is filled instead
 * of throwing an error; the caller must check SOFT_ERROR_OCCURRED()
 * to detect errors.
 *
 * "num" could validly be declared "const char *", but that results in an
 * unreasonable amount of extra casting both here and in callers, so we don't.
 */
float8
float8in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string,
				  struct Node *escontext)
{
	double		val;
	char	   *endptr;

	/* skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	/*
	 * Check for an empty-string input to begin with, to avoid the vagaries of
	 * strtod() on different platforms.
	 */
	if (*num == '\0')
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						type_name, orig_string)));

	errno = 0;
	val = strtod(num, &endptr);

	/* did we not see anything that looks like a double? */
	if (endptr == num || errno != 0)
	{
		int			save_errno = errno;

		/*
		 * C99 requires that strtod() accept NaN, [+-]Infinity, and [+-]Inf,
		 * but not all platforms support all of these (and some accept them
		 * but set ERANGE anyway...)  Therefore, we check for these inputs
		 * ourselves if strtod() fails.
		 *
		 * Note: C99 also requires hexadecimal input as well as some extended
		 * forms of NaN, but we consider these forms unportable and don't try
		 * to support them.  You can use 'em if your strtod() takes 'em.
		 */
		if (pg_strncasecmp(num, "NaN", 3) == 0)
		{
			val = get_float8_nan();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "Infinity", 8) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 8;
		}
		else if (pg_strncasecmp(num, "+Infinity", 9) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "-Infinity", 9) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "inf", 3) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "+inf", 4) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 4;
		}
		else if (pg_strncasecmp(num, "-inf", 4) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 4;
		}
		else if (save_errno == ERANGE)
		{
			/*
			 * Some platforms return ERANGE for denormalized numbers (those
			 * that are not zero, but are too close to zero to have full
			 * precision).  We'd prefer not to throw error for that, so try to
			 * detect whether it's a "real" out-of-range condition by checking
			 * to see if the result is zero or huge.
			 *
			 * On error, we intentionally complain about double precision not
			 * the given type name, and we print only the part of the string
			 * that is the current number.
			 */
			if (val == 0.0 || val >= HUGE_VAL || val <= -HUGE_VAL)
			{
				char	   *errnumber = pstrdup(num);

				errnumber[endptr - num] = '\0';
				ereturn(escontext, 0,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("\"%s\" is out of range for type double precision",
								errnumber)));
			}
		}
		else
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							type_name, orig_string)));
	}

	/* skip trailing whitespace */
	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	/* report stopping point if wanted, else complain if not end of string */
	if (endptr_p)
		*endptr_p = endptr;
	else if (*endptr != '\0')
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						type_name, orig_string)));

	return val;
}

/* ---------------------------------------------------------------------------
 * regexp.c: compiled-regex cache. The struct + constants are VERBATIM
 * (regexp.c:92-113); the statics are declared _Thread_local HERE and reset
 * per pg_diff entry (see file header — environment deviation only).
 * ------------------------------------------------------------------------ */
/* ---- regexp.c:92-110 VERBATIM (cached_re_str) ---- */

/* this is the maximum number of cached regular expressions */
#ifndef MAX_CACHED_RES
#define MAX_CACHED_RES	32
#endif

/* A parent memory context for regular expressions. */
static MemoryContext RegexpCacheMemoryContext;

/* this structure describes one cached regular expression */
typedef struct cached_re_str
{
	MemoryContext cre_context;	/* memory context for this regexp */
	char	   *cre_pat;		/* original RE (not null terminated!) */
	int			cre_pat_len;	/* length of original RE, in bytes */
	int			cre_flags;		/* compile flags: extended,icase etc */
	Oid			cre_collation;	/* collation to use */
	regex_t		cre_re;			/* the compiled regular expression */
} cached_re_str;

static _Thread_local int num_res = 0;	/* # of cached re's (SHIM: TLS) */
static _Thread_local cached_re_str re_array[MAX_CACHED_RES];	/* (SHIM: TLS) */

/*
 * SHIM (environment): every compiled regex + its cache bookkeeping lives in
 * the per-entry TLS arena; pg_diff_entry resets call this FIRST so no cache
 * slot ever dangles across an arena reset. Within one evaluation the cache
 * behaves exactly like PostgreSQL's.
 */
void
pg_jsonpath_regex_cache_reset(void)
{
	num_res = 0;
}

/* ---- regexp.c:128-265 VERBATIM (RE_compile_and_cache) ---- */
/*
 * RE_compile_and_cache - compile a RE, caching if possible
 *
 * Returns regex_t *
 *
 *	text_re --- the pattern, expressed as a TEXT object
 *	cflags --- compile options for the pattern
 *	collation --- collation to use for LC_CTYPE-dependent behavior
 *
 * Pattern is given in the database encoding.  We internally convert to
 * an array of pg_wchar, which is what Spencer's regex package wants.
 */
regex_t *
RE_compile_and_cache(text *text_re, int cflags, Oid collation)
{
	int			text_re_len = VARSIZE_ANY_EXHDR(text_re);
	char	   *text_re_val = VARDATA_ANY(text_re);
	pg_wchar   *pattern;
	int			pattern_len;
	int			i;
	int			regcomp_result;
	cached_re_str re_temp;
	char		errMsg[100];
	MemoryContext oldcontext;

	/*
	 * Look for a match among previously compiled REs.  Since the data
	 * structure is self-organizing with most-used entries at the front, our
	 * search strategy can just be to scan from the front.
	 */
	for (i = 0; i < num_res; i++)
	{
		if (re_array[i].cre_pat_len == text_re_len &&
			re_array[i].cre_flags == cflags &&
			re_array[i].cre_collation == collation &&
			memcmp(re_array[i].cre_pat, text_re_val, text_re_len) == 0)
		{
			/*
			 * Found a match; move it to front if not there already.
			 */
			if (i > 0)
			{
				re_temp = re_array[i];
				memmove(&re_array[1], &re_array[0], i * sizeof(cached_re_str));
				re_array[0] = re_temp;
			}

			return &re_array[0].cre_re;
		}
	}

	/* Set up the cache memory on first go through. */
	if (unlikely(RegexpCacheMemoryContext == NULL))
		RegexpCacheMemoryContext =
			AllocSetContextCreate(TopMemoryContext,
								  "RegexpCacheMemoryContext",
								  ALLOCSET_SMALL_SIZES);

	/*
	 * Couldn't find it, so try to compile the new RE.  To avoid leaking
	 * resources on failure, we build into the re_temp local.
	 */

	/* Convert pattern string to wide characters */
	pattern = (pg_wchar *) palloc((text_re_len + 1) * sizeof(pg_wchar));
	pattern_len = pg_mb2wchar_with_len(text_re_val,
									   pattern,
									   text_re_len);

	/*
	 * Make a memory context for this compiled regexp.  This is initially a
	 * child of the current memory context, so it will be cleaned up
	 * automatically if compilation is interrupted and throws an ERROR. We'll
	 * re-parent it under the longer lived cache context if we make it to the
	 * bottom of this function.
	 */
	re_temp.cre_context = AllocSetContextCreate(CurrentMemoryContext,
												"RegexpMemoryContext",
												ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(re_temp.cre_context);

	regcomp_result = pg_regcomp(&re_temp.cre_re,
								pattern,
								pattern_len,
								cflags,
								collation);

	pfree(pattern);

	if (regcomp_result != REG_OKAY)
	{
		/* re didn't compile (no need for pg_regfree, if so) */
		pg_regerror(regcomp_result, &re_temp.cre_re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				 errmsg("invalid regular expression: %s", errMsg)));
	}

	/* Copy the pattern into the per-regexp memory context. */
	re_temp.cre_pat = palloc(text_re_len + 1);
	memcpy(re_temp.cre_pat, text_re_val, text_re_len);

	/*
	 * NUL-terminate it only for the benefit of the identifier used for the
	 * memory context, visible in the pg_backend_memory_contexts view.
	 */
	re_temp.cre_pat[text_re_len] = 0;
	MemoryContextSetIdentifier(re_temp.cre_context, re_temp.cre_pat);

	re_temp.cre_pat_len = text_re_len;
	re_temp.cre_flags = cflags;
	re_temp.cre_collation = collation;

	/*
	 * Okay, we have a valid new item in re_temp; insert it into the storage
	 * array.  Discard last entry if needed.
	 */
	if (num_res >= MAX_CACHED_RES)
	{
		--num_res;
		Assert(num_res < MAX_CACHED_RES);
		/* Delete the memory context holding the regexp and pattern. */
		MemoryContextDelete(re_array[num_res].cre_context);
	}

	/* Re-parent the memory context to our long-lived cache context. */
	MemoryContextSetParent(re_temp.cre_context, RegexpCacheMemoryContext);

	if (num_res > 0)
		memmove(&re_array[1], &re_array[0], num_res * sizeof(cached_re_str));

	re_array[0] = re_temp;
	num_res++;

	MemoryContextSwitchTo(oldcontext);

	return &re_array[0].cre_re;
}

/* ---- regexp.c:267-308 VERBATIM (RE_wchar_execute) ---- */
/*
 * RE_wchar_execute - execute a RE on pg_wchar data
 *
 * Returns true on match, false on no match
 *
 *	re --- the compiled pattern as returned by RE_compile_and_cache
 *	data --- the data to match against (need not be null-terminated)
 *	data_len --- the length of the data string
 *	start_search -- the offset in the data to start searching
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Data is given as array of pg_wchar which is what Spencer's regex package
 * wants.
 */
static bool
RE_wchar_execute(regex_t *re, pg_wchar *data, int data_len,
				 int start_search, int nmatch, regmatch_t *pmatch)
{
	int			regexec_result;
	char		errMsg[100];

	/* Perform RE match and return result */
	regexec_result = pg_regexec(re,
								data,
								data_len,
								start_search,
								NULL,	/* no details */
								nmatch,
								pmatch,
								0);

	if (regexec_result != REG_OKAY && regexec_result != REG_NOMATCH)
	{
		/* re failed??? */
		pg_regerror(regexec_result, re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				 errmsg("regular expression failed: %s", errMsg)));
	}

	return (regexec_result == REG_OKAY);
}

/* ---- regexp.c:310-340 VERBATIM (RE_execute) ---- */
/*
 * RE_execute - execute a RE
 *
 * Returns true on match, false on no match
 *
 *	re --- the compiled pattern as returned by RE_compile_and_cache
 *	dat --- the data to match against (need not be null-terminated)
 *	dat_len --- the length of the data string
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Data is given in the database encoding.  We internally
 * convert to array of pg_wchar which is what Spencer's regex package wants.
 */
static bool
RE_execute(regex_t *re, char *dat, int dat_len,
		   int nmatch, regmatch_t *pmatch)
{
	pg_wchar   *data;
	int			data_len;
	bool		match;

	/* Convert data string to wide characters */
	data = (pg_wchar *) palloc((dat_len + 1) * sizeof(pg_wchar));
	data_len = pg_mb2wchar_with_len(dat, data, dat_len);

	/* Perform RE match and return result */
	match = RE_wchar_execute(re, data, data_len, 0, nmatch, pmatch);

	pfree(data);
	return match;
}

/* ---- regexp.c:342-372 VERBATIM (RE_compile_and_execute) ---- */
/*
 * RE_compile_and_execute - compile and execute a RE
 *
 * Returns true on match, false on no match
 *
 *	text_re --- the pattern, expressed as a TEXT object
 *	dat --- the data to match against (need not be null-terminated)
 *	dat_len --- the length of the data string
 *	cflags --- compile options for the pattern
 *	collation --- collation to use for LC_CTYPE-dependent behavior
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Both pattern and data are given in the database encoding.  We internally
 * convert to array of pg_wchar which is what Spencer's regex package wants.
 */
bool
RE_compile_and_execute(text *text_re, char *dat, int dat_len,
					   int cflags, Oid collation,
					   int nmatch, regmatch_t *pmatch)
{
	regex_t    *re;

	/* Use REG_NOSUB if caller does not want sub-match details */
	if (nmatch < 2)
		cflags |= REG_NOSUB;

	/* Compile RE */
	re = RE_compile_and_cache(text_re, cflags, collation);

	return RE_execute(re, dat, dat_len, nmatch, pmatch);
}
