/*
 * pg_define_io.c: vendored PostgreSQL C oracle for the define_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/commands/define).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below), from the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src
 * @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3):
 *   - src/backend/commands/define.c: defGetString, defGetBoolean — VERBATIM.
 *   - src/backend/parser/parse_type.c: appendTypeNameToBuffer,
 *     TypeNameToString — VERBATIM.
 *   - src/port/pgstrcasecmp.c: pg_strcasecmp — VERBATIM.
 *   - src/include/nodes/pg_list.h: foreach, list_head, lfirst, NIL, ListCell/
 *     ForEachState shapes — VERBATIM macro/struct text (subset).
 *   - src/include/nodes/value.h: intVal/floatVal/strVal/boolVal — VERBATIM.
 *
 * Shims (numbered; plumbing only, never logic):
 *   1. NodeTag: a local enum carrying ONLY the tags this family dispatches on
 *      (T_Integer/T_Float/T_Boolean/T_String/T_List/T_TypeName/T_A_Star).
 *      Numeric values differ from PG's generated nodetags.h; the only place a
 *      tag VALUE escapes is the elog(ERROR "unrecognized node type: %d")
 *      message text, which is out of scope (comparator checks errcode class).
 *   2. Node structs (DefElem, TypeName, List, Integer/Float/Boolean/String):
 *      field-for-field copies of the parsenodes.h/value.h/pg_list.h shapes,
 *      restricted to the fields this family reads. Fixture CONSTRUCTION (in
 *      the pg_diff_* driver entries) is environment, not computation.
 *   3. ereport(ERROR)/elog(ERROR) -> record an errcode CLASS in the shared
 *      _Thread_local pg_diff_errcode and longjmp back to the pg_diff_* entry
 *      (pg_rangetypes_io.c pattern). errmsg args are swallowed unevaluated.
 *   4. palloc family -> TLS pointer arena, reset at every pg_diff_* entry
 *      (scaffold convention; models PG's memory-context reset). StringInfo
 *      is a compact arena-backed shim (initStringInfo/appendStringInfoChar/
 *      appendStringInfoString/appendBinaryStringInfo): growth bookkeeping is
 *      lib/stringinfo.c's, not define.c logic, and this family's strings are
 *      driver-capped far below MaxAllocSize, so the 54000-ceiling arm of the
 *      real enlargeStringInfo is unreachable by construction.
 *   5. psprintf: fixed-buffer vsnprintf + arena pstrdup (pg_strfam.c
 *      pattern). Only ever called as psprintf("%ld", (long) int32) here, so
 *      the 4096-byte buffer bound is unreachable.
 *   6. format_type_be / NameListToString: abort() stubs. Both back arms the
 *      DRIVER DOMAIN EXCLUDES (TypeName with names==NIL; T_List / T_A_Star
 *      args) because the shipped Rust carries them as unported-arm panics —
 *      recorded exception rows, not fuzzed surface. abort() = loud shim-bug
 *      detector if the domain ever drifts.
 *   7. pg_strcasecmp uses tolower()/isupper() on high-bit bytes (locale-
 *      sensitive in C). The fuzz process runs under the C locale, where the
 *      high-bit arm is inert, matching the shipped Rust's ASCII-only
 *      eq_ignore_ascii_case. High-bit-set boolean svals thus compare
 *      byte-identically on both sides; no carve needed under C locale.
 *
 * Errcode classes (keep in sync with the Rust driver's sqlstate map):
 *   1 = ERRCODE_SYNTAX_ERROR (42601); 99 = internal elog.
 */

#include <assert.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

#define ERRCODE_SYNTAX_ERROR 1

/* ---------------- error machinery (shim 3) ---------------- */

static _Thread_local jmp_buf pg_define_jmp;

__attribute__((noreturn)) static void
pg_define_throw(void)
{
	if (pg_diff_errcode == 0)
		pg_diff_errcode = 99;
	longjmp(pg_define_jmp, 1);
}

#define errcode(c) (pg_diff_errcode = (c))
static int pg_define_errsink(const char *fmt, ...) { (void) fmt; return 0; }
#define errmsg(...) pg_define_errsink(__VA_ARGS__)
#define ereport(level, ...) \
	do { __VA_ARGS__; pg_define_throw(); } while (0)
#define elog(level, ...) \
	do { pg_diff_errcode = 99; pg_define_throw(); } while (0)

/* ---------------- palloc arena (shim 4; scaffold convention) ------------- */

#define PG_DIFF_ARENA_MAX 64
static _Thread_local void *pg_define_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_define_arena_n;

static void
pg_define_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_define_arena_n; i++)
		free(pg_define_arena[i]);
	pg_define_arena_n = 0;
}

static void *
pg_define_palloc(size_t n)
{
	void	   *p = malloc(n ? n : 1);

	if (!p)
		abort();
	assert(pg_define_arena_n < PG_DIFF_ARENA_MAX);
	pg_define_arena[pg_define_arena_n++] = p;
	return p;
}

static void *
pg_define_arena_realloc(void *old, size_t n)
{
	int			i;

	for (i = 0; i < pg_define_arena_n; i++)
	{
		if (pg_define_arena[i] == old)
		{
			void	   *p = realloc(old, n);

			if (!p)
				abort();
			pg_define_arena[i] = p;
			return p;
		}
	}
	assert(!"realloc of a pointer the arena never issued");
	abort();
}

static char *
pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *r = pg_define_palloc(n);

	memcpy(r, s, n);
	return r;
}

/* psprintf (shim 5): only ever "%ld" of an int32 in this family. */
static char *
psprintf(const char *fmt, ...)
{
	char		buf[4096];
	va_list		ap;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	return pstrdup(buf);
}

/* ---------------- node environment (shims 1+2) ---------------- */

typedef enum NodeTag
{
	T_Invalid = 0,
	T_Integer,
	T_Float,
	T_Boolean,
	T_String,
	T_List,
	T_TypeName,
	T_A_Star,
	T_DefElem
} NodeTag;

typedef struct Node { NodeTag type; } Node;
typedef unsigned int Oid;
typedef int ParseLoc;

#define nodeTag(nodeptr) (((const Node *) (nodeptr))->type)

/* value.h node shapes (fields verbatim) */
typedef struct Integer { NodeTag type; int ival; } Integer;
typedef struct Float { NodeTag type; char *fval; } Float;
typedef struct Boolean { NodeTag type; bool boolval; } Boolean;
typedef struct String { NodeTag type; char *sval; } String;

/* value.h accessor macros — VERBATIM */
#define intVal(v)		(((Integer *) (v))->ival)
#define floatVal(v)		atof(((Float *) (v))->fval)
#define boolVal(v)		(((Boolean *) (v))->boolval)
#define strVal(v)		(((String *) (v))->sval)

/* castNode: shim of nodes.h castNode (assert-checked cast) */
#define castNode(_type_, nodeptr) \
	(assert(nodeTag(nodeptr) == T_##_type_), (_type_ *) (nodeptr))

/* pg_list.h shapes (subset; fields verbatim) */
typedef union ListCell
{
	void	   *ptr_value;
	int			int_value;
	Oid			oid_value;
} ListCell;

typedef struct List
{
	NodeTag		type;
	int			length;
	int			max_length;
	ListCell   *elements;
} List;

#define NIL						((List *) NULL)

typedef struct ForEachState
{
	const List *l;
	int			i;
} ForEachState;

static ListCell *
list_head(const List *l)
{
	return l ? &l->elements[0] : NULL;
}

#define lfirst(lc)				((lc)->ptr_value)

/* foreach — VERBATIM (pg_list.h) */
#define foreach(cell, lst)	\
	for (ForEachState cell##__state = {(lst), 0}; \
		 (cell##__state.l != NIL && \
		  cell##__state.i < cell##__state.l->length) ? \
		 (cell = &cell##__state.l->elements[cell##__state.i], true) : \
		 (cell = NULL, false); \
		 cell##__state.i++)

/* parsenodes.h shapes (subset; fields verbatim, tail fields elided) */
typedef struct TypeName
{
	NodeTag		type;
	List	   *names;
	Oid			typeOid;
	bool		setof;
	bool		pct_type;
	List	   *typmods;
	int32_t		typemod;
	List	   *arrayBounds;
	ParseLoc	location;
} TypeName;

typedef struct DefElem
{
	NodeTag		type;
	char	   *defnamespace;
	char	   *defname;
	Node	   *arg;
	int			defaction;
	ParseLoc	location;
} DefElem;

/* ---------------- StringInfo (shim 4, compact arena-backed) -------------- */

typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
} StringInfoData;
typedef StringInfoData *StringInfo;

static void
initStringInfo(StringInfo str)
{
	str->maxlen = 1024;
	str->data = pg_define_palloc(str->maxlen);
	str->len = 0;
	str->data[0] = '\0';
}

static void
appendBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
	if (str->len + datalen + 1 > str->maxlen)
	{
		while (str->len + datalen + 1 > str->maxlen)
			str->maxlen *= 2;
		str->data = pg_define_arena_realloc(str->data, str->maxlen);
	}
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

static void
appendStringInfoChar(StringInfo str, char ch)
{
	appendBinaryStringInfo(str, &ch, 1);
}

static void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, (int) strlen(s));
}

/* ---------------- out-of-domain stubs (shim 6) ---------------- */

static const char *
format_type_be(Oid t)
{
	(void) t;
	assert(!"format_type_be: TypeName with names==NIL is outside the driver domain");
	abort();
}

static char *
NameListToString(const List *names)
{
	(void) names;
	assert(!"NameListToString: T_List args are outside the driver domain");
	abort();
}

/* ================================================================
 * src/port/pgstrcasecmp.c — VERBATIM (shim 7 note: C locale pinned)
 * ================================================================ */

#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)

/*
 * Case-independent comparison of two null-terminated strings.
 */
static int
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

/* ================================================================
 * src/backend/parser/parse_type.c — VERBATIM
 * ================================================================ */

/*
 * appendTypeNameToBuffer
 *		Append a string representing the name of a TypeName to a StringInfo.
 *		This is the shared guts of TypeNameToString and TypeNameListToString.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
static void
appendTypeNameToBuffer(const TypeName *typeName, StringInfo string)
{
	if (typeName->names != NIL)
	{
		/* Emit possibly-qualified name as-is */
		ListCell   *l;

		foreach(l, typeName->names)
		{
			if (l != list_head(typeName->names))
				appendStringInfoChar(string, '.');
			appendStringInfoString(string, strVal(lfirst(l)));
		}
	}
	else
	{
		/* Look up internally-specified type */
		appendStringInfoString(string, format_type_be(typeName->typeOid));
	}

	/*
	 * Add decoration as needed, but only for fields considered by
	 * LookupTypeName
	 */
	if (typeName->pct_type)
		appendStringInfoString(string, "%TYPE");

	if (typeName->arrayBounds != NIL)
		appendStringInfoString(string, "[]");
}

/*
 * TypeNameToString
 *		Produce a string representing the name of a TypeName.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
static char *
TypeNameToString(const TypeName *typeName)
{
	StringInfoData string;

	initStringInfo(&string);
	appendTypeNameToBuffer(typeName, &string);
	return string.data;
}

/* ================================================================
 * src/backend/commands/define.c — VERBATIM
 * ================================================================ */

/*
 * Extract a string value (otherwise uninterpreted) from a DefElem.
 */
static char *
defGetString(DefElem *def)
{
	if (def->arg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s requires a parameter",
						def->defname)));
	switch (nodeTag(def->arg))
	{
		case T_Integer:
			return psprintf("%ld", (long) intVal(def->arg));
		case T_Float:
			return castNode(Float, def->arg)->fval;
		case T_Boolean:
			return boolVal(def->arg) ? "true" : "false";
		case T_String:
			return strVal(def->arg);
		case T_TypeName:
			return TypeNameToString((TypeName *) def->arg);
		case T_List:
			return NameListToString((List *) def->arg);
		case T_A_Star:
			return pstrdup("*");
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(def->arg));
	}
	return NULL;				/* keep compiler quiet */
}

/*
 * Extract a boolean value from a DefElem.
 */
static bool
defGetBoolean(DefElem *def)
{
	/*
	 * If no parameter value given, assume "true" is meant.
	 */
	if (def->arg == NULL)
		return true;

	/*
	 * Allow 0, 1, "true", "false", "on", "off"
	 */
	switch (nodeTag(def->arg))
	{
		case T_Integer:
			switch (intVal(def->arg))
			{
				case 0:
					return false;
				case 1:
					return true;
				default:
					/* otherwise, error out below */
					break;
			}
			break;
		default:
			{
				char	   *sval = defGetString(def);

				/*
				 * The set of strings accepted here should match up with the
				 * grammar's opt_boolean_or_string production.
				 */
				if (pg_strcasecmp(sval, "true") == 0)
					return true;
				if (pg_strcasecmp(sval, "false") == 0)
					return false;
				if (pg_strcasecmp(sval, "on") == 0)
					return true;
				if (pg_strcasecmp(sval, "off") == 0)
					return false;
			}
			break;
	}
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("%s requires a Boolean value",
					def->defname)));
	return false;				/* keep compiler quiet */
}

/* ========== fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * Fixture construction from flat driver arguments (environment, not
 * computation). argkind: 0 = no arg, 1 = Integer(ival), 2 = Float(s),
 * 3 = Boolean(ival & 1), 4 = String(s), 5 = TypeName(names[0..nnames),
 * pct_type, arrayBounds nonempty iff nbounds > 0).
 */
static Node *
pg_define_build_arg(int argkind, int ival, const char *s,
					const char *const *names, int nnames,
					int pct_type, int nbounds)
{
	switch (argkind)
	{
		case 0:
			return NULL;
		case 1:
			{
				Integer    *n = pg_define_palloc(sizeof(Integer));

				n->type = T_Integer;
				n->ival = ival;
				return (Node *) n;
			}
		case 2:
			{
				Float	   *n = pg_define_palloc(sizeof(Float));

				n->type = T_Float;
				n->fval = pstrdup(s);
				return (Node *) n;
			}
		case 3:
			{
				Boolean    *n = pg_define_palloc(sizeof(Boolean));

				n->type = T_Boolean;
				n->boolval = (ival & 1) != 0;
				return (Node *) n;
			}
		case 4:
			{
				String	   *n = pg_define_palloc(sizeof(String));

				n->type = T_String;
				n->sval = pstrdup(s);
				return (Node *) n;
			}
		case 5:
			{
				TypeName   *tn = pg_define_palloc(sizeof(TypeName));
				List	   *nl = pg_define_palloc(sizeof(List));
				int			i;

				memset(tn, 0, sizeof(TypeName));
				tn->type = T_TypeName;
				nl->type = T_List;
				nl->length = nnames;
				nl->max_length = nnames;
				nl->elements = pg_define_palloc(sizeof(ListCell) * (nnames ? nnames : 1));
				for (i = 0; i < nnames; i++)
				{
					String	   *sn = pg_define_palloc(sizeof(String));

					sn->type = T_String;
					sn->sval = pstrdup(names[i]);
					nl->elements[i].ptr_value = sn;
				}
				tn->names = nnames > 0 ? nl : NIL;
				tn->pct_type = pct_type != 0;
				if (nbounds > 0)
				{
					/* contents never read; only NIL-ness is consulted */
					List	   *bl = pg_define_palloc(sizeof(List));

					bl->type = T_List;
					bl->length = nbounds;
					bl->max_length = nbounds;
					bl->elements = pg_define_palloc(sizeof(ListCell));
					tn->arrayBounds = bl;
				}
				else
					tn->arrayBounds = NIL;
				return (Node *) tn;
			}
		default:
			abort();
	}
}

/*
 * pg_diff_defGetString: 0 = ok (result cstring copied into out, cap outcap),
 * nonzero = errcode class.
 */
int
pg_diff_defGetString(int argkind, const char *defname, int ival,
					 const char *s, const char *const *names, int nnames,
					 int pct_type, int nbounds, char *out, int outcap)
{
	DefElem		def;
	char	   *r;
	size_t		rlen;

	pg_define_arena_reset();
	pg_diff_errcode = 0;

	if (setjmp(pg_define_jmp) != 0)
	{
		pg_define_arena_reset();
		return pg_diff_errcode;
	}

	memset(&def, 0, sizeof(def));
	def.type = T_DefElem;
	def.defname = (char *) defname;
	def.arg = pg_define_build_arg(argkind, ival, s, names, nnames,
								  pct_type, nbounds);

	r = defGetString(&def);
	rlen = strlen(r);
	if (rlen + 1 > (size_t) outcap)
		abort();				/* driver caps inputs; overflow = shim bug */
	memcpy(out, r, rlen + 1);
	pg_define_arena_reset();
	return 0;
}

/*
 * pg_diff_defGetBoolean: 0 = ok (*bool_out set), nonzero = errcode class.
 */
int
pg_diff_defGetBoolean(int argkind, const char *defname, int ival,
					  const char *s, int *bool_out)
{
	DefElem		def;
	bool		b;

	pg_define_arena_reset();
	pg_diff_errcode = 0;

	if (setjmp(pg_define_jmp) != 0)
	{
		pg_define_arena_reset();
		return pg_diff_errcode;
	}

	memset(&def, 0, sizeof(def));
	def.type = T_DefElem;
	def.defname = (char *) defname;
	def.arg = pg_define_build_arg(argkind, ival, s, NULL, 0, 0, 0);

	b = defGetBoolean(&def);
	*bool_out = b ? 1 : 0;
	pg_define_arena_reset();
	return 0;
}
