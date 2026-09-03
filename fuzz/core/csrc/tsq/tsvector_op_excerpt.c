/*
 * tsvector_op_excerpt.c — EXCERPT of src/backend/utils/adt/tsvector_op.c
 * @ REL_18_6 (re-stamped 2026-09-02; the function is byte-identical to the
 * original 18.3 extract @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0, lines
 * 1146-1183 there).
 *
 * VERBATIM: lines 1149-1186 (tsCompareString) — the one tsvector_op.c
 * function the vendored tsquery family reaches (QTNodeCompare in
 * tsquery_util.c). Rust counterpart: ts_compare_string in
 * adt/tsvector_core (p1-laneae's crate).
 *
 * CARVE (documented): the rest of tsvector_op.c (tsvector ops, TS_execute
 * match engine, stat machinery) is neither called nor claimed by this
 * lane and is not vendored.
 */

#include "postgres.h"

#include "tsearch/ts_utils.h"

/* ---- BEGIN VERBATIM tsvector_op.c:1149-1186 @ REL_18_6 ---- */
/*
 * Compare two strings by tsvector rules.
 *
 * if prefix = true then it returns zero value iff b has prefix a
 */
int32
tsCompareString(char *a, int lena, char *b, int lenb, bool prefix)
{
	int			cmp;

	if (lena == 0)
	{
		if (prefix)
			cmp = 0;			/* empty string is prefix of anything */
		else
			cmp = (lenb > 0) ? -1 : 0;
	}
	else if (lenb == 0)
	{
		cmp = (lena > 0) ? 1 : 0;
	}
	else
	{
		cmp = memcmp(a, b, Min((unsigned int) lena, (unsigned int) lenb));

		if (prefix)
		{
			if (cmp == 0 && lena > lenb)
				cmp = 1;		/* a is longer, so not a prefix of b */
		}
		else if (cmp == 0 && lena != lenb)
		{
			cmp = (lena < lenb) ? -1 : 1;
		}
	}

	return cmp;
}
/* ---- END VERBATIM ---- */
