/*
 * SHIM nodes/pg_list.h — NOT PostgreSQL code. (tsq oracle family, p1-laneaf)
 *
 * Minimal List so tsquery.c's parse machinery (polstr built with lcons,
 * consumed with list_length + foreach/lfirst) compiles verbatim. Upstream
 * (src/include/nodes/pg_list.h) is an array-backed list; this shim is the
 * same shape reduced to the four operations the vendored files use. List
 * MECHANICS are plumbing — the list never crosses the comparison plane,
 * only the QueryItem array serialized from it does.
 *
 * Allocation goes through the family bump arena (palloc), so lists vanish
 * at the per-entry arena reset like a memory-context reset would.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_PG_LIST_H
#define PG_DIFFFUZZ_TSQ_SHIM_PG_LIST_H

#include "postgres.h"
#include "nodes/nodes.h"		/* upstream pg_list.h includes nodes.h; the
								 * vendored ts_utils.h relies on the Node
								 * typedef arriving this way */

typedef struct ListCell
{
	void	   *ptr_value;
} ListCell;

typedef struct List
{
	int			length;
	int			max_length;
	ListCell   *elements;
} List;

#define NIL ((List *) NULL)

static inline int
list_length(const List *l)
{
	return l ? l->length : 0;
}

#define lfirst(lc) ((lc)->ptr_value)

/* upstream foreach declares nothing; caller supplies the ListCell *var */
#define foreach(cell, lst) \
	for ((cell) = ((lst) == NIL ? NULL : &(lst)->elements[0]); \
		 (cell) != NULL && (cell) < &(lst)->elements[(lst)->length]; \
		 (cell)++)

extern List *lcons(void *datum, List *list);

/* arena-backed: freeing is the per-entry arena reset */
#define list_free(l) ((void) (l))

#endif							/* PG_DIFFFUZZ_TSQ_SHIM_PG_LIST_H */
