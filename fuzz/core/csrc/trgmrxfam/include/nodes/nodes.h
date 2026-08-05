/* SHIM (trgmrxfam): minimal NodeTag surface for nodes/pg_list.h (verbatim).
 * Tag VALUES are identity tokens compared only against each other —
 * plumbing, not logic. */
#ifndef TRGMRX_NODES_H
#define TRGMRX_NODES_H
typedef enum NodeTag
{
	T_Invalid = 0,
	T_List = 1,
	T_IntList = 2,
	T_OidList = 3,
	T_XidList = 4,
} NodeTag;
#define nodeTag(nodeptr) (((const Node *) (nodeptr))->type)
typedef struct Node
{
	NodeTag		type;
} Node;
#define IsA(nodeptr, _type_) (nodeTag(nodeptr) == T_##_type_)
#define castNode(_type_, nodeptr) ((_type_ *) (nodeptr))
/* node machinery referenced only by list.c's deep-copy/equality helpers,
 * which trgm_regexp never calls -- abort stubs in pg_trgm_regexp_io.c */
extern void *copyObjectImpl(const void *obj);
extern bool equal(const void *a, const void *b);
#endif
