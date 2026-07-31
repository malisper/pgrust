/* SHIM nodes/pg_list.h (tsvec oracle) — NOT PostgreSQL code.
 * Only the List typedef is needed: ts_utils.h declares
 * TS_execute_locations (definition carved in tsvector_op.c). */
#ifndef PG_DIFFFUZZ_TSVEC_PG_LIST_H
#define PG_DIFFFUZZ_TSVEC_PG_LIST_H
typedef struct List List;
#define NIL ((List *) NULL)
#endif
