/*
 * SHIM utils/builtins.h — NOT PostgreSQL code. (tsq oracle family)
 * Declarations for the three text helpers the vendored files call;
 * implemented in pg_tsq_shim.c over the family arena, semantics of
 * src/backend/utils/adt/varlena.c.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_BUILTINS_H
#define PG_DIFFFUZZ_TSQ_SHIM_BUILTINS_H

#include "postgres.h"
#include "fmgr.h"

extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern char *text_to_cstring(const text *t);

#endif
