/* SHIM utils/builtins.h (tsvec oracle) — NOT PostgreSQL code.
 * cstring_to_text_with_len implemented in pg_tsvector_core_io.c with the
 * verbatim-equivalent varlena.c body (palloc + SET_VARSIZE + memcpy). */
#ifndef PG_DIFFFUZZ_TSVEC_BUILTINS_H
#define PG_DIFFFUZZ_TSVEC_BUILTINS_H
/* upstream builtins.h includes fmgrprotos.h (function prototypes) */
#include "utils/fmgrprotos.h"
extern text *cstring_to_text_with_len(const char *s, int len);
#endif
