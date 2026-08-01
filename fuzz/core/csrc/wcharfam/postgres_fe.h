/*
 * SHIM postgres_fe.h — NOT PostgreSQL code. See wcharfam/c.h for provenance.
 * The vendored src/backend/utils/mb/wstrncmp.c ("can be used in either
 * frontend or backend") includes this; the family's c.h shim is the whole
 * environment it needs. Plumbing only.
 */
#ifndef PG_DIFFFUZZ_WCHARFAM_SHIM_POSTGRES_FE_H
#define PG_DIFFFUZZ_WCHARFAM_SHIM_POSTGRES_FE_H
#include "c.h"
#endif
