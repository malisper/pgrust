/*
 * SHIM postgres_fe.h — NOT PostgreSQL code. See shim/c.h for provenance.
 * path.c compiles -DFRONTEND (identical pure-path logic; the FRONTEND arms
 * differ only inside make_absolute_path's error legs, which the fuzz driver
 * never calls — cwd-reading arm, excluded-state carve).
 */
#ifndef PG_DIFFFUZZ_PORTFAM_SHIM_POSTGRES_FE_H
#define PG_DIFFFUZZ_PORTFAM_SHIM_POSTGRES_FE_H

#include "c.h"

#endif
