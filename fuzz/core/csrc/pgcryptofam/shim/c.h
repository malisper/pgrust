/* SHIM c.h -> shared shim env (src/port/snprintf.c and src/port/strlcpy.c
 * include "c.h" directly; everything they need lives in postgres.h). */
#include "postgres.h"
