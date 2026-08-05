/*
 * SHIM utils/builtins.h for the contribb_diff oracle: segparse.y includes
 * it, but nothing it declares is reached by the vendored bodies (seg_atof
 * routes through utils/float.h's float4in_internal). Intentionally empty.
 */
#ifndef PG_CB_UTILS_BUILTINS_H
#define PG_CB_UTILS_BUILTINS_H

#include "postgres.h"

#endif							/* PG_CB_UTILS_BUILTINS_H */
