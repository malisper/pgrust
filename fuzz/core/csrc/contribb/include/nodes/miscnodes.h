/*
 * SHIM nodes/miscnodes.h for the contribb_diff oracle: the soft-error test
 * reads the unified TLS channel (both sides of the diff run the soft-input
 * face; see include/postgres.h header comment for the protocol).
 */
#ifndef PG_CB_MISCNODES_H
#define PG_CB_MISCNODES_H

#include "postgres.h"

#define SOFT_ERROR_OCCURRED(escontext) (pg_cb_soft_occurred() != 0)

#endif							/* PG_CB_MISCNODES_H */
