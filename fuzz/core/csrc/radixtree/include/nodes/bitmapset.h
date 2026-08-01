/*
 * REDUCED nodes/bitmapset.h — verbatim pieces only (radixtree_diff oracle).
 *
 * lib/radixtree.h consumes just the bitmapword typedef and the bmw_*
 * helper macros. The lines below are BYTE-FOR-BYTE from
 * src/include/nodes/bitmapset.h @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (Stamp-18.3), 64-bit arm only (the oracle build targets LP64 exactly like
 * the shipped crate); nothing else from the header is consumed.
 */
#ifndef BITMAPSET_H
#define BITMAPSET_H

#include "port/pg_bitutils.h"

/* verbatim: src/include/nodes/bitmapset.h lines 37..39 */
#define BITS_PER_BITMAPWORD 64
typedef uint64 bitmapword;		/* must be an unsigned type */
typedef int64 signedbitmapword; /* must be the matching signed type */

/* verbatim: src/include/nodes/bitmapset.h lines 81..84 */
#define bmw_leftmost_one_pos(w)		pg_leftmost_one_pos64(w)
#define bmw_rightmost_one_pos(w)	pg_rightmost_one_pos64(w)
#define bmw_popcount(w)				pg_popcount64(w)

#endif							/* BITMAPSET_H */
