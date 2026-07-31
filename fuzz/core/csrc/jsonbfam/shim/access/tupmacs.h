/* SHIM access/tupmacs.h — VERBATIM subset of src/include/access/tupmacs.h
 * @ 62d6c7d3df (18.3): fetch_att + att_align_nominal +
 * att_addlength_pointer, the attribute-walk primitives deconstruct_array
 * uses. TYPALIGN_* constants from c.h (verbatim values). */
#ifndef PG_JSONBFAM_SHIM_TUPMACS_H
#define PG_JSONBFAM_SHIM_TUPMACS_H

#include "postgres.h"

/* c.h TYPALIGN_* (verbatim values) */
#define TYPALIGN_CHAR	'c'
#define TYPALIGN_SHORT	's'
#define TYPALIGN_INT	'i'
#define TYPALIGN_DOUBLE 'd'

/* tupmacs.h fetch_att (lines 52-77), verbatim */
static inline Datum
fetch_att(const void *T, bool attbyval, int attlen)
{
	if (attbyval)
	{
		switch (attlen)
		{
			case sizeof(char):
				return CharGetDatum(*((const char *) T));
			case sizeof(int16):
				return Int16GetDatum(*((const int16 *) T));
			case sizeof(int32):
				return Int32GetDatum(*((const int32 *) T));
#if SIZEOF_DATUM == 8
			case sizeof(Datum):
				return *((const Datum *) T);
#endif
			default:
				elog(ERROR, "unsupported byval length: %d", attlen);
				return 0;
		}
	}
	else
		return PointerGetDatum(T);
}

/* tupmacs.h att_align_nominal (lines 150-159), verbatim */
#define att_align_nominal(cur_offset, attalign) \
( \
	((attalign) == TYPALIGN_INT) ? INTALIGN(cur_offset) : \
	 (((attalign) == TYPALIGN_CHAR) ? (uintptr_t) (cur_offset) : \
	  (((attalign) == TYPALIGN_DOUBLE) ? DOUBLEALIGN(cur_offset) : \
	   ( \
			AssertMacro((attalign) == TYPALIGN_SHORT), \
			SHORTALIGN(cur_offset) \
	   ))) \
)

/* tupmacs.h att_addlength_pointer (lines 185-201), verbatim */
#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
	((attlen) > 0) ? \
	( \
		(cur_offset) + (attlen) \
	) \
	: (((attlen) == -1) ? \
	( \
		(cur_offset) + VARSIZE_ANY(attptr) \
	) \
	: \
	( \
		AssertMacro((attlen) == -2), \
		(cur_offset) + (strlen((char *) (attptr)) + 1) \
	)) \
)

#endif
