/*
 * SHIM varatt.h — NOT PostgreSQL code. (tsq oracle family, p1-laneaf)
 *
 * The 4-byte-header (4B uncompressed) varlena macros only, bit-layout
 * IDENTICAL to upstream src/include/varatt.h on a little-endian host
 * (va_header = len << 2; the low 2 bits 00 mean plain uncompressed).
 * Every value crossing this oracle's boundary is flat: the pg_diff_*
 * entries build well-formed 4B headers on input copies and zero the
 * header on output (matching the Rust image plane's zeroed vl_len_).
 * Short-header/TOAST forms are unreachable and deliberately undefined.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_VARATT_H
#define PG_DIFFFUZZ_TSQ_SHIM_VARATT_H

#include "postgres.h"

#define VARHDRSZ ((int32) sizeof(int32))

typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
} varattrib_4b;

/* little-endian bit layout, upstream varatt.h "#else" (!WORDS_BIGENDIAN) arm */
#define VARATT_IS_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header & 0x03) == 0x00)

#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define SET_VARSIZE_4B(PTR, len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))

#define VARSIZE(PTR) VARSIZE_4B(PTR)
#define SET_VARSIZE(PTR, len) SET_VARSIZE_4B(PTR, len)
#define VARDATA(PTR) (((varattrib_4b *) (PTR))->va_4byte.va_data)

/* flat-only oracle: *_ANY forms collapse to the 4B forms */
#define VARSIZE_ANY(PTR) VARSIZE_4B(PTR)
#define VARSIZE_ANY_EXHDR(PTR) (VARSIZE_4B(PTR) - VARHDRSZ)
#define VARDATA_ANY(PTR) VARDATA(PTR)

#endif							/* PG_DIFFFUZZ_TSQ_SHIM_VARATT_H */
