/*
 * SHIM utils/memutils.h — NOT PostgreSQL code (plumbing for the verbatim
 * src/common/stringinfo.c and src/common/psprintf.c backend arms).
 * MaxAllocSize is the verbatim src/include/utils/memutils.h value: the
 * enlargeStringInfo ceiling is load-bearing for the ERROR-VERDICT plane.
 */
#ifndef PGCRYPTOFAM_SHIM_MEMUTILS_H
#define PGCRYPTOFAM_SHIM_MEMUTILS_H

#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)

#endif
