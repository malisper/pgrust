/* Minimal c.h shim so the vendored wchar.c (plus the real mb/pg_wchar.h,
 * utils/ascii.h, port/simd.h) compiles standalone for parity/bench runs. */
#ifndef C_H_SHIM
#define C_H_SHIM

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef size_t Size;

#define UINT64CONST(x) UINT64_C(x)
#define Assert(x) ((void) 0)
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & HIGHBIT)
#define PGDLLIMPORT
#define pg_attribute_unused()
#define pg_noreturn _Noreturn
#define FRONTEND 1
typedef unsigned int Oid;

#endif
