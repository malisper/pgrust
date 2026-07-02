#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Assert(x) ((void) 0)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define PGDLLIMPORT
