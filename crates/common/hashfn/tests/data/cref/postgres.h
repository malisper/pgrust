#include <stdint.h>
#include <stddef.h>
#include <string.h>
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Assert(x) ((void) 0)
#define UINT64CONST(x) UINT64_C(x)
#define FRONTEND 1
