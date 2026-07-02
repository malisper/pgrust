/* Minimal shim so the vendored pg_crc32c_*.c compile unmodified. */
#include <stdint.h>
#include <stddef.h>
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
#define PointerIsAligned(pointer, type) \
	(((uintptr_t)(pointer) % (sizeof (type))) == 0)
