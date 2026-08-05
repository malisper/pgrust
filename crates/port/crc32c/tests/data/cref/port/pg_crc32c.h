/* Minimal shim of src/include/port/pg_crc32c.h for the vector generator. */
#ifndef PG_CRC32C_H
#define PG_CRC32C_H

typedef uint32 pg_crc32c;

#define INIT_CRC32C(crc) ((crc) = 0xFFFFFFFF)
#define FIN_CRC32C(crc) ((crc) ^= 0xFFFFFFFF)
#define EQ_CRC32C(c1, c2) ((c1) == (c2))

extern pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len);

#endif
