#ifndef NV_HASHFN_H
#define NV_HASHFN_H
extern Datum hash_any(const unsigned char *k, int keylen);
extern Datum hash_any_extended(const unsigned char *k, int keylen, uint64 seed);
#endif
