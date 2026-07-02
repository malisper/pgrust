static inline uint32 pg_rotate_left32(uint32 word, int n)
{ return (word << n) | (word >> (32 - n)); }
