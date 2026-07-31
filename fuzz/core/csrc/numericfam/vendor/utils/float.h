#ifndef NV_FLOAT_H
#define NV_FLOAT_H
typedef double float8;
typedef float float4;

static inline float8
get_float8_infinity(void)
{
	return (float8) INFINITY;
}

static inline float4
get_float4_infinity(void)
{
	return (float4) INFINITY;
}

static inline float8
get_float8_nan(void)
{
	return (float8) NAN;
}

static inline float4
get_float4_nan(void)
{
	return (float4) NAN;
}
#endif
