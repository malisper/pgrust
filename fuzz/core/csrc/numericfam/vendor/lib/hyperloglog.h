#ifndef NV_HLL_H
#define NV_HLL_H
typedef struct hyperLogLogState
{
	uint8		registerWidth;
	Size		nRegisters;
	double		alphaMM;
	uint8	   *hashesArr;
	Size		arrSize;
} hyperLogLogState;

extern void initHyperLogLog(hyperLogLogState *cState, uint8 bwidth);
extern void addHyperLogLog(hyperLogLogState *cState, uint32 hash);
extern double estimateHyperLogLog(hyperLogLogState *cState);
#endif
