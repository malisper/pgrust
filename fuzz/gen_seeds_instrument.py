#!/usr/bin/env python3
"""gen_seeds_instrument.py — directed seed corpus for instrument_diff.

Emits, per arm: the option-bit matrix (TIMER/BUFFERS/ROWS/WAL off/on),
error-shape sequences (start-twice, stop-without-start, end-loop-running),
zero-duration stops, zero-clock sentinel starts, nloops=0 EndLoop,
wal_bytes wrap pairs, negative-delta accum-diff shapes, plus
SINGLE-FIELD-DIFFERENCE WITNESS PAIRS (the Lane-0B seeding obligation):
pairs differing in exactly ONE clock step (both orders), exactly ONE
BufferUsage field (all 16 fields), exactly ONE WalUsage field (all 4), and
AggNode firsttuple/running order pairs — so every field's individual
contribution to the compared planes is witnessed. Deterministic; output
committed. Sibling of gen_seeds_tsmtime.py.

Input layout (core/src/instrument_diff.rs):
  [sel%4] 0=init 1=cycle 2=agg 3=arith
  arm1: [flags][base u32 (skipped if flags&1)][options u8] then ops:
    op%8: 0 start[dtsel..] 1 stop[dtsel..][ntuples] 2 update[ntuples]
          3 endloop 4 injbuf[buf] 5 injwal[wal] 6 injaccums[buf][wal]
          7 walglobal[wal]
    dtsel&3: 0 none, 1 u8, 2 u16<<8, 3 u16<<20
    ntuples: [mode u8] mode&1 ? u16 : f64 bits
  buf = 16 x u32 (i32-derived i64 fields)
  wal = [mode u8][raw u64][records u32][fpi u32][full u32]
"""
import struct
from pathlib import Path

OUT = Path(__file__).resolve().parent / "corpus" / "instrument_diff"
OUT.mkdir(parents=True, exist_ok=True)

seeds = {}


def put(name: str, data: bytes):
    assert name not in seeds, name
    seeds[name] = data


def buf(fields=None, **kw) -> bytes:
    """16 x u32 BufferUsage derivation words."""
    v = list(fields) if fields is not None else [0] * 16
    for k, val in kw.items():
        v[int(k[1:])] = val
    return struct.pack("<16I", *[x & 0xFFFFFFFF for x in v])


def wal(mode=0, raw=0, records=0, fpi=0, full=0) -> bytes:
    return struct.pack("<BQIII", mode, raw, records & 0xFFFFFFFF,
                       fpi & 0xFFFFFFFF, full & 0xFFFFFFFF)


def ntup_small(n: int) -> bytes:
    return struct.pack("<BH", 1, n)


def ntup_bits(bits: int) -> bytes:
    return struct.pack("<BQ", 0, bits)


def start(dtsel=1, dt=5) -> bytes:
    if dtsel == 0:
        return bytes([0, 0])
    if dtsel == 1:
        return bytes([0, 1, dt & 0xFF])
    return bytes([0, dtsel]) + struct.pack("<H", dt & 0xFFFF)


def stop(dtsel=1, dt=7, ntup=None) -> bytes:
    hd = bytes([1])
    if dtsel == 0:
        hd += bytes([0])
    elif dtsel == 1:
        hd += bytes([1, dt & 0xFF])
    else:
        hd += bytes([dtsel]) + struct.pack("<H", dt & 0xFFFF)
    return hd + (ntup if ntup is not None else ntup_small(1))


def cycle(flags=0, base=1000, options=0x0F, ops=b"") -> bytes:
    hd = bytes([1, flags])
    if not flags & 1:
        hd += struct.pack("<I", base)
    return hd + bytes([options]) + ops


def instr_state(flags=0, start_t=3, counter=9, f64s=None, bufa=None,
                wala=None, bufb=None, walb=None) -> bytes:
    """One derive_instr payload: [flags][starttime u32?][counter u32]
    [firsttuple f64][tuplecount f64][buf][wal][7 x f64][buf][wal]."""
    out = bytes([flags])
    if not flags & 32:
        out += struct.pack("<I", start_t & 0xFFFFFFFF)
    out += struct.pack("<I", counter & 0xFFFFFFFF)
    f = f64s if f64s is not None else [0.0] * 9
    out += struct.pack("<dd", f[0], f[1])
    out += bufa if bufa is not None else buf()
    out += wala if wala is not None else wal()
    out += struct.pack("<7d", *f[2:9])
    out += bufb if bufb is not None else buf()
    out += walb if walb is not None else wal()
    return out


def agg(ops, dst, add) -> bytes:
    return bytes([2, ops]) + dst + add


def arith(bufs, wals) -> bytes:
    """5 buf derivations (add: d,a; accumdiff: d,a,s) + 7 wal."""
    return bytes([3]) + b"".join(bufs) + b"".join(wals)


# ---------------------------------------------------------------------------
# Arm 0: option-bit matrix over garbage + full-width options.
# ---------------------------------------------------------------------------
garbage = instr_state(
    flags=0x1F, start_t=77, counter=88,
    f64s=[1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5],
    bufa=buf([10 + i for i in range(16)]), wala=wal(0, 5000, 3, 1, 2),
    bufb=buf([90 + i for i in range(16)]), walb=wal(0, 7000, 8, 2, 1))
for opt in range(16):
    put(f"init_opt_{opt:02d}", bytes([0]) + garbage + struct.pack("<I", opt))
put("init_opt_all", bytes([0]) + garbage + struct.pack("<I", 0x7FFFFFFF))
put("init_opt_highbits", bytes([0]) + garbage + struct.pack("<I", 0xFFFFFFF0))

# ---------------------------------------------------------------------------
# Arm 1: cycles — option matrix, error shapes, zero-duration, zero-clock.
# ---------------------------------------------------------------------------
basic = start() + stop() + start(2, 40) + stop(2, 44, ntup_small(3)) + bytes([3])
for opt in range(16):
    put(f"cycle_opt_{opt:02d}", cycle(options=opt, ops=basic))

put("cycle_async", cycle(flags=2, options=0x0F, ops=basic))
put("cycle_async_frac_first",
    cycle(flags=2, options=0x01,
          ops=start() + stop(1, 5, ntup_bits(struct.unpack("<Q", struct.pack("<d", 0.25))[0]))
          + start() + stop(1, 5, ntup_small(2))))
put("cycle_zero_clock", cycle(flags=1, options=0x01, ops=start(0) + stop(0)))
put("cycle_zero_duration", cycle(options=0x01, ops=start(1, 9) + stop(0)))
put("cycle_start_twice", cycle(options=0x01, ops=start() + start()))
put("cycle_stop_wo_start", cycle(options=0x01, ops=stop()))
put("cycle_endloop_running",
    cycle(options=0x01, ops=start() + stop() + start() + bytes([3])))
put("cycle_endloop_nloops0", cycle(options=0x01, ops=bytes([3])))
put("cycle_update_tuplecount",
    cycle(options=0x01, ops=bytes([2]) + ntup_small(41) + start() + stop()))
put("cycle_ntuples_nan",
    cycle(options=0x01,
          ops=start() + stop(1, 5, ntup_bits(0x7FF8000000000001))))
put("cycle_ntuples_neg",
    cycle(options=0x01,
          ops=start() + stop(1, 5, ntup_bits(struct.unpack("<Q", struct.pack("<d", -2.0))[0]))))

# Injections + wal-global advance between start and stop.
put("cycle_inject_all",
    cycle(options=0x0F,
          ops=bytes([7]) + wal(0, 100, 1, 0, 0)
          + bytes([6]) + buf([5] * 16) + wal(0, 50, 2, 1, 0)
          + start()
          + bytes([4]) + buf([3] * 16)
          + bytes([5]) + wal(0, 30, 1, 1, 0)
          + bytes([7]) + wal(0, 900, 9, 3, 2)
          + stop() + bytes([3])))
# wal-global wrap across a cycle: start below, wrap past u64::MAX by stop.
put("cycle_walglobal_wrap",
    cycle(options=0x08,
          ops=bytes([7]) + wal(1, 5, 1, 0, 0)   # near-MAX at start
          + start()
          + bytes([7]) + wal(0, 100, 2, 0, 0)   # small after wrap
          + stop()))

# SINGLE-FIELD WITNESS: one clock step differing, both orders.
put("cycle_wit_dt_a", cycle(options=0x01, ops=start(1, 10) + stop(1, 20)))
put("cycle_wit_dt_b", cycle(options=0x01, ops=start(1, 10) + stop(1, 21)))
put("cycle_wit_dt2_a", cycle(options=0x01, ops=start(1, 11) + stop(1, 20)))
put("cycle_wit_dt2_b", cycle(options=0x01, ops=start(1, 10) + stop(1, 20)))

# ---------------------------------------------------------------------------
# Arm 2: agg — running/firsttuple order pairs + endloop combos.
# ---------------------------------------------------------------------------
def st(running: bool, firsttuple: float, flags_extra=0x0F & ~16) -> bytes:
    flags = (0x0F | 16 | 32) if running else ((0x0F | 32) & ~16)
    return instr_state(flags=flags, counter=100,
                       f64s=[firsttuple, 2.0, 1.0, 4.0, 3.0, 2.0, 1.0, 0.5, 0.25],
                       bufa=buf([1] * 16), wala=wal(0, 10, 1, 0, 0),
                       bufb=buf([2] * 16), walb=wal(0, 20, 2, 1, 0))

for name, d_run, a_run, d_ft, a_ft in [
    ("agg_ft_dst_gt", True, True, 9.0, 3.0),
    ("agg_ft_dst_lt", True, True, 3.0, 9.0),
    ("agg_ft_dst_eq", True, True, 5.0, 5.0),
    ("agg_run_10", True, False, 5.0, 7.0),
    ("agg_run_01", False, True, 5.0, 7.0),
    ("agg_run_00", False, False, 5.0, 7.0),
]:
    put(name, agg(0, st(d_run, d_ft), st(a_run, a_ft)))
for ops in range(4):
    put(f"agg_endloop_{ops}", agg(ops, st(True, 4.0), st(True, 6.0)))
# EndLoop error inside arm 2: running + nonzero starttime.
put("agg_endloop_err",
    agg(1, instr_state(flags=0x1F, start_t=5, counter=7), st(True, 1.0)))
# need_bufusage/need_walusage OFF on dst: AggNode skips the usage adds.
put("agg_no_usage_flags",
    agg(0, instr_state(flags=16 | 32, counter=3), st(True, 2.0)))

# ---------------------------------------------------------------------------
# Arm 3: arithmetic — wrap pairs, negative deltas, per-field witnesses.
# ---------------------------------------------------------------------------
Z16 = [buf()] * 5
ZW = [wal()] * 7

put("arith_zero", arith(Z16, ZW))
# wal_bytes wrap pairs (both orders): add near MAX + small, and sub > add.
put("arith_wrap_add",
    arith(Z16, [wal(1, 0), wal(0, 5), wal(), wal(), wal(), wal(), wal()]))
put("arith_wrap_add_rev",
    arith(Z16, [wal(0, 5), wal(1, 0), wal(), wal(), wal(), wal(), wal()]))
put("arith_wrap_diff",
    arith(Z16, [wal(), wal(), wal(0, 3), wal(0, 1), wal(0, 9), wal(), wal()]))
put("arith_wrap_diff_rev",
    arith(Z16, [wal(), wal(), wal(0, 3), wal(0, 9), wal(0, 1), wal(), wal()]))
# negative-delta accum-diff (sub > add) on every buffer field at once.
put("arith_negative_delta",
    arith([buf(), buf(), buf([100] * 16), buf([10] * 16), buf([30] * 16)], ZW))
# negative operands (i32 sign extension).
put("arith_negative_operands",
    arith([buf([0xFFFFFFF0] * 16), buf([0xFFFFFF00] * 16), buf(), buf(), buf()], ZW))

# SINGLE-FIELD WITNESS pairs: every BufferUsage field alone, both orders.
for i in range(16):
    put(f"arith_wit_buf{i:02d}_a",
        arith([buf(**{f"f{i}": 7}), buf(**{f"f{i}": 1}), buf(), buf(), buf()], ZW))
    put(f"arith_wit_buf{i:02d}_b",
        arith([buf(**{f"f{i}": 7}), buf(**{f"f{i}": 2}), buf(), buf(), buf()], ZW))
# Every WalUsage field alone, both orders (add-side operand varies).
wal_variants = [
    ("records", dict(records=3), dict(records=4)),
    ("fpi", dict(fpi=3), dict(fpi=4)),
    ("bytes", dict(raw=3), dict(raw=4)),
    ("full", dict(full=3), dict(full=4)),
]
for nm, a, b in wal_variants:
    put(f"arith_wit_wal_{nm}_a",
        arith(Z16, [wal(0, 9, 1, 1, 1), wal(**a), wal(), wal(), wal(), wal(), wal()]))
    put(f"arith_wit_wal_{nm}_b",
        arith(Z16, [wal(0, 9, 1, 1, 1), wal(**b), wal(), wal(), wal(), wal(), wal()]))

# NaN-payload carve witnesses (2026-08-01): sNaN added onto a qNaN total —
# the shape where fadd operand-commutation makes the propagated payload
# compiler-dependent (any-NaN == any-NaN relaxation in the driver). Both
# orders; TIMER off so no error path interleaves.
QNAN_ALL1 = 0xFFFFFFFFFFFFFFFF
SNAN_PAY = 0xFFF7000000000100
put("cycle_nan_payload_qs",
    cycle(options=0x00, ops=stop(0, ntup=ntup_bits(QNAN_ALL1))
          + stop(0, ntup=ntup_bits(SNAN_PAY))))
put("cycle_nan_payload_sq",
    cycle(options=0x00, ops=stop(0, ntup=ntup_bits(SNAN_PAY))
          + stop(0, ntup=ntup_bits(QNAN_ALL1))))

# ---------------------------------------------------------------------------
for name, data in seeds.items():
    (OUT / name).write_bytes(data)
print(f"wrote {len(seeds)} seeds to {OUT}")
