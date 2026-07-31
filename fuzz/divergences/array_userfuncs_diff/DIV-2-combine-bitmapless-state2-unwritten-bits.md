# DIV-2 (array_userfuncs_diff): array_agg_array_combine leaves the appended
# items' null bits UNWRITTEN when state1 has a null bitmap and state2 has none

Status: CLOSED — upstream-bug; pgrust deliberately diverges by fixing it
(decision already recorded in the shipped code). Plane carved, see below.
Found: 2026-07-31, lane p1-laneai, 500k-exec local smoke of array_userfuncs_diff.
Reproducer: DIV-2-combine-bitmapless-state2-unwritten-bits.input (arm 10).

## The C defect

`src/backend/utils/adt/array_userfuncs.c` @ 62d6c7d3df (Stamp 18.3),
array_agg_array_combine, line 1012:

    if (state2->nullbitmap)
    {
        int newnitems = state1->nitems + state2->nitems;
        if (state1->nullbitmap == NULL) { ...palloc + mark previous non-null... }
        else if (newnitems > state1->aitems) { ...repalloc... }
        array_bitmap_copy(state1->nullbitmap, state1->nitems,
                          state2->nullbitmap, 0, state2->nitems);
    }

The whole bitmap merge is gated on **state2** having a bitmap. When state1
DOES have one (an earlier input contained NULLs) and state2 does not (all its
inputs were null-free), the `state1->nitems .. +state2->nitems` bit range is
never written, while `state1->nitems` still advances past it. state1's bitmap
came from `palloc` (accumArrayResultArr, not palloc0), so those bits are
uninitialized heap, and makeArrayResultArr then copies them into the result
array's null bitmap. The result's null flags for the appended elements are
whatever was in memory.

By contrast accumArrayResultArr gates on `astate->nullbitmap != NULL ||
ARR_HASNULL(arg)` and passes a NULL source bitmap for a null-free arg, which
array_bitmap_copy turns into "all bits set = all non-null". That is the
correct shape; combine is missing exactly that arm.

Witness from this reproducer (4 text arrays of 7 elements; the first has NULLs
at 0,1,2, the rest are null-free; split = 1, so state1 = array 0 and state2 =
arrays 1-3): the 28-bit result bitmap is
  C    f8 be be 0e   (0xbe = the ASan heap-fill pattern showing through)
  Rust f8 ff ff 0f
Both agree on the first byte (elements 0,1,2 NULL); every later bit is
uninitialized on the C side. Under a plain malloc build the same bits would be
whatever the allocator last held, so real PostgreSQL's answer here is
UNSPECIFIED, not merely different — a forced-parallel `array_agg` run against
postgres:18.3 returned the right answer, which is consistent with reading
memory that happened to be set.

## pgrust's position (pre-existing, not decided by this lane)

`crates/backend/utils/adt/array_userfuncs/src/lib.rs:568-570`:

    // Combine the null bitmaps, if either side has one; a bitmap-less state2
    // contributes all-non-null bits (C 14bf2c3).
    if s1.nullbitmap.is_some() || s2.nullbitmap.is_some() {

so the port already gates on EITHER side and feeds `src = None` for a
bitmap-less state2, marking the appended items non-null. That is the behaviour
the cited upstream commit installs. Match-or-fix decision: FIX (already
taken); pgrust is correct and 18.3 is not.

## Plane carve (documented in the target header)

The final-value plane of arm 10 is skipped for exactly the shape that
reaches the unwritten bits — state1's inputs contain a NULL, state2 exists and
none of its inputs do — because the C oracle's bytes there are undefined
rather than divergent. Verdict and errcode planes still compare on that shape,
and every other shape compares the full image. Once the fix is backported into
the vendored oracle (or 18.4+ becomes the pin), delete the skip: it is the
regression test.
