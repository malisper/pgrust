//! M5-B exit-slice batteries (chunks doc §5 M5-B): golden bytes over the
//! frozen vocabulary; visible-tombstone bitmap ≡ per-row heap MVCC oracle
//! under concurrent/subxact/combo-cid schedules (born-RED with seeded
//! skews); trickle-op composition laws; the delta WAL crash ladder with
//! the #253 seeded-sync-skip tooth; the scan-state publication stress.
//! (The scan-merge differentials against real sealed parts live in
//! `lx_source::pgrc_pair_tests` — the consuming side of the pair.)

mod bitmap_oracle;
mod crash;
mod golden;
mod ops_tests;
mod scanstate_stress;
