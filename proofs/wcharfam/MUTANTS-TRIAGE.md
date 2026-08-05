# p1-laneah trailing mutants audit — triage of record

Fleet job: pgrust-mutants-audit-1785511537-5f96-94647 @ 34df12d7e382
(cargo-mutants 27.1.0, c8g.4xlarge). Summary: wchar total 1244 mutants,
caught-in-crate 1058, missed 158, unviable 2, timeout 26 (wall 4096s);
mbutils total 1021, caught 313, missed 138, unviable 566, timeout 4
(wall 757s). rail: UNAVAILABLE on the fleet job — the differential rail
(decoder_fuzz wcharfam tests + committed-corpus replay) did not run
there, so every "missed" row is UNSWEPT, not adjudicated.

Local rail sweep (this file's evidence, /tmp scripts banked in git log):
one representative missed mutant PER IN-SCOPE FUNCTION (36 sampled)
applied + replayed through the full rail (exhaustive quick tier +
corpus replay), reverted:

- 25/36 KILLED outright by the rail.
- 10/36 SURVIVED and each adjudicated EQUIVALENT (arid):
  * pg_euc_dsplen ||->&&: SS2/SS3 imply the high bit; condition collapses.
  * pg_utf2wchar_with_len, pg_utf_mblen_byte &->| on `b & 0x80 == 0`:
    the else arm computes the identical result for ASCII bytes.
  * mbbisearch ||->&& early-exit: binary search returns false anyway.
  * is_valid_ascii ==->!= at the x86_64 arm: cfg'd out on this host
    (platform-other exception rows carry those lines).
  * pg_utf8_verifystr >=-><: swaps fast/slow path selection; the fast
    path is pure acceleration, slow path recomputes — output identical.
  * utf8_to_unicode / unicode_to_utf8 |->^: disjoint bit ranges, | == ^.
  * pg_valid_client_encoding ||->&&: first disjunct implies the second.
  * pg_encoding_mbstrlen_with_len &&->|| (SWAR gate): documented
    pure-acceleration property — ascii_run returns 0 on multibyte leads
    and scores ASCII identically under every encoding. (The audit DID
    expose a pad-plane blind spot at the walk's slice-end boundary; an
    UNPADDED pg_mbstrlen/pg_mbstrlen_with_len compare was added to the
    harness as a strengthening.)
  * ascii_run |->^ : operands disjoint by construction (a & !w vs w).
- utf8_advance >>-><<: PROVEN output-equivalent: every UTF8_TRANSITION
  entry has low 11 bits zero, so under << the running state's low 5 bits
  are 0 after byte 1 and the chunk always lands in ERR — pg_utf8_verifystr
  then recounts the whole input via the slow path. Perf-only mutant.
- 1 sample APPLY-FAIL (pg_verify_mbstr_len match-guard mutant, non-token
  patch shape): its function is compared on both noError arms with
  interior-NUL seeds committed in the corpus (fam-1 planes).
- Out-of-scope functions (conversion/GUC machinery, fc convert wrappers):
  missed rows are arid by the phase-1 carve (the same lines carry
  excluded-state exception rows).
- Timeouts (26 wchar / 4 mbutils): loop-guard mutants that spin; counted
  by cargo-mutants as timeout, not missed — effectively fatal mutants.

Verdict: no rail-visible hole beyond the pad-plane boundary, which is now
covered; no reopen required.
