# PostgreSQL 18.6 source — reference copy for comparison only

This folder contains the complete, pristine PostgreSQL 18.6 source tree,
extracted with `git archive` from the upstream `REL_18_6` tag
(commit `724edf9bde9d356724ad384a2e196edc3c9f80f7`, "Stamp 18.6."). It exists
so that agents and humans working in this repo can compare pgrust code against
the exact upstream C implementation without leaving the repo.

- **Do not edit anything under this folder.** It is a read-only reference.
- **Do not build from it here.** Build artifacts do not belong in this repo.
- It is the TAG content, not a working checkout — no local patches, no
  c2rust shims (unlike some drifted postgres-18.x working trees on dev
  machines, which are known to differ from the tag).
- The only file of the tag that is absent is `src/port/win32ver.rc`: upstream's
  own `.gitignore` (which travels with the archive) lists `win32ver.rc`, so a
  plain `git add` skips it. The 18.3 copy had the same gap.
- Not a Cargo workspace member (no `Cargo.toml`; `members = [...]` in the root
  `Cargo.toml` is an explicit list), not compiled, not read by any `build.rs`.

`src/test/regress` here is also the default regression corpus for
`scripts/pg-regress-fast.sh` (`REGRESS_SRC`), so the corpus the overlays under
`regress/overlay/sql` are keyed to is the one in this tree.

History: `crates/postgres-18.3-reference/` (tag `REL_18_3`,
`62d6c7d3df6287f1bd83199c1a746e50d31571a0`) was added 2026-08-21 on branch
`claudeneworgfirsttry`; replaced by this 18.6 tree on 2026-09-01 per Michael's
ruling that the reference pin moves to PostgreSQL 18.6 (see
`docs/conformance/oracle-patch-policy.md`).
