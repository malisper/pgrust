# PostgreSQL 18.3 source — reference copy for comparison only

This folder contains the complete, pristine PostgreSQL 18.3 source tree,
extracted with `git archive` from the upstream `REL_18_3` tag. It exists so
that agents and humans working in this repo can compare pgrust code against
the exact upstream C implementation without leaving the repo.

- **Do not edit anything under this folder.** It is a read-only reference.
- **Do not build from it here.** Build artifacts do not belong in this repo.
- It is the TAG content, not a working checkout — no local patches, no
  c2rust shims (unlike some drifted postgres-18.3 working trees on dev
  machines, which are known to differ from the tag).

Added 2026-08-21 on branch `claudeneworgfirsttry`.
