# pgrust Hegel tests

This directory is for Hegel-backed property tests that we want to keep
separate from ordinary integration tests while the harness is still new.

Planned first properties:

- parser roundtrip / parser robustness
- value codec roundtrip
- JSONB or numeric edge-case generation

Validation rule:

- every new property test needs a targeted rerun command
- every failing case should be shrinkable and preserved as a regression test
  when the bug is fixed
