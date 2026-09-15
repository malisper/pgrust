
# pgrust overlay of src/test/recovery/t/017_shm.pl (applied over the pristine
# vendored copy by the TAP runner; the vendor file is never edited).
#
# The upstream test proves the postmaster's pre-existing-shared-memory check:
# it SIGKILLs the postmaster while a backend keeps running, then expects a
# fresh startup to refuse with "pre-existing shared memory block", and
# finally `pg_ctl kill QUIT <backend>` to clean the orphan up. That premise
# is the process-per-backend model: a backend is a separate OS process that
# survives its postmaster. In pgrust backends are threads of the one server
# process — SIGKILL takes every backend with it, no orphan can hold the
# segment, and the check has nothing to detect. The scenario has no
# analogue, so the file is skipped here rather than asserting a failure.
#
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all =>
  'pgrust: backends are threads of the server process; a backend cannot outlive a SIGKILLed postmaster, so the orphaned-shared-memory arm has no analogue';
