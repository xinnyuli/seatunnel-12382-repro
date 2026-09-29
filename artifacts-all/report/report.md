# Issue #12382 reproduction report

Exact revisions tested (recorded by the workflow, not typed by hand):

- `baseline` = `c7304ace6e18d350314e92480df1fd3c0962f1f2`
- `dev` = `4c874e2a4061aea9d5db65e74edeb211b498fe27`
- dev contains merged #11864 (`5af8d789aa9ae3d94df4a9cc0f03cee3c0a6d0e6`): **True**
- test-only observation patch sha256: `1d99d850e70c76d7b8e8326c229971fed858d52437e4ca3bb2e9aad89ddab9c5`
- test: `PostgresCDCIT#testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes` (real Zeta savepoint -> restore -> post-reattachment INSERT id=15), zeta container only, ubuntu-latest
- no production code modified; agent is test-only; resume experiment: **not applied**

| target | java | stress | runs | reached id=15 check | PASS | ROW_MISSING | FAIL_AFTER_INSERT | SETUP_TIMEOUT | SETUP_FAIL | NO_TEST_RUN | TIMEOUT | reproduction rate | 95% upper bound |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 11 | on | 30 | 29 | 25 | 4 | 0 | 1 | 0 | 0 | 0 | 13.8% | 28.8% |
| dev | 11 | on | 30 | 30 | 27 | 3 | 0 | 0 | 0 | 0 | 0 | 10.0% | 23.9% |

Rate = ROW_MISSING / runs that reached the id=15 check. Upper bound = exact (Clopper-Pearson) one-sided 95%. Runs that never reached the check (SETUP_*, NO_TEST_RUN, TIMEOUT) are not passes and are not in the denominator.

## What to tell the maintainers

- `baseline` (c7304ace6e18, java 11, stress-ng on): **reproduced** — row id=15 missing in 4/29 runs (13.8%, exact 95% CI 3.9%–31.7%).
- `dev` (4c874e2a4061, java 11, stress-ng on): **reproduced** — row id=15 missing in 3/30 runs (10.0%, exact 95% CI 2.1%–26.5%).

## Captured missing-row runs (boundary evidence)

Only a captured missing-row run can locate the loss. A verdict is attributed only when `trace_valid` is true.

| target | java | run | trace valid | verdict | record LSN | max committed LSN | committed past row |
|---|---|---|---|---|---|---|---|
| baseline | 11 | baseline-j11-n3-36576046533-1 | True | no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1) | None | 35829424 | None |
| baseline | 11 | baseline-j11-n5-36576046533-1 | True | no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1) | None | 35837080 | None |
| baseline | 11 | baseline-j11-n28-36576046533-1 | True | no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1) | None | 35821568 | None |
| baseline | 11 | baseline-j11-n30-36576046533-1 | True | no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1) | None | 35821712 | None |
| dev | 11 | dev-j11-n16-36576046533-1 | True | no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1) | None | 35825192 | None |
| dev | 11 | dev-j11-n20-36576046533-1 | True | no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1) | None | 35834112 | None |
| dev | 11 | dev-j11-n28-36576046533-1 | True | no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1) | None | 35813224 | None |

Raw logs: artifact `run-baseline-j11-28, run-baseline-j11-3, run-baseline-j11-30, run-baseline-j11-5, run-dev-j11-16, run-dev-j11-20, run-dev-j11-28`.

## Caveats

- The tracing agent adds timing overhead; passing with it does not rule out an uninstrumented race. If nothing reproduces, run the same matrix once with `trace=false` to compare (workflow input).
- Hosted runners differ from the original job's runner; a low rate here bounds *this* setup only.
- SETUP_TIMEOUT (the `committed LSN >= postSeedLsn` wait, ~line 489) is a different failure from #12382 and is reported on its own.
