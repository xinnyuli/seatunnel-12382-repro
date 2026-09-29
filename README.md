# seatunnel-12382-repro

Repeats the id=15 restore assertion of apache/seatunnel#12382
(`PostgresCDCIT#testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes`) on GitHub-hosted runners,
one run per parallel job, at the failing revision and at current dev, and reports the reproduction rate.

Test-only: `patches/observation.patch` adds log lines to the IT; `agent/` is a javaagent loaded into the
test's SeaTunnel server JVM. No production code is changed. `patches/flush-gate.EXPERIMENTAL.patch` is the
separate pause experiment; the workflow never applies it.

## Use
1. Create a **public** repo on your GitHub account (public = free runner minutes), push this folder.
2. Actions -> `repro-12382` -> Run workflow. Start with `runs=1` as a smoke test (checks agent build,
   patch, Docker, classifier), then `runs=10` or more.
3. Inputs: `runs` (per target and Java), `targets` (`baseline,dev`), `java` (`11` = the failing CI job;
   `8,11` for both), `dev_sha` (pin dev), `trace` (`false` = uninstrumented control), `stress` (`true` = stress-ng contention while the IT runs, reported as its own group), `max_parallel`.
4. The run's Summary page shows the report; artifact `report` has `report.md/json`, each `run-*` artifact has
   `result.json`, `summary.txt`, `mvn.log.gz` and failsafe reports.

Optional secrets `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` avoid Docker Hub anonymous pull limits when many jobs start together.

## Categories (one per run)
PASS / ROW_MISSING (the #12382 signature) / FAIL_AFTER_INSERT / SETUP_TIMEOUT (the ~line-489 wait, a different failure) /
SETUP_FAIL / NO_TEST_RUN (incl. patch does not apply) / TIMEOUT. Reproduction rate = ROW_MISSING over runs that
reached the id=15 check; setup failures are never counted as passes. Boundary verdicts (B1 fetcher handoff,
B2 reader emission, B3 sink) are only attributed for a ROW_MISSING run with a valid trace.

## Not verified before you run it
The classifier, aggregator, run script and patching were tested on your real logs and a real c7304ace checkout with a
stubbed Maven. The workflow YAML, the agent build (needs Maven Central) and the real container run could not be run
from where this was written; the first `runs=1` dispatch is that test.

## Local tests
`python3 -m unittest discover -s tests`
