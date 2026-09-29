#!/usr/bin/env python3
"""Classify ONE run of PostgresCDCIT#testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes.

Input is the raw Maven log (or a summary made from it). Only test-emitted markers
("[PG12382] NAME job=<id> ...") and agent lines ("[PG12382] run=<r> job=<id> ... stage=<s> ...")
are read, so diff text quoted in a summary never counts as evidence.

Categories (exactly one per run):
  PASS               test passed and the sink query saw id=15 exactly once
  ROW_MISSING        restore + INSERT id=15 happened, the 180 s wait ended with count=0
                     (this is the #12382 signature)
  SETUP_TIMEOUT      ConditionTimeout before the restored slot was active (e.g. the line-489
                     "committed LSN >= postSeedLsn" wait); the id=15 check was never reached
  SETUP_FAIL         failed before the restored slot was active, not a timeout
  FAIL_UNKNOWN       (uninstrumented control only) failed, but without markers the phase cannot be told
  FAIL_AFTER_INSERT  INSERT happened, test failed for a reason other than a plain count=0 timeout
  NO_TEST_RUN        Maven finished but the test method did not run (0 tests / build failure /
                     container start failure)
  TIMEOUT            the workflow's wall-clock limit killed Maven (exit 124/137)

A separate flag `trace_valid` says whether the agent loaded, every hook class was
instrumented and no TRACE_ERROR was printed. Missing row-stage markers alone are NOT data loss
evidence when trace_valid is false.
"""
import argparse
import json
import re
import sys

TARGET_ID = "15"

# Order matters: it is the path one record takes. boundary = Daniel's three boundaries.
STAGES = [
    ("changeRecord_ENTER", "B1 WAL fetch task handoff"),
    ("enqueue_RETURN", "B1 WAL fetch task handoff"),
    ("shouldEmit_RETURN", "B1 WAL fetch task handoff"),
    ("processElement_RETURN", "B2 reader emission"),
    ("collect_RETURN", "B2 reader emission"),
    ("write_ENTER", "B3 sink receipt/commit"),
    ("addToBatch_RETURN", "B3 sink receipt/commit"),
    ("attemptFlush_RETURN", "B3 sink receipt/commit"),
]
SINK_STAGE = "SINK_QUERY_count1"

REQUIRED_HOOKS = [
    "io.debezium.connector.base.ChangeEventQueue",
    "io.debezium.pipeline.EventDispatcher$StreamingChangeRecordReceiver",
    "org.apache.seatunnel.connectors.cdc.base.source.reader.external.IncrementalSourceStreamFetcher",
    "org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter",
    "org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter$OutputCollector",
    "org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceReader",
    "org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.wal.PostgresWalFetchTask",
    "org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkWriter",
    "org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat",
    "org.postgresql.jdbc.PgConnection",
]

AGENT_LINE = re.compile(r"\[PG12382\] run=(?P<run>\S+) job=(?P<job>\S+) seq=(?P<seq>\d+) .*?stage=(?P<stage>\w+)(?P<rest>.*)$")
FIRST_ID = re.compile(r"(?<![\w.])id=(\d+)")
TEST_MARK = re.compile(r"PostgresCDCIT - \[PG12382\] (?P<name>RESTORED_SLOT_ACTIVE|SOURCE_INSERT_RETURN|SINK_QUERY) job=(?P<job>\d+)(?P<rest>.*)$")
SINK_COUNT = re.compile(r"id=" + TARGET_ID + r" count=(?P<count>\d+)")
LSN_FIELD = re.compile(r"\blsn=(\d+)")
EVENT_LSN = re.compile(r"eventLsn=(\d+)")
TESTS_RUN_ANY = re.compile(r"Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+)")
HOOK = re.compile(r"\[PG12382\] HOOK class=(\S+)")


def analyze(lines, maven_exit, bare=False):
    """bare=True: the run had neither the IT log lines nor the agent (instrumentation=none), so there are no
    [PG12382] markers. Outcome is then read from Maven's result and the two awaitility messages only."""
    hooks = set()
    agent_ready = False
    trace_errors = []
    stages_seen = {}          # stage -> first line (only lines about id=15)
    shouldemit_accepted = []  # list of bool
    event_lsn = None
    restored_job = None
    restored = insert_returned = False
    sink_counts = []
    commits = []              # (job, lsn)
    tests = None
    final_tests = None
    error_lines = []
    fail_line_kinds = set()
    setup_wait_msg = False

    for raw in lines:
        line = raw.rstrip("\n")
        if line.startswith(("+", "-")):  # quoted diff text is never evidence
            continue
        if "[PG12382]" not in line and "Tests run:" not in line and "[ERROR]" not in line and "ConditionTimeout" not in line:
            continue
        m = HOOK.search(line)
        if m:
            hooks.add(m.group(1))
        if "[PG12382] AGENT_READY" in line:
            agent_ready = True
        if "[PG12382] TRACE_ERROR" in line or "[Byte Buddy] ERROR" in line:
            trace_errors.append(line.strip()[-200:])
        tm = TEST_MARK.search(line)
        if tm:
            name = tm.group("name")
            if name == "RESTORED_SLOT_ACTIVE":
                restored = True
                restored_job = tm.group("job")
            elif name == "SOURCE_INSERT_RETURN":
                insert_returned = True
            elif name == "SINK_QUERY":
                sc = SINK_COUNT.search(tm.group("rest"))
                if sc:
                    sink_counts.append(int(sc.group("count")))
            continue
        am = AGENT_LINE.search(line)
        if am:
            stage = am.group("stage")
            rest = am.group("rest")
            if stage == "commitCurrentOffset_RETURN":
                # requestedOffset={lsn=...,...}; only the LSN of the requested offset is used
                lm = LSN_FIELD.search(rest)
                if lm:
                    commits.append((am.group("job"), int(lm.group(1))))
            elif (FIRST_ID.search(rest) or [None, None])[1] == TARGET_ID:
                if stage not in stages_seen:
                    stages_seen[stage] = line.strip()[-240:]
                if stage == "shouldEmit_RETURN":
                    shouldemit_accepted.append("accepted=true" in rest)
                if stage == "changeRecord_ENTER" or stage == "enqueue_ENTER":
                    em = EVENT_LSN.search(rest)
                    if em and event_lsn is None:
                        event_lsn = int(em.group(1))
                if stage in ("processElement_ENTER", "shouldEmit_RETURN") and event_lsn is None:
                    em = EVENT_LSN.search(rest)
                    if em:
                        event_lsn = int(em.group(1))
            continue
        # Maven / failsafe
        tr = TESTS_RUN_ANY.search(line)
        if tr and "Time elapsed" not in line and line.strip().startswith(("[INFO]", "[ERROR]", "[WARNING]")):
            final_tests = tuple(int(x) for x in tr.groups())  # last aggregate line wins
        if line.startswith("[ERROR]") and "PostgresCDCIT" in line:
            error_lines.append(line.strip()[:300])
            if "ConditionTimeout" in line:
                fail_line_kinds.add("condition_timeout")
        if "expected: <1> but was: <0>" in line:
            fail_line_kinds.add("count0")
        if "expected: <true> but was: <false> within 30 seconds" in line:
            setup_wait_msg = True

    hook_missing = [h for h in REQUIRED_HOOKS if h not in hooks]
    trace_valid = bool(agent_ready and not trace_errors and not hook_missing) and not bare

    count1 = any(c == 1 for c in sink_counts)
    last_count = sink_counts[-1] if sink_counts else None
    tests_ran = final_tests is not None and final_tests[0] >= 1
    tests_failed = final_tests is not None and (final_tests[1] + final_tests[2]) > 0

    # ---- category -------------------------------------------------------
    if maven_exit in (124, 137, 143):
        category = "TIMEOUT"
    elif bare:
        if not tests_ran:
            category = "NO_TEST_RUN"
        elif maven_exit == 0 and not tests_failed:
            category = "PASS"
        elif "count0" in fail_line_kinds:
            category = "ROW_MISSING"           # same message the original failure had (":625 ... <1> but was: <0>")
        elif setup_wait_msg:
            category = "SETUP_TIMEOUT"
        else:
            category = "FAIL_UNKNOWN"           # failed, phase unknown without markers: not counted as reaching the check
    elif not tests_ran and not restored and not sink_counts:
        category = "NO_TEST_RUN"
    elif maven_exit == 0 and tests_ran and not tests_failed and count1:
        category = "PASS"
    elif not restored:
        category = "SETUP_TIMEOUT" if "condition_timeout" in fail_line_kinds else "SETUP_FAIL"
    elif not insert_returned:
        category = "SETUP_FAIL"
    elif last_count == 0 and not count1 and ("condition_timeout" in fail_line_kinds or "count0" in fail_line_kinds):
        category = "ROW_MISSING"
    elif maven_exit == 0:
        category = "NO_TEST_RUN"  # exit 0 but no id=15 count=1 evidence: never count as PASS
    else:
        category = "FAIL_AFTER_INSERT"

    # ---- boundary localisation (meaningful for ROW_MISSING; also recorded for PASS) --
    reached = [name for name, _ in STAGES if name in stages_seen]
    if shouldemit_accepted and not any(shouldemit_accepted):
        # shouldEmit was called but rejected the record
        reached = [s for s in reached if s not in ("processElement_RETURN", "collect_RETURN", "write_ENTER", "addToBatch_RETURN", "attemptFlush_RETURN")]
    path = [n for n, _ in STAGES] + [SINK_STAGE]
    have = set(reached) | ({SINK_STAGE} if count1 else set())
    first_missing = next((p for p in path if p not in have), None)
    last_seen = next((p for p in reversed(path) if p in have), None)
    boundary_of = dict(STAGES)
    boundary_of[SINK_STAGE] = "B3 sink receipt/commit"
    if bare:
        verdict = "uninstrumented control: no boundary evidence by design"
    elif category == "ROW_MISSING":
        if not trace_valid:
            verdict = "trace incomplete: cannot attribute the loss to a boundary"
        elif last_seen is None:
            verdict = "no id=15 record observed at PostgresWalFetchTask/Debezium receiver (before B1)"
        elif shouldemit_accepted and not any(shouldemit_accepted):
            verdict = "record reached the shared fetcher but shouldEmit rejected it (B1)"
        else:
            verdict = "last observed: %s (%s); next expected but absent: %s (%s)" % (
                last_seen, boundary_of[last_seen], first_missing, boundary_of.get(first_missing, "?"))
    elif category == "PASS":
        verdict = "n/a (row delivered)" if first_missing is None else "row delivered; stage %s not observed" % first_missing
    else:
        verdict = "n/a"

    # ---- committed LSN vs. the row ----------------------------------------
    job_commits = [l for j, l in commits if restored_job is None or j == restored_job]
    max_commit = max(job_commits) if job_commits else None
    committed_past_row = None
    if event_lsn is not None and max_commit is not None:
        committed_past_row = max_commit > event_lsn
    invariant_violation = None
    if category == "ROW_MISSING":
        # regression target: delivered OR committed LSN not past the record
        invariant_violation = (committed_past_row is True) if committed_past_row is not None else None

    return {
        "category": category,
        "reached_check": category in ("PASS", "ROW_MISSING", "FAIL_AFTER_INSERT"),
        "trace_valid": trace_valid,
        "agent_ready": agent_ready,
        "hooks_missing": hook_missing,
        "trace_errors": trace_errors[:5],
        "maven_exit": maven_exit,
        "tests_run": list(final_tests) if final_tests else None,
        "restored_slot_active": restored,
        "insert_returned": insert_returned,
        "sink_query_counts_seen": sorted(set(sink_counts)),
        "sink_query_last": last_count,
        "stages_reached": reached + ([SINK_STAGE] if count1 else []),
        "first_missing_stage": first_missing if category in ("ROW_MISSING", "PASS") else None,
        "boundary_verdict": verdict,
        "event_lsn": event_lsn,
        "max_committed_lsn": max_commit,
        "committed_past_row": committed_past_row,
        "invariant_violation": invariant_violation,
        "failure_lines": error_lines[:5],
    }


def read_lines(path):
    """Read a log as text. Windows PowerShell 5 `*>` redirection writes UTF-16 (BOM); CI logs are UTF-8."""
    with open(path, "rb") as fh:
        head = fh.read(4)
    if head[:2] in (b"\xff\xfe", b"\xfe\xff"):
        enc = "utf-16"
    elif head[:3] == b"\xef\xbb\xbf":
        enc = "utf-8-sig"
    else:
        enc = "utf-8"
    with open(path, "r", encoding=enc, errors="replace") as fh:
        return fh.read().splitlines()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--maven-exit", type=int, required=True)
    ap.add_argument("--meta", default="{}", help="JSON object merged into the result")
    ap.add_argument("--bare", action="store_true", help="instrumentation=none run (no markers exist)")
    ap.add_argument("--out", default="-")
    args = ap.parse_args(argv)
    result = analyze(read_lines(args.log), args.maven_exit, bare=args.bare)
    result.update(json.loads(args.meta))
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.out == "-":
        print(text)
    else:
        with open(args.out, "w") as fh:
            fh.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
