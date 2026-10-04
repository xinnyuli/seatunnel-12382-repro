#!/usr/bin/env python3
"""Aggregate result.json files from all runs into report.md / report.json.

Reproduction rate = ROW_MISSING / runs that reached the id=15 check (PASS + ROW_MISSING +
FAIL_AFTER_INSERT). Runs that never reached it (SETUP_TIMEOUT, NO_TEST_RUN, ...) are reported
separately and are NOT counted as passes: they say nothing about the bug.
"""
import argparse
import glob
import json
import math
import os
import sys
from collections import Counter, OrderedDict

CATS = ["PASS", "ROW_MISSING", "FAIL_AFTER_INSERT", "SETUP_TIMEOUT", "SETUP_FAIL", "NO_TEST_RUN", "TIMEOUT", "FAIL_UNKNOWN"]


def _binom_cdf(k, n, p):
    if p <= 0:
        return 1.0
    if p >= 1:
        return 1.0 if k >= n else 0.0
    return sum(math.comb(n, i) * p ** i * (1 - p) ** (n - i) for i in range(0, k + 1))


def _bisect(f, lo=0.0, hi=1.0):
    for _ in range(200):
        mid = (lo + hi) / 2
        if f(mid) > 0:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def upper_bound(x, n, alpha=0.05):
    """Exact (Clopper-Pearson) one-sided upper confidence bound for a binomial proportion."""
    if n == 0:
        return None
    if x >= n:
        return 1.0
    # P(X <= x | p) decreases in p; find p where it equals alpha
    return _bisect(lambda p: _binom_cdf(x, n, p) - alpha)


def lower_bound(x, n, alpha=0.05):
    if n == 0 or x == 0:
        return 0.0 if n else None
    # P(X >= x | p) = 1 - cdf(x-1, p) increases in p; find where it equals alpha
    return _bisect(lambda p: -(1 - _binom_cdf(x - 1, n, p) - alpha))


def pct(v):
    return "n/a" if v is None else "%.1f%%" % (100 * v)


def load(root):
    out = []
    for path in sorted(glob.glob(os.path.join(root, "**", "result.json"), recursive=True)):
        with open(path) as fh:
            d = json.load(fh)
        d["_path"] = os.path.relpath(path, root)
        out.append(d)
    return out


def summarize(results):
    groups = OrderedDict()
    for r in sorted(results, key=lambda r: (r.get("target", ""), str(r.get("java", "")), r.get("idx", 0))):
        key = (r.get("target", "?"), r.get("sha", "?"), str(r.get("java", "?")), bool(r.get("stress", False)), r.get("instrumentation", "full"), bool(r.get("dbz_logs", False)),
               r.get("test_from_sha") or "", bool(r.get("wal_range", False)))
        groups.setdefault(key, []).append(r)
    rows = []
    for (target, sha, java, stress, instr, dbz, test_from, walr), rs in groups.items():
        c = Counter(r["category"] for r in rs)
        reached = c["PASS"] + c["ROW_MISSING"] + c["FAIL_AFTER_INSERT"]
        x = c["ROW_MISSING"]
        rows.append({
            "target": target, "sha": sha, "java": java, "stress": stress, "instrumentation": instr, "dbz_logs": dbz, "runs": len(rs),
            "test_from_sha": test_from or None, "wal_range": walr,
            "counts": {k: c.get(k, 0) for k in CATS},
            "reached_check": reached,
            "row_missing": x,
            "rate": (x / reached) if reached else None,
            "upper95_one_sided": upper_bound(x, reached),
            "ci95_two_sided": [lower_bound(x, reached, 0.025), upper_bound(x, reached, 0.025)] if reached else None,
            "untrusted_trace": sum(1 for r in rs if r["category"] in ("PASS", "ROW_MISSING", "FAIL_AFTER_INSERT") and not r.get("trace_valid")),
            "row_missing_runs": [
                {"idx": r.get("idx"), "verdict": r.get("boundary_verdict"), "trace_valid": r.get("trace_valid"),
                 "event_lsn": r.get("event_lsn"), "max_committed_lsn": r.get("max_committed_lsn"),
                 "committed_past_row": r.get("committed_past_row"), "run_id": r.get("run_id"), "artifact": r.get("_path")}
                for r in rs if r["category"] == "ROW_MISSING"],
            # Debezium WAL resume check: does "the stored COMMIT-end LSN was mistaken for a processed event" separate
            # missing-row runs from passing ones? Only runs whose log contains the resume search are counted.
            "dbz": {cat: {
                "logs_seen": sum(1 for r in rs if r["category"] == cat and r.get("dbz_logs_seen")),
                "false_match": sum(1 for r in rs if r["category"] == cat and r.get("dbz_false_match") is True),
            } for cat in ("PASS", "ROW_MISSING")},
            "dbz_runs": [
                {"category": r["category"], "run_id": r.get("run_id") or r.get("idx"), "false_match": r.get("dbz_false_match"),
                 "stored_change_lsn": (r.get("dbz_resume") or {}).get("stored_change_lsn"),
                 "stored_is_commit_end": (r.get("dbz_resume") or {}).get("stored_is_commit_end"),
                 "resume_minus_stored": (r.get("dbz_resume") or {}).get("resume_minus_stored"),
                 "skipped_at_stored": ((r.get("dbz_resume") or {}).get("stored_change_lsn") in ((r.get("dbz_resume") or {}).get("skipped_lsns") or []))
                                      if r.get("dbz_resume") else None}
                for r in rs if r.get("dbz_logs_seen") and r["category"] in ("ROW_MISSING", "PASS")],
            # The ambiguous boundary itself (first WAL message on restart sits at the stored COMMIT-end LSN), split
            # by outcome. Unfixed code should lose the row on every hit; a fix should deliver it on every hit.
            "boundary": {cat: sum(1 for r in rs if r["category"] == cat and r.get("dbz_boundary_hit") is True)
                         for cat in ("PASS", "ROW_MISSING")},
            "boundary_runs": [
                {"category": r["category"], "run_id": r.get("run_id") or r.get("idx"),
                 "stored": (r.get("dbz_resume") or {}).get("stored_change_lsn"),
                 "first": (r.get("dbz_resume") or {}).get("first_lsn"),
                 "resume_minus_stored": (r.get("dbz_resume") or {}).get("resume_minus_stored"),
                 "false_match": r.get("dbz_false_match"),
                 "wal": r.get("id15_wal"), "eq": r.get("wal_equality")}
                for r in rs if r.get("dbz_boundary_hit") is True or (r["category"] == "ROW_MISSING" and r.get("id15_wal"))],
        })
    return rows


def render(rows, expected, found, meta):
    L = []
    L.append("# Issue #12382 reproduction report\n")
    L.append("Exact revisions tested (recorded by the workflow, not typed by hand):\n")
    seen = OrderedDict()
    for r in rows:
        seen[(r["target"], r["sha"])] = True
    for (t, s) in seen:
        L.append("- `%s` = `%s`" % (t, s))
    for tf in OrderedDict((r["test_from_sha"], 1) for r in rows if r.get("test_from_sha")):
        L.append("- some groups run the IT file taken from `%s` (test file only; production code is the group's own revision)" % tf)
    if meta.get("dev_contains_11864") is not None:
        L.append("- dev contains merged #11864 (`5af8d789aa9ae3d94df4a9cc0f03cee3c0a6d0e6`): **%s**" % meta["dev_contains_11864"])
    if meta.get("observation_patch_sha256"):
        L.append("- test-only observation patch sha256: `%s`" % meta["observation_patch_sha256"])
    L.append("- test: `PostgresCDCIT#testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes` (real Zeta savepoint -> restore -> post-reattachment INSERT id=15), zeta container only, ubuntu-latest")
    L.append("- no production code modified; agent is test-only; resume experiment: **not applied**\n")
    if expected is not None and found < expected:
        L.append("> **WARNING:** %d of %d expected runs uploaded no result (job crashed or was cancelled). They are excluded below.\n" % (expected - found, expected))
    L.append("| target | java | instrumentation | stress | debezium logs | runs | reached id=15 check | PASS | ROW_MISSING | FAIL_AFTER_INSERT | SETUP_TIMEOUT | SETUP_FAIL | NO_TEST_RUN | TIMEOUT | FAIL_UNKNOWN | reproduction rate | 95% upper bound |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        c = r["counts"]
        L.append("| %s | %s | %s | %s | %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %s | %s |" % (
            r["target"] + (" @ " + r["sha"][:9]) + (" (IT from " + r["test_from_sha"][:9] + ")" if r.get("test_from_sha") else "") + (" +wal-range" if r.get("wal_range") else ""), r["java"], r["instrumentation"], "on" if r["stress"] else "off", "on" if r.get("dbz_logs") else "off", r["runs"], r["reached_check"], c["PASS"], c["ROW_MISSING"], c["FAIL_AFTER_INSERT"],
            c["SETUP_TIMEOUT"], c["SETUP_FAIL"], c["NO_TEST_RUN"], c["TIMEOUT"], c["FAIL_UNKNOWN"], pct(r["rate"]), pct(r["upper95_one_sided"])))
    L.append("\nRate = ROW_MISSING / runs that reached the id=15 check. Upper bound = exact (Clopper-Pearson) one-sided 95%. "
             "Runs that never reached the check (SETUP_*, NO_TEST_RUN, TIMEOUT) are not passes and are not in the denominator.\n")

    L.append("## What to tell the maintainers\n")
    for r in rows:
        n, x = r["reached_check"], r["row_missing"]
        who = "`%s` (%s, java %s%s)" % (r["target"], r["sha"][:12], r["java"], (", stress-ng on" if r["stress"] else "") + (", instrumentation=" + r["instrumentation"] if r["instrumentation"] != "full" else "") + (", debezium logs on" if r.get("dbz_logs") else ""))
        if n == 0:
            L.append("- %s: no run reached the id=15 assertion (%d runs); no conclusion." % (who, r["runs"]))
        elif x == 0:
            L.append("- %s: **not reproduced** — id=15 assertion passed in %d/%d runs that reached it (0 ROW_MISSING); 95%% upper bound on the per-run failure rate is %s. Other outcomes: setup timeouts %d, other failures %d." % (
                who, r["counts"]["PASS"], n, pct(r["upper95_one_sided"]), r["counts"]["SETUP_TIMEOUT"] + r["counts"]["SETUP_FAIL"], r["counts"]["FAIL_AFTER_INSERT"]))
        else:
            lo, hi = r["ci95_two_sided"]
            L.append("- %s: **reproduced** — row id=15 missing in %d/%d runs (%s, exact 95%% CI %s–%s)." % (who, x, n, pct(r["rate"]), pct(lo), pct(hi)))
    L.append("")
    lost = [(r, m) for r in rows for m in r["row_missing_runs"]]
    if lost:
        L.append("## Captured missing-row runs (boundary evidence)\n")
        L.append("Only a captured missing-row run can locate the loss. A verdict is attributed only when `trace_valid` is true.\n")
        L.append("| target | java | run | trace valid | verdict | record LSN | max committed LSN | committed past row |")
        L.append("|---|---|---|---|---|---|---|---|")
        for r, m in lost:
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                r["target"], r["java"], m["run_id"] or m["idx"], m["trace_valid"], m["verdict"], m["event_lsn"], m["max_committed_lsn"], m["committed_past_row"]))
        L.append("\nRaw logs: artifact `%s`.\n" % ", ".join(sorted({m["artifact"].split("/")[0] for _, m in lost})))
    else:
        L.append("No missing-row run was captured, so **no boundary can be named** from this data. "
                 "Passing runs only show the tracing works on the normal path.\n")
    if any(r.get("dbz_logs") for r in rows):
        L.append("## Debezium WAL resume check (hypothesis: stored COMMIT-end LSN mistaken for a processed event)\n")
        L.append("`false match` = the restored offset had lsn_proc == lsn_commit, Debezium filtered a replayed message **at exactly that LSN** "
                 "as 'already processed', and resumed after it. If this explains #12382 it should appear in (nearly) every ROW_MISSING run "
                 "and in no PASS run. Runs whose log lacks the resume search are not counted (logging not effective).\n")
        L.append("| target | java | instrumentation | stress | ROW_MISSING with logs | ...of which false match | PASS with logs | ...of which false match |")
        L.append("|---|---|---|---|---|---|---|---|")
        for r in rows:
            if not r.get("dbz_logs"):
                continue
            d = r["dbz"]
            L.append("| %s | %s | %s | %s | %d | %d | %d | %d |" % (r["target"], r["java"], r["instrumentation"], "on" if r["stress"] else "off",
                     d["ROW_MISSING"]["logs_seen"], d["ROW_MISSING"]["false_match"], d["PASS"]["logs_seen"], d["PASS"]["false_match"]))
        miss = [(r, m) for r in rows for m in r.get("dbz_runs", []) if m["category"] == "ROW_MISSING"]
        if miss:
            L.append("\n| missing-row run | stored lsn_proc | stored is COMMIT end | replayed msg at stored LSN filtered | resume - stored (bytes) | false match |")
            L.append("|---|---|---|---|---|---|")
            for r, m in miss:
                L.append("| %s | %s | %s | %s | %s | %s |" % (m["run_id"], m["stored_change_lsn"], m["stored_is_commit_end"],
                         m["skipped_at_stored"], m["resume_minus_stored"], m["false_match"]))
        L.append("")
    if any(r.get("dbz_logs") for r in rows):
        L.append("## Ambiguous boundary hits (stored COMMIT-end LSN == first WAL message on restart)\n")
        L.append("A hit is the exact condition DBZ-6204 / PR #12454 is about. Unfixed code should lose the row on a hit; "
                 "fixed code should replay that transaction (resume == stored) and deliver it.\n")
        L.append("| group | hits that lost the row | hits that delivered the row |")
        L.append("|---|---|---|")
        for r in rows:
            if r.get("dbz_logs"):
                L.append("| %s @ %s%s | %d | %d |" % (r["target"], r["sha"][:9], (" (IT from %s)" % r["test_from_sha"][:9]) if r.get("test_from_sha") else "",
                                                   r["boundary"]["ROW_MISSING"], r["boundary"]["PASS"]))
        br = [(r, m) for r in rows for m in r.get("boundary_runs", [])]
        if br:
            def h(v):
                return "%X/%X" % (v >> 32, v & 0xFFFFFFFF) if isinstance(v, int) else "-"
            L.append("\n| run | outcome | stored offset (lsn_proc) | first LSN on restart | skipped as already processed | id=15 WAL range (insert before -> after) | id=15 starts at stored | resume - stored |")
            L.append("|---|---|---|---|---|---|---|---|")
            for r, m in br:
                w, e = m.get("wal") or {}, m.get("eq") or {}
                L.append("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                    m["run_id"], m["category"], h(m["stored"]), h(m["first"]), h(e.get("skipped_lsn")),
                    ("%s -> %s" % (h(w.get("insert_before")), h(w.get("insert_after")))) if w else "-",
                    e.get("id15_starts_at_stored", "-"), m["resume_minus_stored"]))
        L.append("")
    if any(r["untrusted_trace"] for r in rows):
        L.append("> Some counted runs have `trace_valid=false` (agent not loaded / hook missing / TRACE_ERROR). Their outcome is valid; their boundary evidence is not.\n")
    L.append("## Caveats\n")
    L.append("- The tracing agent adds timing overhead; passing with it does not rule out an uninstrumented race. "
             "Compare against the `instrumentation=none` group (no IT log lines, no agent) when it exists; a difference between groups "
             "suggests observation changes the outcome, equal results only mean neither reproduced under these conditions.")
    L.append("- Hosted runners differ from the original job's runner; a low rate here bounds *this* setup only.")
    if any(r.get("dbz_logs") for r in rows):
        L.append("- `debezium logs on` raises three Debezium loggers to INFO in the test container's log4j2.properties (test resource, config only). "
                 "Extra logging can shift timing slightly, so its rate is reported as its own group.")
    L.append("- SETUP_TIMEOUT (the `committed LSN >= postSeedLsn` wait, ~line 489) is a different failure from #12382 and is reported on its own.")
    return "\n".join(L) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True, help="directory containing result.json files (searched recursively)")
    ap.add_argument("--expected", type=int, default=None)
    ap.add_argument("--meta", default=None, help="path to plan.json (revisions, dev_contains_11864, patch hash)")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args(argv)
    results = load(args.results)
    meta = {}
    if args.meta and os.path.exists(args.meta):
        with open(args.meta) as fh:
            meta = json.load(fh)
    rows = summarize(results)
    os.makedirs(args.out_dir, exist_ok=True)
    md = render(rows, args.expected, len(results), meta)
    with open(os.path.join(args.out_dir, "report.md"), "w") as fh:
        fh.write(md)
    with open(os.path.join(args.out_dir, "report.json"), "w") as fh:
        json.dump({"meta": meta, "groups": rows, "runs_found": len(results), "runs_expected": args.expected}, fh, indent=2)
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
