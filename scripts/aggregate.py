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

CATS = ["PASS", "ROW_MISSING", "FAIL_AFTER_INSERT", "SETUP_TIMEOUT", "SETUP_FAIL", "NO_TEST_RUN", "TIMEOUT"]


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
        key = (r.get("target", "?"), r.get("sha", "?"), str(r.get("java", "?")))
        groups.setdefault(key, []).append(r)
    rows = []
    for (target, sha, java), rs in groups.items():
        c = Counter(r["category"] for r in rs)
        reached = c["PASS"] + c["ROW_MISSING"] + c["FAIL_AFTER_INSERT"]
        x = c["ROW_MISSING"]
        rows.append({
            "target": target, "sha": sha, "java": java, "runs": len(rs),
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
    if meta.get("dev_contains_11864") is not None:
        L.append("- dev contains merged #11864 (`5af8d789aa9ae3d94df4a9cc0f03cee3c0a6d0e6`): **%s**" % meta["dev_contains_11864"])
    if meta.get("observation_patch_sha256"):
        L.append("- test-only observation patch sha256: `%s`" % meta["observation_patch_sha256"])
    L.append("- test: `PostgresCDCIT#testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes` (real Zeta savepoint -> restore -> post-reattachment INSERT id=15), zeta container only, ubuntu-latest")
    L.append("- no production code modified; agent is test-only; resume experiment: **not applied**\n")
    if expected is not None and found < expected:
        L.append("> **WARNING:** %d of %d expected runs uploaded no result (job crashed or was cancelled). They are excluded below.\n" % (expected - found, expected))
    L.append("| target | java | runs | reached id=15 check | PASS | ROW_MISSING | FAIL_AFTER_INSERT | SETUP_TIMEOUT | SETUP_FAIL | NO_TEST_RUN | TIMEOUT | reproduction rate | 95% upper bound |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        c = r["counts"]
        L.append("| %s | %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %s | %s |" % (
            r["target"], r["java"], r["runs"], r["reached_check"], c["PASS"], c["ROW_MISSING"], c["FAIL_AFTER_INSERT"],
            c["SETUP_TIMEOUT"], c["SETUP_FAIL"], c["NO_TEST_RUN"], c["TIMEOUT"], pct(r["rate"]), pct(r["upper95_one_sided"])))
    L.append("\nRate = ROW_MISSING / runs that reached the id=15 check. Upper bound = exact (Clopper-Pearson) one-sided 95%. "
             "Runs that never reached the check (SETUP_*, NO_TEST_RUN, TIMEOUT) are not passes and are not in the denominator.\n")

    L.append("## What to tell the maintainers\n")
    for r in rows:
        n, x = r["reached_check"], r["row_missing"]
        who = "`%s` (%s, java %s)" % (r["target"], r["sha"][:12], r["java"])
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
    if any(r["untrusted_trace"] for r in rows):
        L.append("> Some counted runs have `trace_valid=false` (agent not loaded / hook missing / TRACE_ERROR). Their outcome is valid; their boundary evidence is not.\n")
    L.append("## Caveats\n")
    L.append("- The tracing agent adds timing overhead; passing with it does not rule out an uninstrumented race. "
             "If nothing reproduces, run the same matrix once with `trace=false` to compare (workflow input).")
    L.append("- Hosted runners differ from the original job's runner; a low rate here bounds *this* setup only.")
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
