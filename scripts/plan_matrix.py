#!/usr/bin/env python3
"""Build the run matrix + plan.json (exact revisions) for the workflow. No network access here:
the workflow resolves/validates SHAs first and passes them in."""
import argparse
import json
import re
import sys

BASELINE_SHA = "c7304ace6e18d350314e92480df1fd3c0962f1f2"   # failing scheduled run (all-connectors-it-7)
FIX_11864 = "5af8d789aa9ae3d94df4a9cc0f03cee3c0a6d0e6"      # merged #11864
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def plan(targets, runs, javas, dev_sha, dev_contains_fix, patch_sha):
    if runs < 1:
        raise SystemExit("runs must be >= 1")
    javas = [j.strip() for j in javas.split(",") if j.strip()]
    for j in javas:
        if j not in ("8", "11"):
            raise SystemExit("java must be 8 and/or 11 (upstream CI matrix), got %r" % j)
    targets = [t.strip() for t in targets.split(",") if t.strip()]
    shas = {"baseline": BASELINE_SHA}
    if "dev" in targets:
        if not dev_sha or not SHA_RE.match(dev_sha):
            raise SystemExit("dev requested but no valid dev sha")
        shas["dev"] = dev_sha
    include = []
    for t in targets:
        if t not in shas:
            raise SystemExit("unknown target %r (use baseline,dev)" % t)
        for j in javas:
            for i in range(1, runs + 1):
                include.append({"target": t, "sha": shas[t], "java": j, "idx": i})
    if len(include) > 256:
        raise SystemExit("matrix would have %d jobs; GitHub allows 256" % len(include))
    return {
        "matrix": {"include": include},
        "expected": len(include),
        "revisions": {t: shas[t] for t in targets},
        "dev_contains_11864": dev_contains_fix if "dev" in targets else None,
        "fix_11864": FIX_11864,
        "observation_patch_sha256": patch_sha,
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", default="baseline,dev")
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--java", default="11")
    ap.add_argument("--dev-sha", default="")
    ap.add_argument("--dev-contains-11864", default="")
    ap.add_argument("--patch-sha256", default="")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    contains = {"true": True, "false": False}.get(a.dev_contains_11864.lower())
    p = plan(a.targets, a.runs, a.java, a.dev_sha, contains, a.patch_sha256)
    with open(a.out, "w") as fh:
        json.dump(p, fh, indent=2)
    print(json.dumps(p["matrix"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
