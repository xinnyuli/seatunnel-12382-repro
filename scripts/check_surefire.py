#!/usr/bin/env python3
"""Read surefire XML reports and check named test cases against an expected outcome.

Usage: check_surefire.py <reports dir> <Class#method=pass|fail> [...]
Writes a markdown table to stdout and exits 0 only if every expectation holds and
every listed test actually ran (a test that did not run is never counted as a pass).
"""
import glob
import os
import sys
import xml.etree.ElementTree as ET


def load(reports_dir):
    results = {}
    for path in glob.glob(os.path.join(reports_dir, "TEST-*.xml")):
        root = ET.parse(path).getroot()
        for case in root.iter("testcase"):
            cls = case.get("classname", "").rsplit(".", 1)[-1]
            name = case.get("name", "").split("(")[0]
            if case.find("failure") is not None or case.find("error") is not None:
                node = case.find("failure") if case.find("failure") is not None else case.find("error")
                outcome = "fail"
                detail = (node.get("message") or node.get("type") or "").strip().splitlines()
                detail = detail[0][:160] if detail else ""
            elif case.find("skipped") is not None:
                outcome, detail = "skipped", ""
            else:
                outcome, detail = "pass", ""
            results[f"{cls}#{name}"] = (outcome, detail)
    return results


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 2
    results = load(argv[1])
    ok = True
    print("| test | expected | actual | detail |")
    print("|---|---|---|---|")
    for spec in argv[2:]:
        key, expected = spec.rsplit("=", 1)
        actual, detail = results.get(key, ("did not run", ""))
        good = actual == expected
        ok &= good
        mark = "" if good else " **(unexpected)**"
        print(f"| `{key}` | {expected} | {actual}{mark} | {detail.replace('|', '/')} |")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
