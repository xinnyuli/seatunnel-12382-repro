"""Debezium WAL resume check (#12382 hypothesis). Log lines use the exact message formats of Debezium 1.9.8
(WalPositionLocator / AbstractMessageDecoder / PostgresStreamingChangeEventSource), wrapped the way the test
container prints them (logger name via %c, MDC job id often empty on Debezium threads)."""
import json, os, sys, tempfile, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import analyze_run as A
import aggregate as G

PFX = "[] 2026-09-29 13:43:52,340 INFO  tc.seatunnel-engine:seatunnelhub/openjdk:8u342 - STDOUT: [] 2026-09-29 13:43:52,339 INFO  "
LOC = PFX + "io.debezium.connector.postgresql.connection.WalPositionLocator - "
DEC = PFX + "io.debezium.connector.postgresql.connection.AbstractMessageDecoder - "
SRC = PFX + "io.debezium.connector.postgresql.PostgresStreamingChangeEventSource - "


def search(commit, change):
    return [SRC + "Retrieved latest position from stored offset 'LSN{%s}'" % change,
            LOC + "Looking for WAL restart position for last commit LSN 'LSN{%s}' and last change LSN 'LSN{%s}'" % (commit, change),
            SRC + "Searching for WAL resume position"]


# Missing-row shape seen in the stress runs: id=15's first record sits exactly at the stored COMMIT-end LSN
# 0/22296E0 (=35821280); its commit ends 312 bytes later (0/2229818).
MISS = search("0/22296E0", "0/22296E0") + [
    LOC + "First LSN 'LSN{0/22296E0}' received",
    LOC + "LSN after last stored change LSN 'LSN{0/2229818}' received",
    SRC + "WAL resume position 'LSN{0/2229818}' discovered",
    DEC + "Streaming requested from LSN LSN{0/22296E0}, received LSN LSN{0/22296E0} identified as already processed",
    DEC + "Streaming requested from LSN LSN{0/22296E0}, received LSN LSN{0/22296E0} identified as already processed",
    LOC + "Message with LSN 'LSN{0/2229818}' arrived, switching off the filtering",
    SRC + "Processing messages",
]
# Normal shape: other WAL sits between the stored LSN and id=15 (+408), so no false match.
PASS = search("0/2228F38", "0/2228F38") + [
    LOC + "First LSN 'LSN{0/22290D0}' received",
    LOC + "Received COMMIT LSN 'LSN{0/2229370}' larger than than last stored commit LSN 'LSN{0/2228F38}'",
    LOC + "Will restart from LSN 'LSN{0/22290D0}' that is start of the first unprocessed transaction",
    SRC + "WAL resume position 'LSN{0/22290D0}' discovered",
    LOC + "Message with LSN 'LSN{0/22290D0}' arrived, switching off the filtering",
]


class Resume(unittest.TestCase):
    def test_missing_row_shape_is_false_match(self):
        r = A.analyze_dbz_resume(MISS)
        self.assertTrue(r["dbz_logs_seen"])
        self.assertTrue(r["dbz_false_match"])
        d = r["dbz_resume"]
        self.assertEqual(d["stored_change_lsn"], 35821280)
        self.assertTrue(d["stored_is_commit_end"])
        self.assertEqual(d["resume_minus_stored"], 312)
        self.assertEqual(d["skipped_lsns"], [35821280, 35821280])
        self.assertEqual(d["filter_off_lsn"], 35821280 + 312)

    def test_normal_resume_is_not_false_match(self):
        r = A.analyze_dbz_resume(PASS)
        self.assertFalse(r["dbz_false_match"])
        self.assertEqual(r["dbz_resume"]["restart_reason"], "is start of the first unprocessed transaction")
        self.assertEqual(r["dbz_resume"]["skipped_lsns"], [])

    def test_last_search_wins(self):
        # the committed-offset job searched earlier; the restore is the last search in the log
        r = A.analyze_dbz_resume(PASS + MISS)
        self.assertEqual(r["dbz_searches"], 2)
        self.assertTrue(r["dbz_false_match"])
        r = A.analyze_dbz_resume(MISS + PASS)
        self.assertFalse(r["dbz_false_match"])

    def test_stored_not_a_commit_end_is_not_counted(self):
        lines = [l.replace("last commit LSN 'LSN{0/22296E0}'", "last commit LSN 'LSN{0/2229000}'") for l in MISS]
        r = A.analyze_dbz_resume(lines)
        self.assertFalse(r["dbz_resume"]["stored_is_commit_end"])
        self.assertFalse(r["dbz_false_match"])

    def test_skip_elsewhere_is_not_false_match(self):
        lines = [l.replace("received LSN LSN{0/22296E0}", "received LSN LSN{0/22296F0}") for l in MISS]
        self.assertFalse(A.analyze_dbz_resume(lines)["dbz_false_match"])

    def test_no_logs(self):
        r = A.analyze_dbz_resume(["[INFO] BUILD SUCCESS"])
        self.assertEqual(r, {"dbz_logs_seen": False, "dbz_searches": 0, "dbz_resume": None, "dbz_false_match": None,
                             "dbz_boundary_hit": None})

    def test_null_commit_lsn_and_diff_text(self):
        lines = [LOC + "Looking for WAL restart position for last commit LSN 'null' and last change LSN 'LSN{0/10}'",
                 "+" + DEC + "Streaming requested from LSN LSN{0/10}, received LSN LSN{0/10} identified as already processed"]
        r = A.analyze_dbz_resume(lines)
        self.assertIsNone(r["dbz_resume"]["stored_commit_lsn"])
        self.assertEqual(r["dbz_resume"]["skipped_lsns"], [])  # quoted diff text is not evidence
        self.assertFalse(r["dbz_false_match"])

    def test_hex_high_word(self):
        lines = search("1/A0", "1/A0")
        self.assertEqual(A.analyze_dbz_resume(lines)["dbz_resume"]["stored_change_lsn"], (1 << 32) + 0xA0)


class ReportSection(unittest.TestCase):
    def test_section_counts_by_category(self):
        root = tempfile.mkdtemp()
        miss = A.analyze_dbz_resume(MISS)
        ok = A.analyze_dbz_resume(PASS)
        runs = [("ROW_MISSING", miss), ("ROW_MISSING", miss), ("PASS", ok), ("PASS", ok), ("PASS", {"dbz_logs_seen": False})]
        for i, (cat, dbz) in enumerate(runs):
            d = os.path.join(root, "run-%d" % i)
            os.makedirs(d)
            kw = dict(category=cat, target="baseline", sha="c7", java="11", idx=i, stress=True, instrumentation="none",
                      dbz_logs=True, trace_valid=False, boundary_verdict="v", run_id="r%d" % i)
            kw.update(dbz)
            with open(os.path.join(d, "result.json"), "w") as fh:
                json.dump(kw, fh)
        rows = G.summarize(G.load(root))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["dbz"], {"PASS": {"logs_seen": 2, "false_match": 0}, "ROW_MISSING": {"logs_seen": 2, "false_match": 2}})
        text = G.render(rows, 5, 5, {})
        self.assertIn("Debezium WAL resume check", text)
        self.assertIn("| baseline | 11 | none | on | 2 | 2 | 2 | 0 |", text)
        self.assertIn("| r0 | 35821280 | True | True | 312 | True |", text)
        self.assertIn("debezium logs on", text)

    def test_groups_split_by_dbz_logs(self):
        root = tempfile.mkdtemp()
        for i, flag in enumerate([True, False]):
            d = os.path.join(root, "run-%d" % i)
            os.makedirs(d)
            with open(os.path.join(d, "result.json"), "w") as fh:
                json.dump(dict(category="PASS", target="baseline", sha="c7", java="11", idx=i, dbz_logs=flag, trace_valid=True,
                               boundary_verdict="v"), fh)
        rows = G.summarize(G.load(root))
        self.assertEqual(sorted(r["dbz_logs"] for r in rows), [False, True])
        self.assertNotIn("Debezium WAL resume check", G.render([r for r in rows if not r["dbz_logs"]], 1, 1, {}))


if __name__ == "__main__":
    unittest.main()
