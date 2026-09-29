import os, re, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import analyze_run as A

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def load(name):
    with open(os.path.join(FIX, name), errors="replace") as fh:
        return fh.readlines()


def drop(lines, pattern):
    rx = re.compile(pattern)
    return [l for l in lines if not rx.search(l)]


LOSS_TAIL = [
    "[ERROR] Tests run: 1, Failures: 0, Errors: 1, Skipped: 0, Time elapsed: 259 s <<< FAILURE! - in x.PostgresCDCIT\n",
    "[ERROR]   PostgresCDCIT.testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes:625 ? ConditionTimeout expected: <1> but was: <0> within 3 minutes.\n",
    "[ERROR] Tests run: 1, Failures: 0, Errors: 1, Skipped: 0\n",
]


def as_loss(lines, drop_pattern):
    """Synthetic loss derived from the REAL passing log: remove the tail of the id=15 path,
    make every sink query return 0 and append the failure lines (SYNTHETIC, for logic tests)."""
    out = drop(lines, drop_pattern)
    out = drop(out, r"Tests run:|BUILD ")
    out = [l.replace("id=15 count=1", "id=15 count=0") for l in out]
    return out + LOSS_TAIL


class RealLogs(unittest.TestCase):
    def test_real_pass(self):
        r = A.analyze(load("pass_real.txt"), 0)
        self.assertEqual(r["category"], "PASS")
        self.assertTrue(r["trace_valid"])
        self.assertEqual(r["event_lsn"], 35617928)
        self.assertTrue(r["committed_past_row"])   # delivered AND committed past it: fine
        self.assertIsNone(r["invariant_violation"])
        self.assertEqual(r["stages_reached"][-1], "SINK_QUERY_count1")
        self.assertIsNone(r["first_missing_stage"])

    def test_real_setup_timeout_is_not_row_loss(self):
        r = A.analyze(load("setup_timeout_real.txt"), 1)
        self.assertEqual(r["category"], "SETUP_TIMEOUT")
        self.assertFalse(r["reached_check"])
        self.assertFalse(r["restored_slot_active"])

    def test_pass_requires_exit_zero(self):
        r = A.analyze(load("pass_real.txt"), 1)
        self.assertNotEqual(r["category"], "PASS")

    def test_pass_requires_sink_evidence(self):
        lines = [l.replace("id=15 count=1", "id=15 count=0") for l in load("pass_real.txt")]
        r = A.analyze(lines, 0)
        self.assertNotEqual(r["category"], "PASS")


class SyntheticLoss(unittest.TestCase):
    base = load("pass_real.txt")

    def test_lost_between_reader_and_sink(self):
        lines = as_loss(self.base, r"stage=(write_|addToBatch_|attemptFlush_)\w+ id=15")
        r = A.analyze(lines, 1)
        self.assertEqual(r["category"], "ROW_MISSING")
        self.assertEqual(r["first_missing_stage"], "write_ENTER")
        self.assertIn("collect_RETURN", r["boundary_verdict"])
        self.assertIn("B3", r["boundary_verdict"])
        # committed LSN advanced past the record that was never delivered:
        self.assertTrue(r["invariant_violation"])

    def test_lost_before_reader_emission(self):
        lines = as_loss(self.base, r"stage=(processElement_|collect_|write_|addToBatch_|attemptFlush_)\w+ id=15")
        r = A.analyze(lines, 1)
        self.assertEqual(r["category"], "ROW_MISSING")
        self.assertEqual(r["first_missing_stage"], "processElement_RETURN")
        self.assertIn("shouldEmit_RETURN", r["boundary_verdict"])

    def test_never_seen_at_fetcher(self):
        lines = as_loss(self.base, r"\[PG12382\] run=\S+ job=\S+ seq=\d+ .*stage=\w+ id=15")
        r = A.analyze(lines, 1)
        self.assertEqual(r["category"], "ROW_MISSING")
        self.assertIn("before B1", r["boundary_verdict"])
        self.assertIsNone(r["event_lsn"])
        self.assertIsNone(r["invariant_violation"])  # cannot be evaluated without the record LSN

    def test_rejected_by_shouldemit(self):
        lines = as_loss(self.base, r"stage=(processElement_|collect_|write_|addToBatch_|attemptFlush_)\w+ id=15")
        lines = [l.replace("accepted=true", "accepted=false") for l in lines]
        r = A.analyze(lines, 1)
        self.assertEqual(r["category"], "ROW_MISSING")
        self.assertIn("shouldEmit rejected", r["boundary_verdict"])

    def test_invalid_trace_never_attributes_boundary(self):
        lines = as_loss(self.base, r"stage=(write_|addToBatch_|attemptFlush_)\w+ id=15")
        lines = drop(lines, r"AGENT_READY")
        r = A.analyze(lines, 1)
        self.assertEqual(r["category"], "ROW_MISSING")
        self.assertFalse(r["trace_valid"])
        self.assertIn("trace incomplete", r["boundary_verdict"])

    def test_id150_is_not_id15(self):
        lines = as_loss(self.base, r"stage=(write_|addToBatch_|attemptFlush_)\w+ id=15")
        lines = [l.replace("stage=collect_RETURN id=15", "stage=collect_RETURN id=150") for l in lines]
        r = A.analyze(lines, 1)
        self.assertNotIn("collect_RETURN", r["stages_reached"])


class OtherOutcomes(unittest.TestCase):
    base = load("pass_real.txt")

    def test_fail_after_insert_other_reason(self):
        lines = drop(self.base, r"Tests run:|BUILD ")
        lines = [l.replace("id=15 count=1", "id=15 count=0") for l in lines]
        lines += ["[ERROR] Tests run: 1, Failures: 1, Errors: 0, Skipped: 0\n",
                  "[ERROR]   PostgresCDCIT.test...:610 AssertionError job failed with async error\n"]
        r = A.analyze(lines, 1)
        self.assertEqual(r["category"], "FAIL_AFTER_INSERT")

    def test_timeout_exit(self):
        self.assertEqual(A.analyze(self.base[:20], 124)["category"], "TIMEOUT")

    def test_no_test_ran_exit_zero(self):
        r = A.analyze(["[INFO] BUILD SUCCESS\n"], 0)
        self.assertEqual(r["category"], "NO_TEST_RUN")

    def test_empty_log(self):
        self.assertEqual(A.analyze([], 1)["category"], "NO_TEST_RUN")

    def test_diff_text_is_not_evidence(self):
        quoted = ['+                    "[PG12382] SINK_QUERY job={} table={}.{} id=15 count={}",\n',
                  '+ [PG12382] RESTORED_SLOT_ACTIVE job=1 slot=x id=15\n']
        r = A.analyze(quoted, 1)
        self.assertEqual(r["category"], "NO_TEST_RUN")


if __name__ == "__main__":
    unittest.main()


class Encoding(unittest.TestCase):
    def test_utf16_powershell_log(self):
        import tempfile
        text = "".join(load("pass_real.txt"))
        with tempfile.NamedTemporaryFile("wb", suffix=".log", delete=False) as fh:
            fh.write(text.encode("utf-16"))
        self.assertEqual(A.analyze(A.read_lines(fh.name), 0)["category"], "PASS")
