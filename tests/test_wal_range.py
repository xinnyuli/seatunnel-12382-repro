"""Boundary-hit + id=15 WAL range checks (the three values asked for on #12382), and the before/after shape for
PR #12454 (which replays the transaction at the stored commit-end LSN instead of skipping it)."""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import analyze_run as A
from test_dbz_resume import LOC, DEC, SRC, search, MISS, PASS

IT = "[] 2026-10-05 01:00:00,000 INFO  org.apache.seatunnel.connectors.seatunnel.cdc.postgres.PostgresCDCIT - "


def wal(before, after):
    return IT + ("[PG12382] ID15_WAL job=777 insertBefore=%s writeBefore=%s insertAfter=%s writeAfter=%s"
                 % (before, before, after, after))


# PR #12454 shape at the same boundary: first LSN == stored, resume AT stored, nothing skipped, filter off at stored.
FIXED = search("0/22296E0", "0/22296E0") + [
    LOC + "First LSN 'LSN{0/22296E0}' received",
    SRC + "WAL resume position 'LSN{0/22296E0}' discovered",
    LOC + "Message with LSN 'LSN{0/22296E0}' arrived, switching off the filtering",
    SRC + "Processing messages",
]


def run(lines):
    d = A.analyze_dbz_resume(lines)
    d.update(A.analyze_wal_range(lines, d))
    return d


class BoundaryHit(unittest.TestCase):
    def test_unfixed_collision_is_hit_and_false_match(self):
        d = run(MISS)
        self.assertTrue(d["dbz_boundary_hit"])
        self.assertTrue(d["dbz_false_match"])

    def test_fixed_collision_is_hit_but_not_false_match(self):
        d = run(FIXED)
        self.assertTrue(d["dbz_boundary_hit"])
        self.assertFalse(d["dbz_false_match"])
        self.assertEqual(d["dbz_resume"]["resume_minus_stored"], 0)

    def test_normal_restart_is_not_hit(self):
        d = run(PASS)
        self.assertFalse(d["dbz_boundary_hit"])
        self.assertFalse(d["dbz_false_match"])

    def test_no_dbz_logs(self):
        d = run([IT + "something else"])
        self.assertIsNone(d["dbz_boundary_hit"])
        self.assertIsNone(d["id15_wal"])


class WalRange(unittest.TestCase):
    def test_id15_starts_at_stored_and_contains_skipped(self):
        d = run(MISS + [wal("0/22296E0", "0/2229818")])
        e = d["wal_equality"]
        self.assertTrue(e["id15_starts_at_stored"])
        self.assertTrue(e["id15_range_contains_skipped"])
        self.assertEqual(e["skipped_lsn"], e["stored_change_lsn"])
        self.assertEqual(e["id15_tx_bytes"], 312)

    def test_other_wal_in_between(self):
        d = run(PASS + [wal("0/22290D0", "0/2229208")])
        e = d["wal_equality"]
        self.assertFalse(e["id15_starts_at_stored"])
        self.assertFalse(e["id15_range_contains_skipped"])
        self.assertIsNone(e["skipped_lsn"])

    def test_range_without_dbz_logs(self):
        d = run([wal("0/1000", "0/1138")])
        self.assertEqual(d["id15_wal"]["insert_after"] - d["id15_wal"]["insert_before"], 312)
        self.assertIsNone(d["wal_equality"])

    def test_quoted_diff_lines_ignored(self):
        d = run(["+" + wal("0/1000", "0/1138")])
        self.assertIsNone(d["id15_wal"])


if __name__ == "__main__":
    unittest.main()
