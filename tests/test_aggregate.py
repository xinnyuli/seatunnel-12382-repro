import json, os, sys, tempfile, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import aggregate as G


def write(root, name, **kw):
    d = os.path.join(root, name)
    os.makedirs(d)
    with open(os.path.join(d, "result.json"), "w") as fh:
        json.dump(kw, fh)


class Stats(unittest.TestCase):
    def test_rule_of_three(self):
        # 0 failures in 30 runs -> ~9.5% exact, "3/n" approximation is 10%
        ub = G.upper_bound(0, 30)
        self.assertAlmostEqual(ub, 1 - 0.05 ** (1 / 30), places=6)
        self.assertAlmostEqual(ub, 0.0950, places=3)

    def test_known_clopper_pearson(self):
        self.assertAlmostEqual(G.upper_bound(1, 10, 0.025), 0.4450, places=3)
        self.assertAlmostEqual(G.lower_bound(1, 10, 0.025), 0.0025, places=3)
        self.assertEqual(G.upper_bound(10, 10), 1.0)
        self.assertIsNone(G.upper_bound(0, 0))


class Report(unittest.TestCase):
    def make(self, cats, target="baseline", sha="c7304ace6e18d350314e92480df1fd3c0962f1f2", java="11", extra=None):
        root = tempfile.mkdtemp()
        for i, c in enumerate(cats):
            kw = dict(category=c, target=target, sha=sha, java=java, idx=i, trace_valid=True, run_id="r%d" % i)
            if c == "ROW_MISSING":
                kw.update(boundary_verdict="last observed: collect_RETURN", event_lsn=5, max_committed_lsn=9, committed_past_row=True)
            write(root, "run-%s-%d" % (target, i), **kw)
        return root

    def test_not_reproduced_wording_and_exclusion_of_setup_failures(self):
        root = self.make(["PASS"] * 8 + ["SETUP_TIMEOUT", "NO_TEST_RUN"])
        rows = G.summarize(G.load(root))
        self.assertEqual(rows[0]["reached_check"], 8)       # setup failures not counted as passes
        md = G.render(rows, 10, 10, {})
        self.assertIn("**not reproduced**", md)
        self.assertIn("8/8", md)
        self.assertIn("No missing-row run was captured", md)

    def test_reproduced_lists_boundary(self):
        root = self.make(["PASS"] * 9 + ["ROW_MISSING"])
        md = G.render(G.summarize(G.load(root)), 10, 10, {})
        self.assertIn("**reproduced**", md)
        self.assertIn("1/10", md)
        self.assertIn("collect_RETURN", md)

    def test_missing_runs_warning(self):
        root = self.make(["PASS"] * 3)
        md = G.render(G.summarize(G.load(root)), 5, 3, {})
        self.assertIn("2 of 5 expected runs", md)

    def test_groups_are_separate(self):
        root = self.make(["PASS"] * 2)
        os.makedirs(os.path.join(root, "x"))
        write(root, "dev-0", category="ROW_MISSING", target="dev", sha="4c874e2a", java="11", idx=0, trace_valid=True, boundary_verdict="v")
        rows = G.summarize(G.load(root))
        self.assertEqual({(r["target"]): r["row_missing"] for r in rows}, {"baseline": 0, "dev": 1})


if __name__ == "__main__":
    unittest.main()


class StressGroups(unittest.TestCase):
    def test_stress_is_a_separate_group(self):
        root = tempfile.mkdtemp()
        for i, s in enumerate([False, False, True, True]):
            write(root, "r%d" % i, category="PASS" if i < 3 else "ROW_MISSING", target="baseline", sha="c7", java="11",
                  idx=i, stress=s, trace_valid=True, boundary_verdict="v")
        rows = G.summarize(G.load(root))
        self.assertEqual({(r["stress"]): (r["runs"], r["row_missing"]) for r in rows}, {False: (2, 0), True: (2, 1)})
        self.assertIn("stress-ng on", G.render(rows, 4, 4, {}))
