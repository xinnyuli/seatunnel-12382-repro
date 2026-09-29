import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import plan_matrix as P

DEV = "4c874e2a4061aea9d5db65e74edeb211b498fe27"


class Plan(unittest.TestCase):
    def test_default(self):
        p = P.plan("baseline,dev", 10, "11", DEV, True, "x")
        self.assertEqual(p["expected"], 20)
        shas = {(i["target"], i["sha"]) for i in p["matrix"]["include"]}
        self.assertEqual(shas, {("baseline", P.BASELINE_SHA), ("dev", DEV)})
        self.assertEqual(p["matrix"]["include"][0]["idx"], 1)

    def test_java_matrix(self):
        self.assertEqual(P.plan("baseline", 3, "8,11", "", None, "")["expected"], 6)

    def test_rejects(self):
        for args in [("baseline,dev", 1, "11", "abc", True, ""), ("baseline", 0, "11", "", None, ""),
                     ("baseline", 1, "17", "", None, ""), ("nope", 1, "11", "", None, ""),
                     ("baseline,dev", 200, "8,11", DEV, True, "")]:
            with self.assertRaises(SystemExit):
                P.plan(*args)


if __name__ == "__main__":
    unittest.main()
