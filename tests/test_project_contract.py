import json
import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class ProjectContractTests(unittest.TestCase):
    def test_bundle_references_existing_files(self):
        bundle = (ROOT / "databricks.yml").read_text(encoding="utf-8")
        self.assertIn("transformations/**", bundle)
        self.assertTrue((ROOT / "dashboards" / "nyc_taxi_analytics.lvdash.json").exists())
        self.assertTrue((ROOT / "maintenance" / "table_maintenance.py").exists())

    def test_dashboard_json_is_valid(self):
        dashboard = ROOT / "dashboards" / "nyc_taxi_analytics.lvdash.json"
        json.loads(dashboard.read_text(encoding="utf-8"))

    def test_no_personal_workspace_or_email_values(self):
        forbidden = [
            "arkadu" + ".rajesh",
            "accenture" + ".com",
            "adb-" + "7405612433234509",
            "nyctaxi" + "storagemedalian",
        ]
        searchable = []
        for path in ROOT.rglob("*"):
            if path.is_file() and ".git" not in path.parts and path.suffix.lower() in {".md", ".py", ".yml", ".sql", ".json"}:
                searchable.append((path, path.read_text(encoding="utf-8", errors="ignore")))

        for path, text in searchable:
            for token in forbidden:
                self.assertNotIn(token, text, f"{token} found in {path.relative_to(ROOT)}")

    def test_readme_has_no_merge_markers_or_mojibake(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertNotRegex(readme, re.compile(r"^(<<<<<<<|=======|>>>>>>>)", re.MULTILINE))
        self.assertNotIn("\u00f0", readme)
        self.assertNotIn("\u00e2", readme)


if __name__ == "__main__":
    unittest.main()
