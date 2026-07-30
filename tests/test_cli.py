import unittest
from unittest.mock import patch

from copetech_sec.cli import run_service


class ServiceCliTests(unittest.TestCase):
    def test_missing_service_dependency_has_actionable_message(self):
        error = ModuleNotFoundError("No module named 'fastapi'")
        error.name = "fastapi"
        with patch("copetech_sec.cli.import_module", side_effect=error):
            with self.assertRaisesRegex(
                SystemExit,
                r"copetech-edgar\[service\]",
            ):
                run_service()
