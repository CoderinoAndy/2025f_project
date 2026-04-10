import tempfile
import unittest
from pathlib import Path

from app import db


class UserSettingsTests(unittest.TestCase):
    def test_display_name_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "app.sqlite")
            db.init_db(db_path=db_path)

            self.assertIsNone(db.get_user_display_name(db_path=db_path))
            saved_name = db.set_user_display_name("  Casey   Nguyen  ", db_path=db_path)

            self.assertEqual(saved_name, "Casey Nguyen")
            self.assertEqual(db.get_user_display_name(db_path=db_path), "Casey Nguyen")

    def test_blank_display_name_clears_setting(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "app.sqlite")
            db.init_db(db_path=db_path)
            db.set_user_display_name("Casey Nguyen", db_path=db_path)

            cleared_name = db.set_user_display_name("   ", db_path=db_path)

            self.assertIsNone(cleared_name)
            self.assertIsNone(db.get_user_display_name(db_path=db_path))


if __name__ == "__main__":
    unittest.main()
