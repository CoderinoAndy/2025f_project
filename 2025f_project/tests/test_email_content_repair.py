import unittest

from app.email_content import repair_body_text, repair_header_text


class EmailContentRepairTests(unittest.TestCase):
    def test_repair_body_text_fixes_common_mojibake(self):
        repaired = repair_body_text("McDonaldâ€™s and EY say donâ€™t miss it.")

        self.assertEqual(repaired, "McDonald’s and EY say don’t miss it.")

    def test_repair_header_text_fixes_common_mojibake(self):
        repaired = repair_header_text("donâ€™t miss todayâ€™s briefing")

        self.assertEqual(repaired, "don’t miss today’s briefing")


if __name__ == "__main__":
    unittest.main()
