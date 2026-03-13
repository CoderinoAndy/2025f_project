import unittest
from unittest import mock

from app import ollama_client


class OllamaSummaryRegressionTests(unittest.TestCase):
    @mock.patch(
        "app.ollama_client._call_ollama",
        side_effect=AssertionError("summary fast path should not call the model"),
    )
    def test_realistic_summary_regressions_use_fast_grounded_paths(self, _mock_call):
        cases = [
            {
                "name": "wsj_article_alert_emil_michael",
                "email": {
                    "sender": '"The Wall Street Journal." <access@interactive.wsj.com>',
                    "title": "Latest in Artificial Intelligence: The Pentagon Dealmaker Who Has Become Anthropic's Nemesis",
                    "body": (
                        "Is this email difficult to read? View in browser\n\n"
                        "Latest in Artificial Intelligence\n\n"
                        "The Pentagon Dealmaker Who Has Become Anthropic's Nemesis\n\n"
                        "Emil Michael, a veteran of controversies at Uber, is the Trump administration's "
                        "point person in the fight over military use of AI.\n\nRead More"
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["Emil Michael", "military use of AI"],
            },
            {
                "name": "wsj_article_alert_bytedance",
                "email": {
                    "sender": '"The Wall Street Journal." <access@interactive.wsj.com>',
                    "title": "Latest in Artificial Intelligence: China's ByteDance Gets Access to Top Nvidia AI Chips",
                    "body": (
                        "Is this email difficult to read? View in browser\n\n"
                        "Latest in Artificial Intelligence\n\n"
                        "China's ByteDance Gets Access to Top Nvidia AI Chips\n\n"
                        "TikTok's parent company has global ambitions to compete with companies such as "
                        "Google and OpenAI by offering a range of AI applications for everyday users.\n\n"
                        "Read More"
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["TikTok's parent company", "Google and OpenAI"],
            },
            {
                "name": "hr_appreciation_update",
                "email": {
                    "sender": "Chao Yu <ycshk2000@hotmail.com>",
                    "title": "A message from HR",
                    "body": (
                        "A Message from Leadership\n"
                        "Hello everyone,\n"
                        "As we move through March, I want to take a moment to thank each of you for your "
                        "continued hard work, dedication, and collaboration.\n"
                        "This month, we've made meaningful progress across several areas of the business, "
                        "from strengthening client relationships to improving internal processes and "
                        "launching new initiatives.\n"
                        "These achievements reflect not only our goals, but also the shared values that "
                        "define who we are as an organization."
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["thanks the team", "shared values"],
            },
            {
                "name": "wsj_article_alert_grid",
                "email": {
                    "sender": '"The Wall Street Journal." <access@interactive.wsj.com>',
                    "title": "Latest in Artificial Intelligence: The Electric Grid Needs Huge Upgrades. No One Knows Who Will Pay for Them.",
                    "body": (
                        "Is this email difficult to read? View in browser\n\n"
                        "Latest in Artificial Intelligence\n\n"
                        "The Electric Grid Needs Huge Upgrades. No One Knows Who Will Pay for Them.\n\n"
                        "Utilities around the U.S. are set to spend tens of billions of dollars on "
                        "high-voltage lines, largely to meet demand from data centers.\n\nRead More"
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["Utilities around the U.S.", "data centers"],
            },
            {
                "name": "cbc_weekly_digest",
                "email": {
                    "sender": "CBC <info@newsletters.cbc.ca>",
                    "title": "Never smoked before? You could still be at risk of lung cancer, experts say",
                    "body": (
                        "CBC - This Week\n"
                        "Don't Miss This\n"
                        "Never smoked before? You could still be at risk of lung cancer, experts say\n"
                        "Occupational, environmental, genetic risks for lung cancer remain - even if "
                        "you're not a smoker.\n"
                        "Read Now\n"
                        "Mini cottage pie\n"
                        "These mini cottage pies are hearty, comforting and perfect for meal prep.\n"
                        "Read Now\n"
                        "A historic number of women are serving their communities as chief\n"
                        "164 women are leading their communities this year, says Assembly of First Nations."
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["lung cancer", "cottage pies"],
            },
            {
                "name": "bushtukah_short_promo",
                "email": {
                    "sender": "Bushtukah <orders@bushtukah.com>",
                    "title": "Fresh Gear: Trek Domane Family, Norda Runners & Kids Bikes",
                    "body": (
                        "Ride, Run & Gear Up for the Season\n"
                        "New season picks for road riders, runners, and families.\n"
                        "Fresh arrivals include Trek Domane bikes, Norda runners, and kids bikes for spring.\n"
                        "No images? Click here\n"
                        "STORE HOURS\n"
                        "Preferences | Unsubscribe"
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["Trek Domane", "kids' bikes"],
            },
            {
                "name": "scientific_american_digest",
                "email": {
                    "sender": "Scientific American <newsletters@scientificamerican.com>",
                    "title": "Today in Science: Iran was nowhere close to a nuclear bomb",
                    "body": (
                        "Plus, how to build a moon base\n"
                        "March 12, 2026 - An alcoholic exocomet, how to build a moon base and the premise "
                        "and impacts of the Iran war.\n"
                        "Iran was nowhere close to developing a nuclear bomb, despite White House claims "
                        "that the country was weeks away from it. | 2 min read\n"
                        "The war in Iran is triggering the largest supply disruption in the history of the "
                        "global oil market, the International Energy Agency says. | 4 min read"
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["moon base", "nuclear bomb"],
            },
            {
                "name": "brevite_bundle_promo",
                "email": {
                    "sender": '"Brevitē" <support@brevite.co>',
                    "title": "Ready for Anything Kit: $41 off",
                    "body": (
                        "When it starts pouring three blocks from the subway and your camera bag is on your back, what's the plan?\n"
                        "Because that's a $5,000+ kit protected by cardio and good intentions.\n"
                        "We built the Ready for Anything Kit to make that moment irrelevant. Here's what you get:\n"
                        "-> The Jumper 2026. Rebuilt from scratch after collecting feedback from 10,000+ photographers over five years.\n"
                        "-> The Camera Top Insert. Turns the top everyday compartment into dedicated carry for a point-and-shoot, DJI Osmo Pocket, or an extra lens.\n"
                        "-> The Packable Rain Cover. Full waterproof protection for the entire bag.\n"
                        "-> Free shipping.\n"
                        "-> Lifetime warranty.\n"
                        "All for $41 off. Limited time bundle.\n"
                        "You received this email from Brevitē. If you would like to unsubscribe, click here."
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["Jumper 2026 bag", "$41 off"],
            },
            {
                "name": "uber_eats_membership_promo",
                "email": {
                    "sender": "Uber Eats <uber@uber.com>",
                    "title": "Enjoy up to 50% off your next 3 orders!",
                    "body": (
                        "Sign up for an Uber One membership to start saving!\n"
                        "Enjoy $0 Delivery Fee on eligible orders, 5% Uber One credits on eligible rides, and more.\n"
                        "Get 4 weeks free\n"
                        "Get 50% off your next 3 orders of $1 or more\n"
                        "Terms and conditions apply."
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["4-week free trial", "50% off"],
            },
            {
                "name": "wsj_whats_news_digest",
                "email": {
                    "sender": '"WSJ What\'s News" <access@interactive.wsj.com>',
                    "title": "The Growing Economic Risks of the Iran War",
                    "body": (
                        "Plus, violence rocks a Virginia university and a Detroit-area synagogue, and a new "
                        "kind of betting pool is taking over Oscar parties\n"
                        "This is an edition of the What's News newsletter.\n"
                        "1. The economic risks of the war in Iran are getting real on Wall Street.\n"
                        "U.S. stock indexes tumbled today, after it became clear to investors that Iran was "
                        "willing to inflict and suffer economic pain as the conflict drags on.\n"
                        "2. Treasury Secretary Scott Bessent said the U.S. is planning possible military "
                        "escorts for commercial vessels through the Strait of Hormuz."
                    ),
                    "recipients": "",
                    "cc": "",
                },
                "expected_terms": ["Virginia university", "Scott Bessent"],
            },
        ]

        for case in cases:
            with self.subTest(case=case["name"]):
                summary = ollama_client.summarize_email(case["email"], email_id=case["name"])
                self.assertIsNotNone(summary)
                self.assertFalse(
                    ollama_client.summary_looks_unusable({**case["email"], "summary": summary})
                )
                self.assertFalse(
                    ollama_client._is_near_subject_copy(summary, case["email"]["title"])
                )
                for term in case["expected_terms"]:
                    self.assertIn(term, summary)


if __name__ == "__main__":
    unittest.main()
