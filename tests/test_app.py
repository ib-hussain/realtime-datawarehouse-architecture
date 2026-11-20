import unittest

from streamlit.testing.v1 import AppTest


class StreamlitSmokeTests(unittest.TestCase):
    def test_every_registered_page_renders_without_uncaught_exception(self):
        pages = (
            "app_pages/overview.py",
            "app_pages/live_operations.py",
            "app_pages/data_preparation.py",
            "app_pages/data_explorer.py",
            "app_pages/query_studio.py",
            "app_pages/meshjoin_internals.py",
            "app_pages/system_health.py",
        )
        app = AppTest.from_file("../app.py", default_timeout=20).run()
        self.assertFalse(app.exception)

        for page in pages:
            with self.subTest(page=page):
                app.switch_page(page).run(timeout=20)
                self.assertFalse(app.exception)


if __name__ == "__main__":
    unittest.main()
