import os
import re
from abc import ABC

from playwright.sync_api import sync_playwright, expect

DOMAIN_NAME = os.environ["DOMAIN_NAME"]
BASE_URL = f"https://{DOMAIN_NAME}"
BROWSER_MODE = os.getenv("BROWSER_MODE", "HEADLESS")


class BaseTestUI(ABC):
    def set_up_base_page(self, playwright):
        browser = playwright.chromium.launch(
            headless=BROWSER_MODE == "HEADLESS", slow_mo=1000
        )
        context = browser.new_context()
        return context.new_page()

    def go_to(self, page, path: str):
        print(f"Going directly to page: {path}")
        page.goto(f"{BASE_URL}{path}")

    def click_link(self, page, link_text: str):
        print(f"Clicking on link: {link_text}")
        page.click(f"//a[text()='{link_text}']")

    def assert_title(self, page, title: str):
        print(f"Checking page title: {title}")
        expect(page).to_have_title(re.compile(title))


class TestUI(BaseTestUI):
    def test_ui(self):
        with sync_playwright() as playwright:
            page = self.set_up_base_page(playwright)
            print("--------- UI Tests ---------")
            self.go_to(page, "/upload")
            # Should be redirected to log in when not authenticated
            self.assert_title(page, "rAPId - Login")
            self.click_link(page, "Log in to rAPId")
