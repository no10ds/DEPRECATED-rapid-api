import os
import re
from abc import ABC

from playwright.sync_api import sync_playwright, expect

from test.test_utils import get_secret

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

    def click_button(self, page, button_text: str):
        print(f"Clicking button: {button_text}")
        page.click(f"//button[text()='{button_text}']")

    def assert_title(self, page, title: str):
        print(f"Checking page title: {title}")
        expect(page).to_have_title(re.compile(title))

    def input_text_value(self, page, input_id: str, value: str):
        print(f"Typing {value} into input with ID {input_id}")
        page.locator(f"//input[@id='{input_id}']").fill(value)

    def login_with_cognito(self, page):
        print("Logging test user in with Cognito")

        test_user_credentials = get_secret(secret_name="UI_TEST_USER")
        username = test_user_credentials["username"]
        password = test_user_credentials["password"]

        page.locator(
            "//div[contains(@class, 'visible-lg')]//input[@id='signInFormUsername']"
        ).fill(username)
        page.locator(
            "//div[contains(@class, 'visible-lg')]//input[@id='signInFormPassword']"
        ).fill(password)
        page.locator(
            "//div[contains(@class, 'visible-lg')]//input[@name='signInSubmitButton']"
        ).click()

    def logout(self, page):
        print("Logging out")
        self.click_button(page, "Log out")

    def assert_on_cognito_login(self, page):
        print("Checking that we are on the Cognito login page")
        assert "amazoncognito.com/login" in page.url


class TestUI(BaseTestUI):
    def test_log_in_and_log_out_sequence(self):
        with sync_playwright() as playwright:
            page = self.set_up_base_page(playwright)

            # Should be redirected to log in when not authenticated
            self.go_to(page, "/upload")

            self.assert_title(page, "rAPId - Login")
            self.click_link(page, "Log in to rAPId")

            self.login_with_cognito(page)

            self.assert_title(page, "rAPId - Upload")

            # Should log out of Cognito session too and not redirect to /upload when logging in again
            self.logout(page)
            self.click_link(page, "Log in to rAPId")
            self.assert_on_cognito_login(page)
