import os
import re
from abc import ABC

from playwright.sync_api import sync_playwright, expect

from test.test_utils import get_secret

DOMAIN_NAME = os.environ["DOMAIN_NAME"]
BASE_URL = f"https://{DOMAIN_NAME}"
BROWSER_MODE = os.getenv("BROWSER_MODE", "HEADLESS")
TEST_CSV_PATH = "./test/e2e/test_journey_file.csv"
FILENAME = "test_journey_file.csv"


class BaseTestUI(ABC):
    username = None
    password = None
    subject_id = None

    def set_up_base_page(self, playwright):
        test_user_credentials = get_secret(secret_name="UI_TEST_USER")
        self.username = test_user_credentials["username"]
        self.password = test_user_credentials["password"]
        self.subject_id = test_user_credentials["subject_id"]

        browser = playwright.chromium.launch(
            headless=BROWSER_MODE == "HEADLESS", slow_mo=1000
        )
        context = browser.new_context()
        return context.new_page()

    def login_with_cognito(self, page):
        print("Logging test user in with Cognito")

        page.locator(
            "//div[contains(@class, 'visible-lg')]//input[@id='signInFormUsername']"
        ).fill(self.username)
        page.locator(
            "//div[contains(@class, 'visible-lg')]//input[@id='signInFormPassword']"
        ).fill(self.password)
        page.locator(
            "//div[contains(@class, 'visible-lg')]//input[@name='signInSubmitButton']"
        ).click()

    def logout(self, page):
        print("Logging out")
        self.click_button(page, "Log out")

    def login(self, page):
        self.go_to(page, "/login")
        self.assert_title(page, "rAPId - Login")
        self.click_link(page, "Log in to rAPId")
        self.login_with_cognito(page)

    def go_to(self, page, path: str):
        print(f"Going directly to page: {path}")
        page.goto(f"{BASE_URL}{path}")

    def click_link(self, page, link_text: str):
        print(f"Clicking on link: {link_text}")
        page.click(f"//a[text()='{link_text}']")

    def click_button(self, page, button_text: str):
        print(f"Clicking button: {button_text}")
        page.click(f"//button[text()='{button_text}']")

    def click_label(self, page, label_text: str):
        print(f"Clicking label: {label_text}")
        page.click(f"//label[text()='{label_text}']")

    def click_by_id(self, page, element_id: str):
        print(f"Clicking element with ID: {element_id}")
        page.click(f"#{element_id}")

    def input_text_value(self, page, input_id: str, value: str):
        print(f"Typing {value} into input with ID {input_id}")
        page.locator(f"//input[@id='{input_id}']").fill(value)

    def choose_and_upload_file(self, page):
        with page.expect_file_chooser() as fc_info:
            self.click_label(page, "Choose file")
        file_chooser = fc_info.value
        file_chooser.set_files(TEST_CSV_PATH)

    ## Assertions -------------------------------
    def assert_title(self, page, title: str):
        print(f"Checking page title: {title}")
        expect(page).to_have_title(re.compile(title))

    def assert_contains_label(self, page, label_text: str):
        print(f"Checking that '{label_text}' exists")
        locator = page.locator(f"//label[text()='{label_text}']")
        expect(locator).to_contain_text(label_text)

    def assert_not_contains_label(self, page, label_text: str):
        print(f"Checking that '{label_text}' does not exist")
        locator = page.locator(f"//label[text()='{label_text}']")
        expect(locator).to_be_disabled()
        expect(locator).not_to_be_visible()

    def assert_dropdown_exists(self, page, dropdown_id: str):
        print("Checking dropdown exists in the page")
        expect(page.locator(f"#{dropdown_id}")).to_have_count(1)

    def assert_dataset_exists(self, page, dropdown_id: str, dataset: str):
        print(f"Checking dataset {dataset} exists in the dropdown")
        options = page.locator(f"#{dropdown_id} option")
        option_text = options.all_text_contents()
        assert dataset in option_text

    def assert_can_upload(self, page, dropdown_id, upload_dataset):
        print(f"Trying uploading to '{upload_dataset}'")
        self.select_from_dropdown(page, dropdown_id, upload_dataset)
        self.choose_and_upload_file(page)
        self.assert_contains_label(page, FILENAME)
        self.click_button(page, "Upload dataset")
        self.assert_contains_label(page, "File uploaded: ui_test.csv")

    def select_from_dropdown(self, page, dropdown_id, value_to_select):
        print(f"Selecting '{value_to_select}' from '{dropdown_id}'")
        selected = page.select_option(f"select#{dropdown_id}", value_to_select)
        assert selected == [value_to_select]

    def assert_on_cognito_login(self, page):
        print("Checking that we are on the Cognito login page")
        assert "amazoncognito.com/login" in page.url

    def assert_text_on_page(self, page, text: str):
        assert page.locator(f"text={text}")


class TestUI(BaseTestUI):
    def test_log_in_and_log_out_sequence(self):
        with sync_playwright() as playwright:
            page = self.set_up_base_page(playwright)

            # Should be redirected to log in when not authenticated
            self.go_to(page, "/")

            self.assert_title(page, "rAPId - Login")
            self.click_link(page, "Log in to rAPId")

            self.login_with_cognito(page)

            self.assert_title(page, "rAPId")

            # Should log out of Cognito session too and not redirect to /upload when logging in again
            self.logout(page)
            self.click_link(page, "Log in to rAPId")
            self.assert_on_cognito_login(page)

    def test_upload_journey(self):
        with sync_playwright() as playwright:
            page = self.set_up_base_page(playwright)

            write_all_datasets = [
                "ui_test/upload",
                "ui_test/upload_private",
                "test_e2e_protected/do_not_delete",
            ]
            upload_dataset = "ui_test/upload"
            dropdown_id = "dataset"

            self.login(page)

            self.click_link(page, "Upload Data")
            self.assert_title(page, "rAPId - Upload")

            self.assert_dropdown_exists(page, dropdown_id)
            self.assert_dataset_exists(page, dropdown_id, write_all_datasets[0])
            self.assert_dataset_exists(page, dropdown_id, write_all_datasets[1])
            self.assert_dataset_exists(page, dropdown_id, write_all_datasets[2])

            self.assert_can_upload(page, dropdown_id, upload_dataset)

            self.logout(page)

    def test_modify_subject_journey(self):
        with sync_playwright() as playwright:
            page = self.set_up_base_page(playwright)

            self.login(page)

            self.click_link(page, "Modify User")
            self.assert_title(page, "rAPId - Select Subject")
            self.assert_text_on_page(page, "Step 1 of 2")

            self.input_text_value(page, "subject-id-input", self.subject_id)

            self.click_button(page, "Next")

            self.assert_title(page, "rAPId - Modify Subject")
            self.assert_text_on_page(page, "Step 2 of 2")
            self.assert_text_on_page(page, f"Permissions for {self.subject_id}")

            self.logout(page)

    def test_create_subject_journey(self):
        with sync_playwright() as playwright:
            page = self.set_up_base_page(playwright)

            self.login(page)

            self.click_link(page, "Create User")
            self.assert_title(page, "rAPId - Create Subject")
            self.assert_contains_label(page, "Email")
            self.select_from_dropdown(page, "select_subject", "CLIENT")
            self.assert_not_contains_label(page, "Email")
            self.select_from_dropdown(page, "select_subject", "USER")
            self.assert_contains_label(page, "Email")

            self.input_text_value(page, "name", "my_name")
            self.input_text_value(page, "email", "my_email@email.com")

            self.click_by_id(page, "USER_ADMIN")
            self.click_by_id(page, "READ_ALL")
            self.click_by_id(page, "READ_PUBLIC")
            self.click_by_id(page, "WRITE_ALL")
            self.click_by_id(page, "WRITE_PUBLIC")
            self.click_by_id(page, "READ_PROTECTED_TEST")
            self.click_by_id(page, "WRITE_PROTECTED_TEST")

            self.click_button(page, "Create subject")

            self.logout(page)
