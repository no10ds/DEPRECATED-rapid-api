import os
import re

from playwright.sync_api import Playwright, sync_playwright, expect

DOMAIN_NAME = os.environ["DOMAIN_NAME"]
BASE_URL = f"https://{DOMAIN_NAME}"
BROWSER_MODE = os.getenv("BROWSER_MODE", "HEADLESS")


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(
        headless=BROWSER_MODE == "HEADLESS", slow_mo=1000
    )
    context = browser.new_context()
    print("--------- UI Tests ---------")
    page = context.new_page()
    go_to(page, "/upload")
    # Should be redirected to log in when not authenticated
    assert_title(page, "rAPId - Login")
    click_link(page, "Log in to rAPId")


def go_to(page, path: str):
    print(f"Going directly to page: {path}")
    page.goto(f"{BASE_URL}{path}")


def click_link(page, link_text: str):
    print(f"Clicking on link: {link_text}")
    page.click(f"//a[text()='{link_text}']")


def assert_title(page, title: str):
    print(f"Checking page title: {title}")
    expect(page).to_have_title(re.compile(title))


with sync_playwright() as playwright:
    run(playwright)
