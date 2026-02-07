"""
Browser management for Playwright-based crawling.

This module provides a wrapper around Playwright for managing
browser instances with proper configuration and cleanup.
"""

from typing import Optional, Dict, Any
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Playwright
import logging

logger = logging.getLogger(__name__)


class BrowserManager:
    """
    Manages Playwright browser instances.

    Provides context manager interface for clean browser lifecycle management.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize browser manager.

        Args:
            config: Configuration dictionary with browser settings
        """
        self.config = config.get('browser', {})
        self.headless = self.config.get('headless', True)
        self.timeout = self.config.get('timeout', 30000)
        self.viewport = self.config.get('viewport', {'width': 1920, 'height': 1080})
        self.user_agent = self.config.get('user_agent')

        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    def __enter__(self):
        """Enter context manager."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close()
        return False

    def start(self) -> Page:
        """
        Start browser and create a new page.

        Returns:
            Playwright Page object
        """
        try:
            logger.info("Starting browser...")

            # Start Playwright
            self.playwright = sync_playwright().start()

            # Launch browser
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled'
                ]
            )

            # Create context with custom settings
            context_options = {
                'viewport': self.viewport,
                'locale': 'ko-KR',
                'timezone_id': 'Asia/Seoul'
            }

            if self.user_agent:
                context_options['user_agent'] = self.user_agent

            self.context = self.browser.new_context(**context_options)

            # Set default timeout
            self.context.set_default_timeout(self.timeout)

            # Create page
            self.page = self.context.new_page()

            logger.info("Browser started successfully")
            return self.page

        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            self.close()
            raise

    def close(self) -> None:
        """Close browser and cleanup resources."""
        try:
            if self.page:
                self.page.close()
                self.page = None

            if self.context:
                self.context.close()
                self.context = None

            if self.browser:
                self.browser.close()
                self.browser = None

            if self.playwright:
                self.playwright.stop()
                self.playwright = None

            logger.info("Browser closed successfully")

        except Exception as e:
            logger.error(f"Error closing browser: {e}")

    def new_page(self) -> Page:
        """
        Create a new page in the current context.

        Returns:
            Playwright Page object
        """
        if not self.context:
            raise RuntimeError("Browser context not initialized")

        page = self.context.new_page()
        return page

    def get_page(self) -> Page:
        """
        Get the current page or create a new one.

        Returns:
            Playwright Page object
        """
        if not self.page:
            if self.context:
                self.page = self.new_page()
            else:
                self.page = self.start()

        return self.page

    def restart(self) -> Page:
        """
        Restart browser (close and start again).

        Returns:
            New Playwright Page object
        """
        logger.info("Restarting browser...")
        self.close()
        return self.start()

    def take_screenshot(self, path: str, full_page: bool = True) -> None:
        """
        Take a screenshot of the current page.

        Args:
            path: Path to save screenshot
            full_page: Whether to capture full page
        """
        if not self.page:
            raise RuntimeError("No page available")

        try:
            self.page.screenshot(path=path, full_page=full_page)
            logger.info(f"Screenshot saved to: {path}")
        except Exception as e:
            logger.error(f"Failed to take screenshot: {e}")

    def set_extra_http_headers(self, headers: Dict[str, str]) -> None:
        """
        Set extra HTTP headers for all requests.

        Args:
            headers: Dictionary of header name-value pairs
        """
        if not self.context:
            raise RuntimeError("Browser context not initialized")

        self.context.set_extra_http_headers(headers)
        logger.debug(f"Set extra headers: {headers}")

    def clear_cookies(self) -> None:
        """Clear all cookies."""
        if not self.context:
            raise RuntimeError("Browser context not initialized")

        self.context.clear_cookies()
        logger.debug("Cleared all cookies")

    def get_cookies(self) -> list:
        """
        Get all cookies.

        Returns:
            List of cookie dictionaries
        """
        if not self.context:
            raise RuntimeError("Browser context not initialized")

        return self.context.cookies()

    def is_alive(self) -> bool:
        """
        Check if browser is still alive and responsive.

        Returns:
            True if browser is alive, False otherwise
        """
        try:
            if not self.browser or not self.browser.is_connected():
                return False

            if self.page:
                # Try to evaluate a simple expression
                self.page.evaluate('1 + 1')

            return True

        except Exception:
            return False
