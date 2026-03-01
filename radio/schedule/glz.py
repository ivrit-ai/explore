"""Schedule scraper for GLZ stations (Galatz, Galgalatz).

Uses playwright to bypass Incapsula protection, then fetches schedule
data from the Umbraco timetable API embedded in glz.co.il.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, time
from typing import Optional

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

GLZ_BASE = "https://www.glz.co.il"
# rootId mapping: Galatz=1051, Galgalatz=1920
GLZ_TIMETABLE_API = "/umbraco/api/timetable/getTimetable?rootId={root_id}&slideindex=0"
# Schedule page paths (used to trigger Incapsula solve)
GLZ_SCHEDULE_PAGES = {
    "1051": "/גלצ/לוח-שידורים",
    "1920": "/גלגלצ/לוח-שידורים",
}


class GlzScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from glz.co.il via playwright."""

    def __init__(self, root_id: str = "1920"):
        self.root_id = root_id

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch GLZ schedule for %s (root %s)", day, self.root_id)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        data = self._fetch_via_playwright()
        if not data:
            return []

        day_str = day.strftime("%d.%m.%y")
        timetable = data.get("glzTimeTable", [])

        for day_entry in timetable:
            if day_entry.get("day") == day_str:
                return self._parse_day(day_entry)

        # If exact date not found, try today's entry
        for day_entry in timetable:
            if day_entry.get("isToday"):
                return self._parse_day(day_entry)

        log.warning("Day %s not found in GLZ timetable (root %s)", day_str, self.root_id)
        return []

    def _fetch_via_playwright(self) -> Optional[dict]:
        """Load schedule page via playwright and intercept the timetable API response."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            log.error("playwright is required for GLZ schedule scraping: pip install playwright && python -m playwright install chromium")
            return None

        schedule_path = GLZ_SCHEDULE_PAGES.get(self.root_id, GLZ_SCHEDULE_PAGES["1920"])
        url = GLZ_BASE + schedule_path
        api_fragment = f"getTimetable?rootId={self.root_id}"

        result = {}

        def on_response(response):
            if api_fragment in response.url:
                try:
                    result["data"] = json.loads(response.text())
                except Exception:
                    pass

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.on("response", on_response)
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(12000)
            finally:
                browser.close()

        return result.get("data")

    def _parse_day(self, day_entry: dict) -> list[ProgramSlot]:
        slots: list[ProgramSlot] = []
        for prog in day_entry.get("programmes", []):
            slot = self._parse_programme(prog)
            if slot:
                slots.append(slot)
        slots.sort(key=lambda s: s.start)
        return slots

    @staticmethod
    def _parse_programme(prog: dict) -> Optional[ProgramSlot]:
        title = (prog.get("topText") or "").strip()
        if not title:
            return None

        start = _parse_local_time(prog.get("start", ""))
        end = _parse_local_time(prog.get("end", ""))
        if start is None or end is None:
            return None

        description = prog.get("description")
        return ProgramSlot(start=start, end=end, title=title, description=description)


def _parse_local_time(s: str) -> Optional[time]:
    """Parse local datetime string like '2026-03-01T05:00:00' to time."""
    if not s:
        return None
    try:
        dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.time()
    except ValueError:
        return None
