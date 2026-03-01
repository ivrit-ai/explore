"""Schedule scraper for Galei Tzahal (Galgalatz)."""
from __future__ import annotations

import json
import logging
import re
from datetime import date, time, datetime
from typing import Optional
from urllib.request import urlopen, Request

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

GLZ_SCHEDULE_URL = "https://www.glz.co.il/schedule"


class GlzScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from glz.co.il."""

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch Galgalatz schedule for %s", day)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        # Galgalatz typically embeds schedule data as JSON in page or has an API
        day_name = _hebrew_day_name(day.weekday())
        url = f"{GLZ_SCHEDULE_URL}?day={day_name}"
        req = Request(url, headers={"User-Agent": "ivrit-explore/1.0"})
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Try to extract JSON data from embedded script tags
        slots = self._parse_html(html)
        slots.sort(key=lambda s: s.start)
        return slots

    def _parse_html(self, html: str) -> list[ProgramSlot]:
        """Extract schedule from HTML — try JSON-LD or embedded data first, fall back to regex."""
        slots: list[ProgramSlot] = []

        # Look for schedule data in script tags
        for m in re.finditer(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL):
            try:
                data = json.loads(m.group(1))
                slots = self._extract_from_json(data)
                if slots:
                    return slots
            except (json.JSONDecodeError, TypeError):
                continue

        # Fallback: extract time + title patterns from HTML
        # Pattern: "HH:MM" followed by program name
        for m in re.finditer(
            r'(\d{1,2}:\d{2})\s*[-–]?\s*(\d{1,2}:\d{2}).*?class="[^"]*title[^"]*"[^>]*>([^<]+)',
            html, re.DOTALL
        ):
            try:
                start = datetime.strptime(m.group(1), "%H:%M").time()
                end = datetime.strptime(m.group(2), "%H:%M").time()
                title = m.group(3).strip()
                if title:
                    slots.append(ProgramSlot(start=start, end=end, title=title))
            except ValueError:
                continue

        return slots

    def _extract_from_json(self, data) -> list[ProgramSlot]:
        """Try to extract schedule slots from parsed JSON structure."""
        slots: list[ProgramSlot] = []
        if isinstance(data, dict):
            # Look for schedule-like arrays in the JSON
            for key, val in data.items():
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    for item in val:
                        slot = self._item_to_slot(item)
                        if slot:
                            slots.append(slot)
                    if slots:
                        return slots
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    slot = self._item_to_slot(item)
                    if slot:
                        slots.append(slot)
        return slots

    @staticmethod
    def _item_to_slot(item: dict) -> Optional[ProgramSlot]:
        title = item.get("title", item.get("name", "")).strip()
        if not title:
            return None
        start_str = item.get("start_time", item.get("startTime", item.get("from", "")))
        end_str = item.get("end_time", item.get("endTime", item.get("to", "")))
        try:
            start = datetime.strptime(str(start_str)[:5], "%H:%M").time()
            end = datetime.strptime(str(end_str)[:5], "%H:%M").time()
            return ProgramSlot(start=start, end=end, title=title)
        except (ValueError, TypeError):
            return None


def _hebrew_day_name(weekday: int) -> str:
    """Convert Python weekday (0=Mon) to Hebrew day name for URL."""
    names = ["שני", "שלישי", "רביעי", "חמישי", "שישי", "שבת", "ראשון"]
    return names[weekday]
