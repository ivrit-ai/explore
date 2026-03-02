"""Schedule scraper for Eco 99FM.

Fetches the broadcast schedule from eco99fm.maariv.co.il public API.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, time
from typing import Optional
from urllib.request import urlopen, Request

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

ECO99_SCHEDULE_API = "https://eco99fm.maariv.co.il/api/v1/public/radio-single-broadcast/all"

# Hebrew day letter -> Python weekday() (Mon=0 .. Sun=6)
_HEBREW_DAY_LETTER = {
    "א": 6,  # Sunday
    "ב": 0,  # Monday
    "ג": 1,  # Tuesday
    "ד": 2,  # Wednesday
    "ה": 3,  # Thursday
    "ו": 4,  # Friday
}

_HEBREW_DAY_WORDS = {
    "ראשון": 6,
    "שני": 0,
    "שלישי": 1,
    "רביעי": 2,
    "חמישי": 3,
    "שישי": 4,
    "שבת": 5,
}


def _parse_day_range(day_range: str) -> set[int]:
    """Parse Hebrew day range string into a set of Python weekday() values.

    Examples:
        "ימים א'-ה'" -> {6, 0, 1, 2, 3}  (Sun-Thu)
        "ימים א'-ד'" -> {6, 0, 1, 2}      (Sun-Wed)
        "יום חמישי"  -> {3}                (Thu)
        "יום שישי"   -> {4}                (Fri)
        "יום שבת"    -> {5}                (Sat)
    """
    s = day_range.strip()

    # Single day: "יום חמישי", "יום שבת", etc.
    for word, wd in _HEBREW_DAY_WORDS.items():
        if word in s:
            return {wd}

    # Range: "ימים א'-ה'" or "ימים א׳-ה׳"
    # Extract Hebrew letters (strip geresh ׳ or apostrophe ')
    letters = re.findall(r"([אבגדהו])[׳']?", s)
    if len(letters) == 2:
        start_wd = _HEBREW_DAY_LETTER.get(letters[0])
        end_wd = _HEBREW_DAY_LETTER.get(letters[1])
        if start_wd is not None and end_wd is not None:
            # Build the range in Israeli week order (Sun=6, Mon=0, ..., Sat=5)
            israel_order = [6, 0, 1, 2, 3, 4, 5]
            start_idx = israel_order.index(start_wd)
            end_idx = israel_order.index(end_wd)
            return set(israel_order[start_idx:end_idx + 1])
    elif len(letters) == 1:
        wd = _HEBREW_DAY_LETTER.get(letters[0])
        if wd is not None:
            return {wd}

    log.warning("Could not parse day range: %r", day_range)
    return set()


class Eco99FmScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from eco99fm.maariv.co.il API."""

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch Eco99FM schedule for %s", day)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        req = Request(ECO99_SCHEDULE_API, headers=headers)
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        weekday = day.weekday()  # Mon=0 .. Sun=6
        slots: list[ProgramSlot] = []

        for item in data.get("items", []):
            day_range = item.get("day_range", "")
            if weekday not in _parse_day_range(day_range):
                continue

            slot = self._parse_item(item)
            if slot:
                slots.append(slot)

        slots.sort(key=lambda s: s.start)
        return slots

    @staticmethod
    def _parse_item(item: dict) -> Optional[ProgramSlot]:
        title = (item.get("program_name") or "").strip()
        if not title:
            return None

        hour_range = item.get("hour_range", "")
        parts = hour_range.split("-")
        if len(parts) != 2:
            return None

        start = _parse_hhmm(parts[0].strip())
        end = _parse_hhmm(parts[1].strip())
        if start is None or end is None:
            return None

        broadcaster = (item.get("broadcaster_name") or "").strip()
        return ProgramSlot(
            start=start,
            end=end,
            title=title,
            description=broadcaster or None,
        )


def _parse_hhmm(s: str) -> Optional[time]:
    """Parse 'HH:MM' string to time."""
    try:
        parts = s.split(":")
        h, m = int(parts[0]), int(parts[1])
        if h == 24:
            h = 0
        return time(h, m)
    except (ValueError, IndexError):
        return None
