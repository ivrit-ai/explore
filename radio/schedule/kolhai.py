"""Schedule scraper for Kol Hai (קול חי).

Fetches the broadcast schedule from emess.co.il public API.
"""
from __future__ import annotations

import json
import logging
from datetime import date, time
from typing import Optional
from urllib.request import urlopen, Request

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

# station_id: 2 = Kol Hai, 1 = Kol Hai Music
KOLHAI_SCHEDULE_API = "https://www.emess.co.il/Home/ScheduleJ/{station_id}"

# emess.co.il day numbering -> Python weekday() (Mon=0 .. Sun=6)
# emess: 0=Sun, 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat
_EMESS_TO_WEEKDAY = {
    0: 6,  # Sun
    1: 0,  # Mon
    2: 1,  # Tue
    3: 2,  # Wed
    4: 3,  # Thu
    5: 4,  # Fri
    6: 5,  # Sat
}
_WEEKDAY_TO_EMESS = {v: k for k, v in _EMESS_TO_WEEKDAY.items()}


class KolHaiScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from emess.co.il API."""

    def __init__(self, station_id: str = "2"):
        self.station_id = station_id

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch KolHai schedule for %s", day)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        url = KOLHAI_SCHEDULE_API.format(station_id=self.station_id)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        req = Request(url, headers=headers)
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        programs = {p["id"]: p for p in data.get("programs", [])}
        emess_day = _WEEKDAY_TO_EMESS.get(day.weekday())
        if emess_day is None:
            return []

        # Group consecutive hours with the same program into slots
        day_entries = sorted(
            [e for e in data.get("schedule", []) if e["day"] == emess_day],
            key=lambda e: e["hour"],
        )

        slots: list[ProgramSlot] = []
        i = 0
        while i < len(day_entries):
            entry = day_entries[i]
            prog_id = entry["program"]
            if prog_id == 0:
                i += 1
                continue

            prog = programs.get(prog_id)
            if not prog:
                i += 1
                continue

            start_hour = entry["hour"]
            end_hour = start_hour + 1

            # Merge consecutive hours with same program
            j = i + 1
            while j < len(day_entries) and day_entries[j]["program"] == prog_id:
                end_hour = day_entries[j]["hour"] + 1
                j += 1

            title = (prog.get("title") or "").strip()
            broadcaster = (prog.get("name") or "").strip()

            if title:
                slots.append(ProgramSlot(
                    start=time(start_hour, 0),
                    end=time(end_hour % 24, 0),
                    title=title,
                    description=broadcaster or None,
                ))

            i = j

        return slots
