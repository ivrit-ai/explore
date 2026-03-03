"""Schedule scraper for Channel 14 (ערוץ 14).

Fetches the broadcast schedule from www.c14.co.il/api/shidurim.
"""
from __future__ import annotations

import json
import logging
from datetime import date, time
from typing import Optional
from urllib.request import urlopen, Request

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

C14_SCHEDULE_API = "https://www.c14.co.il/api/shidurim"


class Channel14ScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from www.c14.co.il/api/shidurim."""

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch Channel 14 schedule for %s", day)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        date_str = day.strftime("%Y-%m-%d")
        req = Request(C14_SCHEDULE_API, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        # data["shidurim"] is a list of single-key dicts: {"YYYY-MM-DD": [programs]}
        programs: list[dict] = []
        for entry in data.get("shidurim", []):
            if date_str in entry:
                programs = entry[date_str]
                break

        slots: list[ProgramSlot] = []
        for prog in programs:
            title = (prog.get("program") or "").strip()
            start_str = prog.get("start", "")
            end_str = prog.get("end", "")
            if not title or not start_str or not end_str:
                continue

            start = _parse_hhmm(start_str)
            end = _parse_hhmm(end_str)
            if start is None or end is None:
                continue

            cast = (prog.get("cast") or "").strip()
            slots.append(ProgramSlot(
                start=start,
                end=end,
                title=title,
                description=cast or None,
            ))

        return slots


def _parse_hhmm(s: str) -> Optional[time]:
    try:
        h, m = map(int, s.split(":"))
        return time(h % 24, m)
    except (ValueError, AttributeError):
        return None
