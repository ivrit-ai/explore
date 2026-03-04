"""Schedule scraper for Channel 12 (ערוץ 12 / Mako).

Fetches the broadcast EPG from www.mako.co.il/AjaxPage?jspName=EPGResponse.jsp.
The response contains ~10 days of schedule data in Israel local time.
"""
from __future__ import annotations

import json
import logging
from datetime import date, time
from typing import Optional
from urllib.request import urlopen, Request

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

MAKO_EPG_URL = "https://www.mako.co.il/AjaxPage?jspName=EPGResponse.jsp"


class MakoScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from Mako Channel 12 EPG endpoint."""

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch Mako schedule for %s", day)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        req = Request(MAKO_EPG_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.mako.co.il/tv-tv-schedule",
        })
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        # StartTime is "DD/MM/YYYY HH:MM:SS" in Israel local time
        date_prefix = day.strftime("%d/%m/%Y")

        slots: list[ProgramSlot] = []
        for prog in data.get("programs", []):
            start_str = prog.get("StartTime", "")
            if not start_str.startswith(date_prefix):
                continue

            title = (prog.get("ProgramName") or "").strip()
            if not title:
                continue

            start = _parse_hhmm(prog.get("DisplayStartTime", ""))
            end = _parse_hhmm(prog.get("DisplayEndTime", ""))
            if start is None or end is None:
                continue

            description = (prog.get("EventDescription") or "").strip() or None
            slots.append(ProgramSlot(
                start=start,
                end=end,
                title=title,
                description=description,
            ))

        slots.sort(key=lambda s: s.start)
        return slots


def _parse_hhmm(s: str) -> Optional[time]:
    try:
        h, m = map(int, s.split(":"))
        return time(h % 24, m)
    except (ValueError, AttributeError):
        return None
