"""Schedule scraper for Channel 13 / רשת 13.

Fetches the broadcast schedule from https://13tv.co.il/tv-guide/
which is a Next.js SSR page with schedule data embedded in __NEXT_DATA__.
End times are inferred from the next show's start time; the last show of
the day ends at midnight (00:00).
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

CH13_GUIDE_URL = "https://13tv.co.il/tv-guide/"


class Channel13ScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from 13tv.co.il/tv-guide/ (Next.js __NEXT_DATA__)."""

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch Channel 13 schedule for %s", day)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        date_str = day.strftime("%Y-%m-%d")

        req = Request(CH13_GUIDE_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        })
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html, re.DOTALL,
        )
        if not m:
            log.warning("No __NEXT_DATA__ found in Channel 13 schedule page")
            return []

        data = json.loads(m.group(1))

        try:
            page_grid = data["props"]["pageProps"]["page"]["Content"]["PageGrid"]
        except (KeyError, TypeError):
            log.warning("Unexpected __NEXT_DATA__ structure for Channel 13")
            return []

        broadcast_week = None
        for item in page_grid:
            if "broadcastWeek" in item:
                broadcast_week = item["broadcastWeek"]
                break

        if not broadcast_week:
            log.warning("No broadcastWeek found in Channel 13 schedule")
            return []

        # Collect shows for the requested day across all week entries
        shows: list[dict] = []
        for week_entry in broadcast_week:
            for show in week_entry.get("shows", []):
                if show.get("show_date") == date_str:
                    shows.append(show)

        if not shows:
            return []

        shows.sort(key=lambda s: s.get("start_time", ""))

        slots: list[ProgramSlot] = []
        for i, show in enumerate(shows):
            title = (show.get("title") or "").strip()
            start = _parse_hhmm(show.get("start_time", ""))
            if not title or start is None:
                continue

            # End time = next show's start; last show ends at midnight
            if i + 1 < len(shows):
                end = _parse_hhmm(shows[i + 1].get("start_time", ""))
            else:
                end = None
            if end is None:
                end = time(0, 0)

            desc = (show.get("desc") or "").strip() or None
            slots.append(ProgramSlot(start=start, end=end, title=title, description=desc))

        return slots


def _parse_hhmm(s: str) -> Optional[time]:
    try:
        h, m = map(int, s.split(":"))
        return time(h % 24, m)
    except (ValueError, AttributeError):
        return None
