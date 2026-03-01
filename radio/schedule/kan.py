"""Schedule scraper for Kan stations (Kan Bet, 88FM, Reshet Gimmel)."""
from __future__ import annotations

import logging
from datetime import date, time, datetime
from typing import Optional
from urllib.request import urlopen, Request
import json

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

KAN_SCHEDULE_API = "https://www.kan.org.il/api/live/schedule"


class KanScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from kan.org.il API."""

    def __init__(self, channel_id: str = "2"):
        self.channel_id = channel_id

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch Kan schedule for %s (channel %s)", day, self.channel_id)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        url = f"{KAN_SCHEDULE_API}?date={day.isoformat()}&channel={self.channel_id}"
        req = Request(url, headers={"User-Agent": "ivrit-explore/1.0"})
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        slots: list[ProgramSlot] = []
        for item in data if isinstance(data, list) else data.get("items", data.get("schedule", [])):
            slot = self._parse_item(item)
            if slot:
                slots.append(slot)

        slots.sort(key=lambda s: s.start)
        return slots

    @staticmethod
    def _parse_item(item: dict) -> Optional[ProgramSlot]:
        try:
            title = item.get("title", item.get("name", "")).strip()
            if not title:
                return None

            start_str = item.get("start_time", item.get("startTime", ""))
            end_str = item.get("end_time", item.get("endTime", ""))

            # Handle both "HH:MM" and ISO datetime formats
            start = _parse_time(start_str)
            end = _parse_time(end_str)
            if start is None or end is None:
                return None

            return ProgramSlot(
                start=start,
                end=end,
                title=title,
                description=item.get("description"),
            )
        except (KeyError, ValueError):
            return None


def _parse_time(s: str) -> Optional[time]:
    """Parse time from HH:MM or ISO datetime string."""
    if not s:
        return None
    # Try ISO datetime first
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s.split("+")[0].split("Z")[0], fmt).time()
        except ValueError:
            continue
    # Try plain time
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None
