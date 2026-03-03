"""Schedule scraper for Kan stations (Kan Bet, 88FM, Reshet Gimmel).

Scrapes the embedded liveSchedule data from kan.org.il/radio/ page.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, time, timezone
from typing import Optional
from urllib.request import urlopen, Request
from zoneinfo import ZoneInfo

IL_TZ = ZoneInfo("Asia/Jerusalem")

from .base import BaseScheduleScraper, ProgramSlot

log = logging.getLogger(__name__)

KAN_RADIO_URL = "https://www.kan.org.il/radio/"
KAN_TV_URL = "https://www.kan.org.il/tv-guide/"


class KanScheduleScraper(BaseScheduleScraper):
    """Scrape schedule from kan.org.il radio page."""

    def __init__(self, channel_id: str = "8"):
        self.channel_id = channel_id

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        try:
            return self._fetch(day)
        except Exception:
            log.exception("Failed to fetch Kan schedule for %s (channel %s)", day, self.channel_id)
            return []

    def _fetch(self, day: date) -> list[ProgramSlot]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html, */*",
        }
        req = Request(KAN_RADIO_URL, headers=headers)
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Extract liveSchedule.push({...}) blocks from inline scripts
        target_id = str(self.channel_id)
        for m in re.finditer(r"liveSchedule\.push\((\{.*?\})\);", html, re.DOTALL):
            try:
                data = json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
            if str(data.get("ChannelId")) != target_id:
                continue

            slots: list[ProgramSlot] = []
            for item in data.get("SchedulelItems", []):
                slot = self._parse_item(item, day)
                if slot:
                    slots.append(slot)
            slots.sort(key=lambda s: s.start)
            return slots

        log.warning("Channel %s not found in Kan radio page", self.channel_id)
        return []

    @staticmethod
    def _parse_item(item: dict, day: date) -> Optional[ProgramSlot]:
        title = (item.get("EpisodeName") or item.get("ProgramName") or "").strip()
        if not title:
            return None

        start_str = item.get("StartingTime", "")
        end_str = item.get("EndingTime", "")
        start = _parse_time(start_str)
        end = _parse_time(end_str)
        if start is None or end is None:
            return None

        return ProgramSlot(
            start=start,
            end=end,
            title=title,
            description=item.get("Description"),
        )


class KanTvScheduleScraper(KanScheduleScraper):
    """Scrape TV schedule from kan.org.il/tv-guide/ page."""

    def __init__(self, channel_id: str = "4444"):
        super().__init__(channel_id=channel_id)

    def _fetch(self, day: date) -> list[ProgramSlot]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html, */*",
        }
        req = Request(KAN_TV_URL, headers=headers)
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        target_id = str(self.channel_id)
        for m in re.finditer(r"liveSchedule\.push\((\{.*?\})\);", html, re.DOTALL):
            try:
                data = json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
            if str(data.get("ChannelId")) != target_id:
                continue

            slots: list[ProgramSlot] = []
            for item in data.get("SchedulelItems", []):
                slot = self._parse_item(item, day)
                if slot:
                    slots.append(slot)
            slots.sort(key=lambda s: s.start)
            return slots

        log.warning("Channel %s not found in Kan TV guide page", self.channel_id)
        return []


def _parse_time(s: str) -> Optional[time]:
    """Parse UTC ISO datetime string and convert to Israel local time."""
    if not s:
        return None
    # Strip timezone suffix for parsing as UTC
    s = s.replace("Z", "").split("+")[0]
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            utc_dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            local_dt = utc_dt.astimezone(IL_TZ)
            return local_dt.time()
        except ValueError:
            continue
    return None
