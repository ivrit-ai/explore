"""Base schedule scraper interface."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from typing import Optional


@dataclass
class ProgramSlot:
    """A single program slot in a station's schedule."""
    start: time
    end: time
    title: str
    description: Optional[str] = None

    def start_seconds(self) -> float:
        return self.start.hour * 3600 + self.start.minute * 60 + self.start.second

    def end_seconds(self) -> float:
        return self.end.hour * 3600 + self.end.minute * 60 + self.end.second


class BaseScheduleScraper:
    """Base class for radio schedule scrapers."""

    def get_schedule(self, day: date) -> list[ProgramSlot]:
        """Return the program schedule for a given date.

        Returns an empty list on failure (graceful degradation).
        """
        raise NotImplementedError
