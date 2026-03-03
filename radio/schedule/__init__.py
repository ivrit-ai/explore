"""Schedule scraper factory."""
from __future__ import annotations

from typing import Optional

from .base import BaseScheduleScraper, ProgramSlot


def get_scraper(name: Optional[str], **kwargs) -> Optional[BaseScheduleScraper]:
    """Return a schedule scraper instance by name, or None."""
    if name is None:
        return None

    if name == "kan":
        from .kan import KanScheduleScraper
        return KanScheduleScraper(channel_id=kwargs.get("schedule_id", "8"))

    if name == "kan_tv":
        from .kan import KanTvScheduleScraper
        return KanTvScheduleScraper(channel_id=kwargs.get("schedule_id", "4444"))

    if name == "glz":
        from .glz import GlzScheduleScraper
        return GlzScheduleScraper(root_id=kwargs.get("schedule_id", "1920"))

    if name == "eco99fm":
        from .eco99fm import Eco99FmScheduleScraper
        return Eco99FmScheduleScraper()

    if name == "kolhai":
        from .kolhai import KolHaiScheduleScraper
        return KolHaiScheduleScraper(station_id=kwargs.get("schedule_id", "2"))

    if name == "c14":
        from .c14 import Channel14ScheduleScraper
        return Channel14ScheduleScraper()

    return None


__all__ = ["get_scraper", "BaseScheduleScraper", "ProgramSlot"]
