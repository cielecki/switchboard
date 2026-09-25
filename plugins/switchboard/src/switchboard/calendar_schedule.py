from __future__ import annotations

import re
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MISSED_POLICIES = {"catch-up-once", "skip"}
AMBIGUOUS_POLICIES = {"first", "second"}
NONEXISTENT_POLICIES = {"next-valid", "skip"}


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("calendar timestamps must include a timezone")
    return value


def _local_time(value: str) -> time:
    if not re.fullmatch(r"\d{2}:\d{2}", value):
        raise ValueError("calendar time must use HH:MM")
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("calendar time must use HH:MM") from exc
    if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
        raise ValueError("calendar time must use HH:MM")
    return parsed.replace(second=0, microsecond=0)


def normalize_calendar_rule(rule: dict[str, Any]) -> dict[str, Any]:
    local = _local_time(str(rule.get("local_time", "")))
    timezone = str(rule.get("timezone", "")).strip()
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown calendar timezone: {timezone}") from exc

    raw_weekdays = rule.get("weekdays", list(WEEKDAYS))
    if not isinstance(raw_weekdays, list) or not raw_weekdays:
        raise ValueError("calendar weekdays must be a non-empty list")
    weekdays = []
    for value in raw_weekdays:
        weekday = str(value).strip().lower()[:3]
        if weekday not in WEEKDAYS:
            raise ValueError(f"unknown calendar weekday: {value}")
        if weekday not in weekdays:
            weekdays.append(weekday)
    weekdays.sort(key=WEEKDAYS.index)

    missed = str(rule.get("missed_policy", "catch-up-once"))
    ambiguous = str(rule.get("ambiguous_time_policy", "first"))
    nonexistent = str(rule.get("nonexistent_time_policy", "next-valid"))
    if missed not in MISSED_POLICIES:
        raise ValueError(f"unknown missed occurrence policy: {missed}")
    if ambiguous not in AMBIGUOUS_POLICIES:
        raise ValueError(f"unknown ambiguous time policy: {ambiguous}")
    if nonexistent not in NONEXISTENT_POLICIES:
        raise ValueError(f"unknown nonexistent time policy: {nonexistent}")
    return {
        "local_time": local.strftime("%H:%M"),
        "timezone": timezone,
        "weekdays": weekdays,
        "missed_policy": missed,
        "ambiguous_time_policy": ambiguous,
        "nonexistent_time_policy": nonexistent,
    }


def _valid_candidates(naive: datetime, zone: ZoneInfo) -> list[datetime]:
    candidates: dict[datetime, datetime] = {}
    for fold in (0, 1):
        local = naive.replace(tzinfo=zone, fold=fold)
        utc = local.astimezone(UTC)
        if utc.astimezone(zone).replace(tzinfo=None) == naive:
            candidates[utc] = local
    return [candidates[key] for key in sorted(candidates)]


def occurrence_for_date(rule: dict[str, Any], local_date: date) -> datetime | None:
    normalized = normalize_calendar_rule(rule)
    if WEEKDAYS[local_date.weekday()] not in normalized["weekdays"]:
        return None
    zone = ZoneInfo(normalized["timezone"])
    scheduled_time = _local_time(normalized["local_time"])
    naive = datetime.combine(local_date, scheduled_time)
    candidates = _valid_candidates(naive, zone)
    if not candidates:
        if normalized["nonexistent_time_policy"] == "skip":
            return None
        for minutes in range(1, 24 * 60 + 1):
            candidates = _valid_candidates(naive + timedelta(minutes=minutes), zone)
            if candidates:
                break
    if not candidates:
        return None
    selected = candidates[0] if normalized["ambiguous_time_policy"] == "first" else candidates[-1]
    return selected.astimezone(UTC)


def next_occurrence(
    rule: dict[str, Any], after: datetime, *, inclusive: bool = False
) -> datetime:
    normalized = normalize_calendar_rule(rule)
    instant = _aware(after).astimezone(UTC)
    local_date = instant.astimezone(ZoneInfo(normalized["timezone"])).date()
    for offset in range(0, 366 * 8):
        occurrence = occurrence_for_date(normalized, local_date + timedelta(days=offset))
        if occurrence is not None:
            is_after = occurrence >= instant if inclusive else occurrence > instant
            if is_after:
                return occurrence
    raise ValueError("could not find a calendar occurrence within eight years")


def due_occurrences(
    rule: dict[str, Any], first_due: datetime, triggered_at: datetime
) -> list[datetime]:
    normalized = normalize_calendar_rule(rule)
    start = _aware(first_due).astimezone(UTC)
    end = _aware(triggered_at).astimezone(UTC)
    if start > end:
        return []
    zone = ZoneInfo(normalized["timezone"])
    first_date = start.astimezone(zone).date() - timedelta(days=1)
    last_date = end.astimezone(zone).date() + timedelta(days=1)
    result: list[datetime] = []
    current = first_date
    while current <= last_date:
        occurrence = occurrence_for_date(normalized, current)
        if occurrence is not None and start <= occurrence <= end:
            result.append(occurrence)
        current += timedelta(days=1)
    return sorted(set(result))


def preview_occurrences(
    rule: dict[str, Any], after: datetime, *, count: int = 5
) -> list[datetime]:
    if count < 1 or count > 100:
        raise ValueError("calendar preview count must be between 1 and 100")
    cursor = _aware(after).astimezone(UTC)
    result: list[datetime] = []
    for _ in range(count):
        cursor = next_occurrence(rule, cursor)
        result.append(cursor)
    return result
