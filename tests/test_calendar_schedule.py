from __future__ import annotations

import unittest
from datetime import UTC, date, datetime

from switchboard.calendar_schedule import (
    due_occurrences,
    next_occurrence,
    normalize_calendar_rule,
    occurrence_for_date,
    preview_occurrences,
)


class CalendarScheduleTest(unittest.TestCase):
    def rule(self, **overrides: object) -> dict[str, object]:
        rule: dict[str, object] = {
            "local_time": "07:00",
            "timezone": "Europe/Warsaw",
            "weekdays": ["mon", "tue", "wed", "thu", "fri"],
            "missed_policy": "catch-up-once",
            "ambiguous_time_policy": "first",
            "nonexistent_time_policy": "next-valid",
        }
        rule.update(overrides)
        return rule

    def test_normalizes_weekdays_and_defaults(self) -> None:
        rule = normalize_calendar_rule(
            {"local_time": "07:00", "timezone": "Europe/Warsaw", "weekdays": ["Friday", "MON"]}
        )
        self.assertEqual(rule["local_time"], "07:00")
        self.assertEqual(rule["weekdays"], ["mon", "fri"])
        self.assertEqual(rule["missed_policy"], "catch-up-once")
        every_day = normalize_calendar_rule(
            {"local_time": "07:00", "timezone": "Europe/Warsaw"}
        )
        self.assertEqual(
            every_day["weekdays"],
            list(("mon", "tue", "wed", "thu", "fri", "sat", "sun")),
        )

    def test_next_occurrence_stays_at_wall_clock_across_dst(self) -> None:
        occurrences = preview_occurrences(
            self.rule(weekdays=list(("mon", "tue", "wed", "thu", "fri", "sat", "sun"))),
            datetime(2026, 3, 27, 12, tzinfo=UTC),
            count=4,
        )
        self.assertEqual(
            [item.isoformat() for item in occurrences],
            [
                "2026-03-28T06:00:00+00:00",
                "2026-03-29T05:00:00+00:00",
                "2026-03-30T05:00:00+00:00",
                "2026-03-31T05:00:00+00:00",
            ],
        )

    def test_ambiguous_time_policy_selects_first_or_second_instant(self) -> None:
        first = occurrence_for_date(
            self.rule(
                local_time="02:30",
                weekdays=["sun"],
                ambiguous_time_policy="first",
            ),
            date(2026, 10, 25),
        )
        second = occurrence_for_date(
            self.rule(
                local_time="02:30",
                weekdays=["sun"],
                ambiguous_time_policy="second",
            ),
            date(2026, 10, 25),
        )
        self.assertEqual(first.isoformat(), "2026-10-25T00:30:00+00:00")
        self.assertEqual(second.isoformat(), "2026-10-25T01:30:00+00:00")

    def test_nonexistent_time_can_move_to_next_valid_minute_or_skip(self) -> None:
        moved = occurrence_for_date(
            self.rule(local_time="02:30", weekdays=["sun"]), date(2026, 3, 29)
        )
        skipped = occurrence_for_date(
            self.rule(
                local_time="02:30", weekdays=["sun"], nonexistent_time_policy="skip"
            ),
            date(2026, 3, 29),
        )
        self.assertEqual(moved.isoformat(), "2026-03-29T01:00:00+00:00")
        self.assertIsNone(skipped)

    def test_due_occurrences_includes_every_missed_occurrence(self) -> None:
        due = due_occurrences(
            self.rule(),
            datetime(2026, 9, 21, 5, tzinfo=UTC),
            datetime(2026, 9, 24, 12, tzinfo=UTC),
        )
        self.assertEqual(
            [item.isoformat() for item in due],
            [
                "2026-09-21T05:00:00+00:00",
                "2026-09-22T05:00:00+00:00",
                "2026-09-23T05:00:00+00:00",
                "2026-09-24T05:00:00+00:00",
            ],
        )

    def test_next_occurrence_rejects_naive_timestamp(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone"):
            next_occurrence(self.rule(), datetime(2026, 9, 24, 12))


if __name__ == "__main__":
    unittest.main()
