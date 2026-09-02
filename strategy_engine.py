"""Real-time competitor telemetry analysis for the Pitwall Pager system.

The engine accepts FastF1-like lap frames, so it can run unchanged against a
historical replay or a caller's normalised live timing feed.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Generator, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from fastf1.core import Laps


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Tunable thresholds; all time-related values are in seconds."""

    rolling_window: int = 3
    baseline_laps: int = 3
    pace_shift_threshold: float = 0.15
    expected_degradation_slope: float = 0.08
    pit_drop_threshold: float = 0.40
    average_pit_loss: float = 20.0
    pit_prediction_horizon_laps: int = 12
    min_laps_for_degradation: int = 4


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    driver: str
    event_type: str
    lap_number: int
    value_seconds: float
    message: str
    timestamp_seconds: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class LiveTelemetrySimulator:
    """Replay valid lap records in their observed chronological order.

    ``speed_multiplier=0`` disables waiting.  A value of 10 plays ten times
    faster than real time.  Invalid / non-timed laps are excluded by default.
    """

    REQUIRED_COLUMNS = frozenset({"Driver", "LapNumber", "LapTime"})

    def __init__(
        self,
        laps: "pd.DataFrame | Laps",
        speed_multiplier: float = 0.0,
        include_invalid_laps: bool = False,
    ) -> None:
        if speed_multiplier < 0:
            raise ValueError("speed_multiplier must be non-negative")
        missing = self.REQUIRED_COLUMNS.difference(laps.columns)
        if missing:
            raise ValueError(f"laps is missing required columns: {sorted(missing)}")
        self._speed_multiplier = speed_multiplier
        self._include_invalid_laps = include_invalid_laps
        self._laps = laps.copy()

    @staticmethod
    def _seconds(value: Any) -> float | None:
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timedelta):
            return float(value.total_seconds())
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        return result if np.isfinite(result) else None

    def stream(self) -> Generator[dict[str, Any], None, None]:
        """Yield normalised rows, sleeping according to lap-start deltas."""
        frame = self._laps.copy()
        if not self._include_invalid_laps and "IsAccurate" in frame.columns:
            frame = frame.loc[frame["IsAccurate"].fillna(False)]
        frame["_lap_seconds"] = frame["LapTime"].map(self._seconds)
        frame = frame.loc[frame["_lap_seconds"].notna()].copy()

        time_column = next(
            (column for column in ("LapStartTime", "Time") if column in frame.columns),
            None,
        )
        if time_column:
            frame["_stream_time"] = frame[time_column].map(self._seconds)
            frame = frame.sort_values(["_stream_time", "Driver", "LapNumber"], na_position="last")
        else:
            frame = frame.sort_values(["LapNumber", "Driver"])

        previous_time: float | None = None
        for _, source_row in frame.iterrows():
            row = source_row.drop(labels=["_lap_seconds", "_stream_time"], errors="ignore").to_dict()
            row["lap_time_seconds"] = float(source_row["_lap_seconds"])
            stream_time = source_row.get("_stream_time")
            if self._speed_multiplier > 0 and pd.notna(stream_time):
                current_time = float(stream_time)
                if previous_time is not None:
                    sleep(max(0.0, current_time - previous_time) / self._speed_multiplier)
                previous_time = current_time
            yield row


@dataclass(slots=True)
class _DriverState:
    stint: int = 0
    stint_laps: list[tuple[int, float]] = field(default_factory=list)
    completed_stints: list[list[tuple[int, float]]] = field(default_factory=list)
    rolling_times: deque[float] = field(default_factory=deque)
    baseline: float | None = None
    emitted: set[str] = field(default_factory=set)


class CompetitorObserver:
    """Consumes normalised lap rows and emits only actionable strategy events."""

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or StrategyConfig()
        if self.config.rolling_window < 2 or self.config.baseline_laps < 2:
            raise ValueError("rolling_window and baseline_laps must both be at least 2")
        self._drivers: dict[str, _DriverState] = defaultdict(_DriverState)

    @staticmethod
    def _lap_seconds(row: Mapping[str, Any]) -> float | None:
        value = row.get("lap_time_seconds", row.get("LapTime"))
        return LiveTelemetrySimulator._seconds(value)

    @staticmethod
    def _is_pit_lap(row: Mapping[str, Any]) -> bool:
        return bool(row.get("PitInTime") is not None and not pd.isna(row.get("PitInTime"))) or bool(
            row.get("PitOutTime") is not None and not pd.isna(row.get("PitOutTime"))
        )

    def process_lap(self, row: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Process one FastF1 lap row and return newly detected event dictionaries."""
        driver = str(row.get("Driver", "")).upper().strip()
        lap_time = self._lap_seconds(row)
        if not driver or lap_time is None or lap_time <= 0:
            return []
        try:
            lap_number = int(row["LapNumber"])
        except (KeyError, TypeError, ValueError):
            return []

        state = self._drivers[driver]
        if self._is_pit_lap(row) and state.stint_laps:
            state.completed_stints.append(state.stint_laps.copy())
            state.stint += 1
            state.stint_laps.clear()
            state.rolling_times.clear()
            state.baseline = None
            state.emitted.clear()
            # A pit-in/out lap is not representative tyre pace.  The next
            # timed lap becomes the first sample of the new stint.
            return []

        state.stint_laps.append((lap_number, lap_time))
        state.rolling_times.append(lap_time)
        if len(state.rolling_times) > self.config.rolling_window:
            state.rolling_times.popleft()
        if state.baseline is None and len(state.stint_laps) >= self.config.baseline_laps:
            state.baseline = float(np.median([time for _, time in state.stint_laps[: self.config.baseline_laps]]))
        if state.baseline is None or len(state.rolling_times) < self.config.rolling_window:
            return []

        events: list[TelemetryEvent] = []
        rolling_median = float(np.median(state.rolling_times))
        pace_delta = rolling_median - state.baseline
        if abs(pace_delta) >= self.config.pace_shift_threshold:
            event_type = "PACE_DROP" if pace_delta > 0 else "PACE_INCREASE"
            events.append(self._event(driver, event_type, lap_number, pace_delta, row))

        slope: float | None = None
        if len(state.stint_laps) >= self.config.min_laps_for_degradation:
            lap_indices = np.arange(len(state.stint_laps), dtype=float)
            times = np.array([time for _, time in state.stint_laps], dtype=float)
            slope = float(np.polyfit(lap_indices, times, 1)[0])
            if slope > self.config.expected_degradation_slope:
                events.append(self._event(driver, "HIGH_DEGRADATION", lap_number, slope, row))

        if slope is not None and pace_delta >= self.config.pit_drop_threshold:
            horizon = self.config.pit_prediction_horizon_laps
            projected_loss = horizon * pace_delta + slope * horizon * (horizon - 1) / 2
            if projected_loss >= self.config.average_pit_loss:
                events.append(self._event(driver, "PIT_WINDOW", lap_number, projected_loss, row))

        new_events = [event for event in events if event.event_type not in state.emitted]
        state.emitted.update(event.event_type for event in new_events)
        return [event.as_dict() for event in new_events]

    def stint_history(self, driver: str) -> tuple[tuple[tuple[int, float], ...], ...]:
        """Return completed stints followed by the driver's current stint."""
        state = self._drivers.get(driver.upper())
        if state is None:
            return ()
        stints = [*state.completed_stints, state.stint_laps]
        return tuple(tuple(stint) for stint in stints)

    @staticmethod
    def _event(
        driver: str, event_type: str, lap_number: int, value: float, row: Mapping[str, Any]
    ) -> TelemetryEvent:
        labels = {
            "PACE_INCREASE": "rolling pace improved",
            "PACE_DROP": "rolling pace deteriorated",
            "HIGH_DEGRADATION": "stint degradation above baseline",
            "PIT_WINDOW": "projected degradation exceeds pit-loss trade-off",
        }
        timestamp = LiveTelemetrySimulator._seconds(row.get("LapStartTime", row.get("Time")))
        return TelemetryEvent(driver, event_type, lap_number, value, labels[event_type], timestamp)


class EventDispatcher:
    """Routes events to pager-ready text and a portable post-race report."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    @staticmethod
    def format_for_pager(event_dict: Mapping[str, Any]) -> str:
        """Return exactly two concise LCD lines, each capped at 16 characters."""
        driver = str(event_dict["driver"])[:3].upper()
        event_type = str(event_dict["event_type"])
        aliases = {"PACE_INCREASE": "PACE UP", "PACE_DROP": "PACE DN", "HIGH_DEGRADATION": "HIGH DEG", "PIT_WINDOW": "PIT WINDOW"}
        line_one = f"{driver}: {aliases.get(event_type, event_type)}"[:16]
        value = float(event_dict["value_seconds"])
        lap = int(event_dict["lap_number"])
        unit = "s/lap" if event_type == "HIGH_DEGRADATION" else "s"
        line_two = f"{value:+.2f}{unit} L{lap}"[:16]
        return f"{line_one}\n{line_two}"

    def log_event(self, event_dict: Mapping[str, Any]) -> None:
        required = {"driver", "event_type", "lap_number", "value_seconds", "message"}
        missing = required.difference(event_dict)
        if missing:
            raise ValueError(f"event is missing fields: {sorted(missing)}")
        self.events.append(dict(event_dict))

    def generate_markdown_summary(self, filepath: str | Path) -> Path:
        target = Path(filepath)
        target.parent.mkdir(parents=True, exist_ok=True)
        grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for event in self.events:
            grouped[str(event["driver"])][str(event["event_type"])].append(event)
        lines = ["# Pitwall Pager: Post-Race Strategy Summary", "", f"**Events logged:** {len(self.events)}", ""]
        for driver in sorted(grouped):
            lines.extend([f"## {driver}", ""])
            for event_type in sorted(grouped[driver]):
                lines.extend([f"### {event_type.replace('_', ' ').title()}", ""])
                for event in grouped[driver][event_type]:
                    lines.append(f"- Lap {event['lap_number']}: {event['value_seconds']:+.3f}s — {event['message']}")
                lines.append("")
        target.write_text("\n".join(lines), encoding="utf-8")
        return target


def _mock_laps() -> pd.DataFrame:
    """Small deterministic replay, deliberately containing VER tyre degradation."""
    return pd.DataFrame(
        {
            "Driver": ["VER", "NOR"] * 6,
            "LapNumber": np.repeat(np.arange(1, 7), 2),
            "LapTime": pd.to_timedelta([90.0, 90.2, 90.1, 90.1, 90.0, 90.2, 90.3, 90.1, 90.6, 90.2, 90.9, 90.1], unit="s"),
            "LapStartTime": pd.to_timedelta(np.repeat(np.arange(0, 600, 100), 2), unit="s"),
            "IsAccurate": True,
        }
    )


if __name__ == "__main__":
    simulator = LiveTelemetrySimulator(_mock_laps())
    observer = CompetitorObserver(StrategyConfig(expected_degradation_slope=0.07, pit_drop_threshold=0.25, average_pit_loss=2.0, pit_prediction_horizon_laps=6))
    dispatcher = EventDispatcher()
    for lap in simulator.stream():
        for event in observer.process_lap(lap):
            dispatcher.log_event(event)
            print(dispatcher.format_for_pager(event))
    report = dispatcher.generate_markdown_summary("post_race_summary.md")
    print(f"Wrote {len(dispatcher.events)} events to {report}")
