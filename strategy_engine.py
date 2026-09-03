"""Real-time competitor telemetry analysis for the Pitwall Pager system.

The engine accepts FastF1-like lap frames, so it can run unchanged against a
historical replay or a caller's normalised live timing feed.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Generator, Mapping
from dataclasses import asdict, dataclass, field
import logging
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from fastf1.core import Laps


LOGGER = logging.getLogger(__name__)
RETIREMENT_STATUS_TERMS = frozenset(
    {"accident", "collision", "engine", "power unit", "hydraulics", "retired"}
)


def _is_retirement_status(value: Any) -> bool:
    """Return whether a FastF1-style status explicitly represents retirement."""
    if value is None or pd.isna(value):
        return False
    normalized = str(value).casefold()
    return any(term in normalized for term in RETIREMENT_STATUS_TERMS)


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Dynamic strategy-model parameters; all time values are seconds."""

    rolling_window: int = 3
    baseline_laps: int = 3
    min_laps_for_degradation: int = 5
    field_min_drivers: int = 3
    zscore_threshold: float = 2.6
    zscore_reset_threshold: float = 1.2
    min_relative_delta: float = 0.35
    min_relative_std: float = 0.05
    outlier_mad_zscore: float = 4.5
    degradation_window: int = 5
    min_compound_peers: int = 3
    tyre_cliff_acceleration: float = 0.08
    undercut_loss_threshold: float = 2.0
    missing_timing_laps_for_dnf: int = 3


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    driver: str
    event_type: str
    lap_number: int
    value_seconds: float
    message: str
    timestamp_seconds: float | None = None
    priority: str = "NORMAL"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class LiveTelemetrySimulator:
    """Replay valid lap records in strict race-lap order.

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
        retirement_mask = (
            frame["Status"].map(_is_retirement_status)
            if "Status" in frame.columns
            else pd.Series(False, index=frame.index)
        )
        if not self._include_invalid_laps and "IsAccurate" in frame.columns:
            # Preserve pit records even when FastF1 marks the transit lap as
            # inaccurate: the observer needs them to reset stint state.
            pit_mask = pd.Series(False, index=frame.index)
            for column in ("PitInTime", "PitOutTime"):
                if column in frame.columns:
                    pit_mask |= frame[column].notna()
            frame = frame.loc[frame["IsAccurate"].fillna(False) | pit_mask | retirement_mask]
        frame["_lap_seconds"] = frame["LapTime"].map(self._seconds)
        # Retirement packets may contain no final LapTime, but must still
        # reach the observer to produce the one-shot DNF alert.
        frame = frame.loc[frame["_lap_seconds"].notna() | retirement_mask.loc[frame.index]].copy()

        time_column = next(
            (column for column in ("LapStartTime", "Time") if column in frame.columns),
            None,
        )
        if time_column:
            frame["_stream_time"] = frame[time_column].map(self._seconds)
            # ``Time`` is commonly the lap-completion timestamp in FastF1 and
            # can place a delayed pit lap after the following race lap.  The
            # field observer requires complete, monotonically numbered lap
            # batches, so it is only a deterministic within-lap tie-breaker.
            frame = frame.sort_values(["LapNumber", "_stream_time", "Driver"], na_position="last")
        else:
            frame = frame.sort_values(["LapNumber", "Driver"])

        previous_time: float | None = None
        for _, source_row in frame.iterrows():
            row = source_row.drop(labels=["_lap_seconds", "_stream_time"], errors="ignore").to_dict()
            if pd.notna(source_row["_lap_seconds"]):
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
    relative_deltas: deque[float] = field(default_factory=deque)
    last_lap_time: float | None = None
    active_pace_signal: str | None = None
    tyre_cliff_active: bool = False
    cumulative_opponent_loss: float = 0.0
    pit_window_emitted: bool = False
    last_seen_lap: int | None = None
    retired: bool = False
    dnf_alert_emitted: bool = False


class CompetitorObserver:
    """Field-relative anomaly detector, processing one complete race lap at a time.

    ``process_lap`` buffers records until the next lap arrives.  Call
    :meth:`flush` after the final streamed row so the final lap is evaluated.
    This batching is essential: a driver is evaluated against the *same-lap*
    active field median, not a static personal lap-time threshold.
    """

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or StrategyConfig()
        if self.config.baseline_laps < 3 or self.config.field_min_drivers < 2:
            raise ValueError("baseline_laps must be >= 3 and field_min_drivers >= 2")
        if self.config.zscore_threshold <= 0 or self.config.min_relative_std <= 0:
            raise ValueError("zscore_threshold and min_relative_std must be positive")
        if self.config.missing_timing_laps_for_dnf < 1:
            raise ValueError("missing_timing_laps_for_dnf must be at least 1")
        self._drivers: dict[str, _DriverState] = defaultdict(_DriverState)
        self._pending_lap_number: int | None = None
        self._pending_rows: list[Mapping[str, Any]] = []
        self._last_field_median: float | None = None
        self._highest_processed_lap = 0

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
        """Buffer a row and emit events once its complete race lap is available."""
        try:
            lap_number = int(row["LapNumber"])
        except (KeyError, TypeError, ValueError):
            return []
        if self._pending_lap_number is None:
            if lap_number <= self._highest_processed_lap:
                LOGGER.warning(
                    "Ignoring late telemetry for lap %d; field state is already processed through lap %d",
                    lap_number,
                    self._highest_processed_lap,
                )
                return []
            self._pending_lap_number = lap_number
        if lap_number < self._pending_lap_number:
            # Reprocessing a completed field batch would mutate both field
            # pace and driver stint state.  Retain deterministic state and
            # tolerate delayed packets rather than terminating live timing.
            LOGGER.warning(
                "Ignoring late telemetry for lap %d while collecting lap %d",
                lap_number,
                self._pending_lap_number,
            )
            return []
        if lap_number != self._pending_lap_number:
            events = self._process_batch(self._pending_rows)
            self._highest_processed_lap = self._pending_lap_number
            self._pending_rows = []
            self._pending_lap_number = lap_number
        else:
            events = []
        self._pending_rows.append(row)
        return events

    def flush(self) -> list[dict[str, Any]]:
        """Evaluate the final buffered lap after a stream has ended."""
        if not self._pending_rows:
            return []
        events = self._process_batch(self._pending_rows)
        self._highest_processed_lap = self._pending_lap_number or self._highest_processed_lap
        self._pending_rows = []
        self._pending_lap_number = None
        return events

    @staticmethod
    def _is_green_lap(row: Mapping[str, Any]) -> bool:
        """Allow only an explicit green-flag TrackStatus (or no status field)."""
        status = row.get("TrackStatus")
        if status is None or pd.isna(status):
            return True
        if isinstance(status, (int, np.integer)):
            return int(status) == 1
        if isinstance(status, (float, np.floating)):
            return float(status) == 1.0
        return str(status).strip() == "1"

    def _process_batch(self, rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        parsed: list[tuple[str, int, float, Mapping[str, Any]]] = []
        events: list[TelemetryEvent] = []
        present_drivers: set[str] = set()
        batch_lap_number = self._pending_lap_number or 0
        for row in rows:
            driver = str(row.get("Driver", "")).upper().strip()
            lap_time = self._lap_seconds(row)
            if not driver:
                continue
            present_drivers.add(driver)
            state = self._drivers[driver]
            state.last_seen_lap = int(row["LapNumber"])
            if _is_retirement_status(row.get("Status")):
                events.extend(self._retire_driver(driver, int(row["LapNumber"]), row, "status"))
                continue
            if state.retired:
                continue
            if self._is_pit_lap(row):
                self._reset_stint(state)
                continue
            if not self._is_green_lap(row):
                self._invalidate_pace_baseline(state)
                continue
            if lap_time is None or lap_time <= 0:
                continue
            parsed.append((driver, int(row["LapNumber"]), lap_time, row))

        for driver, state in self._drivers.items():
            if (
                driver not in present_drivers
                and not state.retired
                and state.last_seen_lap is not None
                and batch_lap_number - state.last_seen_lap >= self.config.missing_timing_laps_for_dnf
            ):
                events.extend(self._retire_driver(driver, batch_lap_number, {}, "missing timing"))

        # A terminal-status packet can share a lap number with the driver's
        # final timing row; retirement takes precedence over pace analysis.
        parsed = [entry for entry in parsed if not self._drivers[entry[0]].retired]

        field_times = np.array([time for _, _, time, _ in parsed], dtype=float)
        field_times = self._remove_field_outliers(field_times)
        if len(field_times) < self.config.field_min_drivers:
            self._last_field_median = None
            return [event.as_dict() for event in events]
        field_median = float(np.median(field_times))
        field_delta = 0.0 if self._last_field_median is None else field_median - self._last_field_median
        self._last_field_median = field_median

        relative_values: dict[str, tuple[int, float, Mapping[str, Any]]] = {}
        for driver, lap_number, lap_time, row in parsed:
            state = self._drivers[driver]
            first_clean_lap_after_transition = state.last_lap_time is None
            driver_delta = 0.0 if first_clean_lap_after_transition else lap_time - state.last_lap_time
            relative_delta = driver_delta - field_delta
            state.last_lap_time = lap_time
            state.stint_laps.append((lap_number, lap_time))
            # The first green flying lap after a pit/SC transition establishes
            # a fresh reference only; it never enters the pace baseline.
            if first_clean_lap_after_transition:
                continue
            relative_values[driver] = (lap_number, relative_delta, row)
            events.extend(self._pace_events(driver, lap_number, relative_delta, row))
            if state.relative_deltas:
                state.cumulative_opponent_loss += max(0.0, relative_delta)
            state.relative_deltas.append(relative_delta)

        events.extend(self._tyre_cliff_events(parsed))
        for driver, (lap_number, _, row) in relative_values.items():
            state = self._drivers[driver]
            if not state.pit_window_emitted and state.cumulative_opponent_loss >= self.config.undercut_loss_threshold:
                state.pit_window_emitted = True
                events.append(self._event(driver, "PIT_WINDOW", lap_number, state.cumulative_opponent_loss, row))
        return [event.as_dict() for event in events]

    def _pace_events(self, driver: str, lap_number: int, relative_delta: float, row: Mapping[str, Any]) -> list[TelemetryEvent]:
        state = self._drivers[driver]
        history = np.asarray(state.relative_deltas, dtype=float)
        if len(history) < self.config.baseline_laps:
            return []
        mean = float(np.median(history))
        std = max(float(np.std(history, ddof=1)), self.config.min_relative_std)
        zscore = (relative_delta - mean) / std
        if abs(zscore) <= self.config.zscore_reset_threshold:
            state.active_pace_signal = None
            return []
        if abs(zscore) <= self.config.zscore_threshold or abs(relative_delta - mean) < self.config.min_relative_delta:
            return []
        event_type = "PACE_DROP" if zscore > 0 else "PACE_GAIN"
        if state.active_pace_signal == event_type:
            return []
        state.active_pace_signal = event_type
        return [self._event(driver, event_type, lap_number, relative_delta, row)]

    def _tyre_cliff_events(self, parsed: list[tuple[str, int, float, Mapping[str, Any]]]) -> list[TelemetryEvent]:
        slopes: dict[str, float] = {}
        compounds: dict[str, str] = {}
        for driver, _, _, row in parsed:
            state = self._drivers[driver]
            if len(state.stint_laps) < self.config.degradation_window:
                continue
            window = state.stint_laps[-self.config.degradation_window :]
            slopes[driver] = float(np.polyfit(np.arange(len(window), dtype=float), [time for _, time in window], 1)[0])
            compounds[driver] = str(row.get("Compound", "UNKNOWN")).upper()
        events: list[TelemetryEvent] = []
        for driver, lap_number, _, row in parsed:
            state = self._drivers[driver]
            slope = slopes.get(driver)
            peers = [value for peer, value in slopes.items() if peer != driver and compounds[peer] == compounds.get(driver)]
            if slope is None or len(peers) < self.config.min_compound_peers:
                continue
            expected = float(np.median(peers))
            peer_std = max(float(np.std(peers, ddof=1)), self.config.min_relative_std)
            acceleration = slope - expected
            abnormal = acceleration >= self.config.tyre_cliff_acceleration and acceleration / peer_std > self.config.zscore_threshold
            if not abnormal:
                state.tyre_cliff_active = False
                continue
            if not state.tyre_cliff_active:
                state.tyre_cliff_active = True
                events.append(self._event(driver, "HIGH_DEG", lap_number, acceleration, row))
        return events

    def _remove_field_outliers(self, values: np.ndarray) -> np.ndarray:
        if len(values) < 4:
            return values
        median = float(np.median(values))
        mad = float(np.median(np.abs(values - median)))
        if mad == 0:
            return values
        robust_z = 0.6745 * np.abs(values - median) / mad
        return values[robust_z <= self.config.outlier_mad_zscore]

    @staticmethod
    def _invalidate_pace_baseline(state: _DriverState) -> None:
        """Prevent a post-pit/SC flying lap from using a stale comparison."""
        state.last_lap_time = None
        state.relative_deltas.clear()
        state.active_pace_signal = None

    def _retire_driver(
        self, driver: str, lap_number: int, row: Mapping[str, Any], reason: str
    ) -> list[TelemetryEvent]:
        state = self._drivers[driver]
        state.retired = True
        self._invalidate_pace_baseline(state)
        if state.dnf_alert_emitted:
            return []
        state.dnf_alert_emitted = True
        LOGGER.warning("DNF detected for %s at lap %d (%s)", driver, lap_number, reason)
        return [self._event(driver, "DNF", lap_number, 0.0, row)]

    @staticmethod
    def _reset_stint(state: _DriverState) -> None:
        if state.stint_laps:
            state.completed_stints.append(state.stint_laps.copy())
        state.stint += 1
        state.stint_laps.clear()
        state.relative_deltas.clear()
        state.last_lap_time = None
        state.active_pace_signal = None
        state.tyre_cliff_active = False
        state.cumulative_opponent_loss = 0.0
        state.pit_window_emitted = False

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
            "DNF": "driver retired from the race",
            "PACE_GAIN": "statistically significant field-relative pace gain",
            "PACE_DROP": "statistically significant field-relative pace loss",
            "HIGH_DEG": "tyre degradation accelerated versus compound peers",
            "PIT_WINDOW": "cumulative field-relative loss reached undercut threshold",
        }
        timestamp = LiveTelemetrySimulator._seconds(row.get("LapStartTime", row.get("Time")))
        priority = "HIGH" if event_type == "DNF" else "NORMAL"
        return TelemetryEvent(driver, event_type, lap_number, value, labels[event_type], timestamp, priority)


class EventDispatcher:
    """Routes events to pager-ready text and a portable post-race report."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    @staticmethod
    def format_for_pager(event_dict: Mapping[str, Any]) -> str:
        """Return exactly two concise LCD lines, each capped at 16 characters."""
        driver = str(event_dict["driver"])[:3].upper()
        event_type = str(event_dict["event_type"])
        aliases = {"PACE_GAIN": "PACE UP", "PACE_DROP": "PACE DN", "HIGH_DEG": "HIGH DEG", "PIT_WINDOW": "PIT WINDOW"}
        if event_type == "DNF":
            return f"[{driver}]: DNF"[:16] + f"\nRETIRED L{int(event_dict['lap_number'])}"[:16]
        line_one = f"{driver}: {aliases.get(event_type, event_type)}"[:16]
        value = float(event_dict["value_seconds"])
        lap = int(event_dict["lap_number"])
        unit = "s/lap" if event_type == "HIGH_DEG" else "s"
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
    """Four-car replay with a single VER pace anomaly against a quiet field."""
    records: list[dict[str, Any]] = []
    for lap in range(1, 13):
        for driver, offset in (("VER", 0.00), ("NOR", 0.03), ("LEC", -0.02), ("HAM", 0.01)):
            anomaly = 0.50 if driver == "VER" and lap == 9 else 0.0
            records.append({"Driver": driver, "LapNumber": lap, "LapTime": pd.Timedelta(seconds=90 + offset + anomaly), "LapStartTime": pd.Timedelta(seconds=(lap - 1) * 90), "IsAccurate": True, "Compound": "MEDIUM"})
    records.append({"Driver": "HAM", "LapNumber": 10, "LapTime": pd.NaT, "LapStartTime": pd.Timedelta(seconds=9 * 90), "IsAccurate": False, "Status": "Accident"})
    return pd.DataFrame(records)


if __name__ == "__main__":
    simulator = LiveTelemetrySimulator(_mock_laps())
    observer = CompetitorObserver()
    dispatcher = EventDispatcher()
    for lap in simulator.stream():
        for event in observer.process_lap(lap):
            dispatcher.log_event(event)
            print(dispatcher.format_for_pager(event))
    for event in observer.flush():
        dispatcher.log_event(event)
        print(dispatcher.format_for_pager(event))
    report = dispatcher.generate_markdown_summary("post_race_summary.md")
    print(f"Wrote {len(dispatcher.events)} events to {report}")
