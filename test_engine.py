"""
Live Telemetry Engine Test Script (Dynamic Anomaly Detection).
Loads a historical FastF1 session, streams laps strictly monotonically by LapNumber via
the strategy simulator, and logs actionable competitor strategy alerts.
"""

from pathlib import Path
import fastf1
import pandas as pd
from strategy_engine import (
    CompetitorObserver,
    EventDispatcher,
    LiveTelemetrySimulator,
    StrategyConfig,
)


def run_historical_test() -> None:
    # Ensure cache directory exists and enable FastF1 cache
    cache_dir = Path("cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.enable_cache(str(cache_dir))

    print("[INFO] Fetching race telemetry session...")
    # Load 2024 British Grand Prix Race data
    session = fastf1.get_session(2024, "Monaco", "R")
    session.load()

    print("[INFO] Enforcing strict monotonic LapNumber sequence for engine processing...")
    # Drop invalid lap numbers and convert LapNumber to integer
    clean_laps = session.laps.dropna(subset=["LapNumber"]).copy()
    clean_laps["LapNumber"] = clean_laps["LapNumber"].astype(int)

    # FastF1 race classification is session-level rather than lap-level.
    # Attach a retirement status only to the driver's final recorded lap so
    # the observer can exercise its one-shot DNF path without flagging every
    # earlier lap from a retired driver's stint.
    if {"Abbreviation", "Status"}.issubset(session.results.columns):
        retirement_terms = ("accident", "collision", "engine", "power unit", "hydraulics", "retired")
        retired = session.results.loc[
            session.results["Status"].fillna("").str.casefold().str.contains("|".join(retirement_terms)),
            ["Abbreviation", "Status"],
        ].set_index("Abbreviation")["Status"]
        final_lap = clean_laps.groupby("Driver")["LapNumber"].transform("max")
        clean_laps["Status"] = clean_laps["Driver"].map(retired).where(clean_laps["LapNumber"] == final_lap)

    # Sort primarily by LapNumber so that no lap N appears after lap N+1
    sorted_laps = clean_laps.sort_values(
        by=["LapNumber", "Time"], ascending=[True, True]
    ).reset_index(drop=True)

    print("[INFO] Initialising Live Telemetry Simulator (Speed Multiplier: 100x)...")
    simulator = LiveTelemetrySimulator(sorted_laps, speed_multiplier=100.0)

    # Dynamic field-relative anomaly detection configuration
    config = StrategyConfig(
        rolling_window=5,
        baseline_laps=5,
        zscore_threshold=2.6,
        min_relative_delta=0.35,
        tyre_cliff_acceleration=0.08,
        undercut_loss_threshold=2.0,
        min_compound_peers=3,
    )

    observer = CompetitorObserver(config)
    dispatcher = EventDispatcher()

    print("\n--- PITWALL PAGER LIVE STREAM STARTED ---\n")

    event_count = 0
    for lap in simulator.stream():
        # Handle single Series or DataFrame row yield
        if isinstance(lap, pd.DataFrame):
            lap_data = lap.iloc[0]
        else:
            lap_data = lap

        events = observer.process_lap(lap_data)
        for event in events:
            event_count += 1
            dispatcher.log_event(event)

            # Display formatted 16x2 LCD output
            formatted_pager_text = dispatcher.format_for_pager(event)
            print(f"[ALERT #{event_count}] Target: LCD Display (16x2)")
            print("┌────────────────┐")
            for line in formatted_pager_text.split("\n"):
                print(f"│{line:<16}│")
            print("└────────────────┘\n")

    # Flush any remaining end-of-race events
    for event in observer.flush():
        event_count += 1
        dispatcher.log_event(event)

    # Generate the Markdown post-race summary report
    output_report = dispatcher.generate_markdown_summary("silverstone_2024_summary.md")
    print("--- SIMULATION COMPLETE ---")
    print(f"Total field-relative strategic events detected: {event_count}")
    print(f"Post-race report generated: {output_report.resolve()}")


if __name__ == "__main__":
    run_historical_test()
