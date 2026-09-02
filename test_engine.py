"""
Live Telemetry Engine Test Script.
Loads a historical FastF1 session, streams laps via the strategy simulator,
and logs actionable competitor strategy alerts.
"""

import fastf1
from strategy_engine import (
    CompetitorObserver,
    EventDispatcher,
    LiveTelemetrySimulator,
    StrategyConfig,
)
import os
from pathlib import Path

# Create cache directory if it does not exist
cache_dir = Path("cache")
cache_dir.mkdir(parents=True, exist_ok=True)

# Enable FastF1 cache using the modern API
fastf1.Cache.enable_cache(str(cache_dir))


def run_historical_test() -> None:
  

    print("[INFO] Fetching race telemetry session...")
    # Load 2024 British Grand Prix Race data
    session = fastf1.get_session(2024, "Silverstone", "R")
    session.load()

    print("[INFO] Initialising Live Telemetry Simulator (Speed Multiplier: 100x)...")
    # Speed multiplier set to 100x for fast simulation
    simulator = LiveTelemetrySimulator(session.laps, speed_multiplier=100.0)

    # Custom strategy configuration for alert thresholds
    config = StrategyConfig(
    rolling_window=4,                   # Povećavamo prozor analize sa 3 na 4 kruga za stabilniji prosek
    baseline_laps=4,                    # Veći baseline sprečava lažne uzbune na početku stinta
    pace_shift_threshold=0.35,          # Reaguje tek na ozbiljan pad/skok tempa (> 0.35s)
    expected_degradation_slope=0.12,    # Reaguje samo na visoku degradaciju (> 0.12s po krugu)
    pit_drop_threshold=0.50,            # Boks prozor se otvara tek pri padu od pola sekunde
    min_laps_for_degradation=5          # Zahteva bar 5 krugova u stint-u za procenu degradacije
)

    observer = CompetitorObserver(config)
    dispatcher = EventDispatcher()

    print("\n--- PITWALL PAGER LIVE STREAM STARTED ---\n")

    event_count = 0
    for lap in simulator.stream():
        events = observer.process_lap(lap)
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

    # Generate the Markdown post-race summary report
    output_report = dispatcher.generate_markdown_summary("silverstone_2024_summary.md")
    print(f"--- SIMULATION COMPLETE ---")
    print(f"Total strategic events detected: {event_count}")
    print(f"Post-race report generated: {output_report.resolve()}")


if __name__ == "__main__":
    run_historical_test()