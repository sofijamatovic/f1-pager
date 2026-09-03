"""Thread-safe outbound serial transport for the Pitwall Pager Arduino Uno."""
import serial

# Umesto 'COM3', koristimo URL Wokwi virtuelnog porta:
SERIAL_PORT = "rfc2217://localhost:4000"
BAUD_RATE = 115200

try:
    ser = serial.serial_for_url(SERIAL_PORT, baudrate=BAUD_RATE, timeout=1)
    print("✅ Upešno povezan Python sa Wokwi simulatorom na portu 4000!")
except Exception as e:
    print(f"❌ Greška pri povezivanju sa Wokwi-jem: {e}")
from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from time import sleep
from typing import Any, Protocol

import pandas as pd

from strategy_engine import (
    CompetitorObserver,
    EventDispatcher,
    LiveTelemetrySimulator,
    StrategyConfig,
)

LOGGER = logging.getLogger(__name__)
_STOP = object()


class _WritableSerial(Protocol):
    is_open: bool

    def write(self, data: bytes) -> int: ...

    def close(self) -> None: ...


@dataclass(slots=True)
class MockSerial:
    """Console-backed serial replacement used when hardware is unavailable."""

    port: str
    is_open: bool = True

    def write(self, data: bytes) -> int:
        packet = data.decode("ascii", errors="replace").rstrip("\n")
        first, separator, second = packet.partition("|")
        if not separator:
            second = ""
        print("+----------------+")
        print(f"|{first[:16]:<16}|")
        print(f"|{second[:16]:<16}|")
        print("+----------------+")
        return len(data)

    def close(self) -> None:
        self.is_open = False


class SerialPagerBridge:
    """Queue LCD notifications so telemetry analysis never waits for I/O.

    The Arduino protocol is one ASCII packet per LCD update, for example:
    ``VER: HIGH DEG|+0.12s/lap L4\\n``.
    """

    def __init__(
        self,
        port: str,
        baud_rate: int = 9600,
        auto_connect: bool = True,
        display_duration: float = 2.5,
    ) -> None:
        if baud_rate <= 0:
            raise ValueError("baud_rate must be positive")
        if display_duration < 0:
            raise ValueError("display_duration must be non-negative")
        self.port = port
        self.baud_rate = baud_rate
        self.display_duration = display_duration
        self._messages: queue.Queue[str | object] = queue.Queue()
        self._serial: _WritableSerial | None = None
        self._worker: threading.Thread | None = None
        self._closed = False
        self._state_lock = threading.RLock()
        self.mock_mode = False
        if auto_connect:
            self.connect()

    @property
    def connected(self) -> bool:
        return self._serial is not None and self._serial.is_open

    def connect(self) -> None:
        """Open the serial port, falling back to mock transport on any failure."""
        with self._state_lock:
            if self._closed:
                raise RuntimeError("cannot connect a closed SerialPagerBridge")
            if self._worker is not None:
                return
            try:
                import serial  # pyserial is intentionally an optional dependency

                self._serial = serial.Serial(self.port, self.baud_rate, timeout=1)
                self.mock_mode = False
                LOGGER.info("Connected to Pitwall Pager on %s at %d baud", self.port, self.baud_rate)
            except Exception as exc:  # SerialException and import errors are hardware-environment dependent.
                LOGGER.warning("Serial unavailable on %s (%s); using LCD mock mode", self.port, exc)
                self._serial = MockSerial(self.port)
                self.mock_mode = True
            self._worker = threading.Thread(target=self._transmit_loop, name="pitwall-serial", daemon=True)
            self._worker.start()

    @staticmethod
    def packet_from_event(event_dict: Mapping[str, Any]) -> str:
        """Encode a pager event as the single-line Arduino serial protocol."""
        lcd_text = EventDispatcher.format_for_pager(event_dict)
        line_one, line_two = lcd_text.split("\n", maxsplit=1)
        return f"{line_one}|{line_two}\n"

    def send_event(self, event_dict: Mapping[str, Any]) -> None:
        self.send_packet(self.packet_from_event(event_dict))

    def send_packet(self, packet: str) -> None:
        """Enqueue a preformatted packet without blocking the telemetry thread."""
        if not packet.endswith("\n"):
            packet = f"{packet}\n"
        with self._state_lock:
            if self._closed:
                raise RuntimeError("cannot send through a closed SerialPagerBridge")
            if self._worker is None:
                self.connect()
        self._messages.put(packet)

    def flush(self) -> None:
        """Wait until the queue has been written to the transport."""
        self._messages.join()

    def close(self) -> None:
        """Drain outstanding messages, stop the worker, and close the port."""
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            worker = self._worker
        self._messages.join()
        if worker is not None:
            self._messages.put(_STOP)
            worker.join(timeout=max(2.0, self.display_duration + 1.0))
        if self._serial is not None:
            try:
                self._serial.close()
            except Exception as exc:
                LOGGER.warning("Could not close serial port %s: %s", self.port, exc)

    def __enter__(self) -> SerialPagerBridge:
        if self._worker is None:
            self.connect()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _transmit_loop(self) -> None:
        while True:
            item = self._messages.get()
            try:
                if item is _STOP:
                    return
                assert isinstance(item, str)
                if self._serial is None:
                    raise RuntimeError("serial transport was not initialised")
                self._serial.write(item.encode("ascii", errors="replace"))
                if self.display_duration:
                    sleep(self.display_duration)
            except Exception as exc:
                LOGGER.exception("Failed to transmit pager packet: %s", exc)
            finally:
                self._messages.task_done()


def _integration_laps() -> pd.DataFrame:
    """56 green laps: four-car field, one VER stop, and two genuine tyre cliffs."""
    records: list[dict[str, object]] = []
    offsets = {"VER": 0.00, "NOR": 0.03, "LEC": -0.02, "HAM": 0.01}
    for lap in range(1, 57):
        stint_lap = lap - 1 if lap < 29 else lap - 29
        for driver, offset in offsets.items():
            tyre_fade = stint_lap * 0.025
            # Only VER loses additional pace after the tyres fall away; the
            # peer field stays on the shared degradation curve.
            cliff = 0.15 * max(0, lap - 12) if driver == "VER" and lap < 29 else 0.0
            cliff += 0.15 * max(0, lap - 40) if driver == "VER" and lap > 29 else 0.0
            pit_in = pd.Timedelta(seconds=1) if driver == "VER" and lap == 29 else pd.NaT
            records.append({"Driver": driver, "LapNumber": lap, "LapTime": pd.Timedelta(seconds=90 + offset + tyre_fade + cliff), "LapStartTime": pd.Timedelta(seconds=(lap - 1) * 90), "IsAccurate": True, "Compound": "MEDIUM", "PitInTime": pit_in})
    return pd.DataFrame(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    simulator = LiveTelemetrySimulator(_integration_laps())
    observer = CompetitorObserver(
        StrategyConfig(
            baseline_laps=5,
            min_compound_peers=3,
            tyre_cliff_acceleration=0.08,
            undercut_loss_threshold=2.0,
        )
    )
    dispatcher = EventDispatcher()
    # Zero delay keeps the demonstration immediate; production uses 2.5 seconds.
    with SerialPagerBridge(port="COM3", display_duration=0.0) as bridge:
        for lap in simulator.stream():
            for event in observer.process_lap(lap):
                dispatcher.log_event(event)
                bridge.send_event(event)
        for event in observer.flush():
            dispatcher.log_event(event)
            bridge.send_event(event)
        bridge.flush()
    assert 5 <= len(dispatcher.events) <= 15, f"Expected 5-15 high-value events, got {len(dispatcher.events)}"
    print(f"Integration replay produced {len(dispatcher.events)} debounced alerts.")
