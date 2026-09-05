"""
Serial bridge between the Python F1 strategy engine and the Arduino/Wokwi pager.

Protocol sent to Arduino:

    TYPE|LINE1|LINE2\n

Example:

    HIGH_DEG|VER: HIGH DEG|+0.15s/lap L16
"""

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

SERIAL_PORT = "rfc2217://localhost:4000?timeout=10"
BAUD_RATE = 115200


class _WritableSerial(Protocol):
    is_open: bool

    def write(self, data: bytes) -> int:
        ...

    def close(self) -> None:
        ...


@dataclass(slots=True)
class MockSerial:
    """
    Console fallback when Wokwi/Arduino is unavailable.
    """

    port: str
    is_open: bool = True

    def write(self, data: bytes) -> int:
        packet = data.decode(
            "ascii",
            errors="replace",
        ).rstrip("\n")

        parts = packet.split("|", 2)

        if len(parts) == 3:
            event_type, line1, line2 = parts

            print()
            print(
                f"[{event_type}]"
            )

        elif len(parts) == 2:
            line1, line2 = parts

        else:
            line1 = packet
            line2 = ""

        print("+----------------+")
        print(
            f"|{line1[:16]:<16}|"
        )
        print(
            f"|{line2[:16]:<16}|"
        )
        print("+----------------+")

        return len(data)

    def close(self) -> None:
        self.is_open = False


class SerialPagerBridge:
    """
    Queued, non-blocking serial output for the F1 pager.

    Python:
        TYPE|LINE1|LINE2\\n

    Arduino:
        parses the three fields and displays them
        on the 16x2 LCD.
    """

    def __init__(
        self,
        port: str = SERIAL_PORT,
        baud_rate: int = BAUD_RATE,
        auto_connect: bool = True,
        display_duration: float = 0.0,
    ) -> None:

        if baud_rate <= 0:
            raise ValueError(
                "baud_rate must be positive"
            )

        if display_duration < 0:
            raise ValueError(
                "display_duration cannot be negative"
            )

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
        return (
            self._serial is not None
            and self._serial.is_open
        )


    def connect(self) -> None:
        """
        Connect to Wokwi using RFC2217.
        Falls back to MockSerial if unavailable.
        """

        with self._state_lock:

            if self._closed:
                raise RuntimeError(
                    "Cannot connect a closed bridge."
                )

            if self._worker is not None:
                return

            try:

                import serial

                self._serial = serial.serial_for_url(
                    self.port,
                    baudrate=self.baud_rate,
                    timeout=1,
                )

                self.mock_mode = False

                LOGGER.info(
                    "Connected to Pitwall Pager on %s "
                    "at %d baud",
                    self.port,
                    self.baud_rate,
                )

            except Exception as exc:

                LOGGER.warning(
                    "Serial unavailable on %s (%s); "
                    "using LCD mock mode",
                    self.port,
                    exc,
                )

                self._serial = MockSerial(
                    self.port
                )

                self.mock_mode = True

            self._worker = threading.Thread(
                target=self._transmit_loop,
                name="pitwall-serial",
                daemon=True,
            )

            self._worker.start()


    @staticmethod
    def packet_from_event(
        event_dict: Mapping[str, Any]
    ) -> str:
        """
        Convert a strategy event to:

            TYPE|LINE1|LINE2\\n
        """

        lcd_text = (
            EventDispatcher
            .format_for_pager(event_dict)
        )

        lines = lcd_text.split(
            "\n",
            maxsplit=1,
        )

        if len(lines) != 2:
            raise ValueError(
                "Pager formatter must return "
                "exactly two LCD lines."
            )

        line_one, line_two = lines

        event_type = str(
            event_dict["event_type"]
        )

        return (
            f"{event_type}|"
            f"{line_one[:16]}|"
            f"{line_two[:16]}\n"
        )


    def send_event(
        self,
        event_dict: Mapping[str, Any],
    ) -> None:
        """
        Send a strategy event.
        """

        self.send_packet(
            self.packet_from_event(
                event_dict
            )
        )


    def send_packet(
        self,
        packet: str,
    ) -> None:
        """
        Queue a raw serial packet.
        """

        if not packet.endswith("\n"):
            packet = packet + "\n"

        with self._state_lock:

            if self._closed:
                raise RuntimeError(
                    "Cannot send through a closed bridge."
                )

            if self._worker is None:
                self.connect()

        self._messages.put(packet)


    def flush(self) -> None:
        """
        Wait until every queued packet has been transmitted.
        """

        self._messages.join()


    def close(self) -> None:
        """
        Drain the queue and close the serial connection.
        """

        with self._state_lock:

            if self._closed:
                return

            self._closed = True

            worker = self._worker

        self._messages.join()

        if worker is not None:

            self._messages.put(_STOP)

            worker.join(timeout=3.0)

        if self._serial is not None:

            try:
                self._serial.close()

            except Exception as exc:

                LOGGER.warning(
                    "Could not close serial port %s: %s",
                    self.port,
                    exc,
                )


    def __enter__(
        self,
    ) -> "SerialPagerBridge":

        if self._worker is None:
            self.connect()

        return self


    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:

        self.close()


    def _transmit_loop(self) -> None:
        """
        Background thread which performs serial writes.
        """

        while True:

            item = self._messages.get()

            try:

                if item is _STOP:
                    return

                assert isinstance(
                    item,
                    str,
                )

                if self._serial is None:
                    raise RuntimeError(
                        "Serial transport not initialized."
                    )

                self._serial.write(
                    item.encode(
                        "ascii",
                        errors="replace",
                    )
                )

                if self.display_duration > 0:
                    sleep(
                        self.display_duration
                    )

            except Exception as exc:

                LOGGER.exception(
                    "Failed to transmit pager packet: %s",
                    exc,
                )

            finally:

                self._messages.task_done()


def _integration_laps() -> pd.DataFrame:
    """
    Generate deterministic replay telemetry.

    Produces a small synthetic four-driver field
    containing:

    - VER pit stop
    - tyre degradation events
    - pit-window events
    """

    records: list[dict[str, object]] = []

    offsets = {
        "VER": 0.00,
        "NOR": 0.03,
        "LEC": -0.02,
        "HAM": 0.01,
    }

    for lap in range(1, 57):

        stint_lap = (
            lap - 1
            if lap < 29
            else lap - 29
        )

        for driver, offset in offsets.items():

            tyre_fade = (
                stint_lap * 0.025
            )

            cliff = 0.0

            if (
                driver == "VER"
                and lap < 29
                and lap > 12
            ):
                cliff = (
                    0.15
                    * (lap - 12)
                )

            if (
                driver == "VER"
                and lap > 40
            ):
                cliff += (
                    0.15
                    * (lap - 40)
                )

            pit_in = pd.NaT

            if (
                driver == "VER"
                and lap == 29
            ):
                pit_in = pd.Timedelta(
                    seconds=1
                )

            records.append(
                {
                    "Driver": driver,

                    "LapNumber": lap,

                    "LapTime": pd.Timedelta(
                        seconds=(
                            90
                            + offset
                            + tyre_fade
                            + cliff
                        )
                    ),

                    "LapStartTime":
                        pd.Timedelta(
                            seconds=(
                                (lap - 1)
                                * 90
                            )
                        ),

                    "IsAccurate": True,

                    "Compound": "MEDIUM",

                    "PitInTime": pit_in,
                }
            )

    return pd.DataFrame(records)


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    simulator = LiveTelemetrySimulator(
        _integration_laps()
    )

    observer = CompetitorObserver(
        StrategyConfig(
            baseline_laps=5,
            min_compound_peers=3,
            tyre_cliff_acceleration=0.08,
            undercut_loss_threshold=2.0,
        )
    )

    dispatcher = EventDispatcher()

    with SerialPagerBridge(
        port=SERIAL_PORT,
        baud_rate=BAUD_RATE,
        display_duration=0.0,
    ) as bridge:

        for lap in simulator.stream():

            for event in observer.process_lap(
                lap
            ):

                dispatcher.log_event(
                    event
                )

                bridge.send_event(
                    event
                )

        for event in observer.flush():

            dispatcher.log_event(
                event
            )

            bridge.send_event(
                event
            )

        bridge.flush()

    print()
    print(
        "Integration replay produced "
        f"{len(dispatcher.events)} "
        "debounced alerts."
    )


if __name__ == "__main__":
    main()