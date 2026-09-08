# F1 Pitwall Pager

A hybrid Python and Embedded C++ system that analyzes Formula 1 timing data and converts statistically significant race events into real-time alerts on a physical Arduino-based pager.

## What it does

* **Strategy Alerts** — detects potential undercut/pit windows based on relative race pace
* **Tyre Degradation** — identifies abnormal tyre performance relative to drivers on the same compound
* **DNF Detection** — detects significant race events and driver retirements
* **Real-Time Hardware Output** — displays actionable alerts on a 1602 LCD and uses a buzzer for notification
* **Race Replay** — processes historical F1 sessions for testing and validation

## How it works

```text
FastF1
  ↓
Python Strategy Engine
  ↓
Event Detection & Debouncing
  ↓
Serial Communication
  ↓
Arduino UNO
  ↓
1602 LCD + Buzzer
```

## Tech stack

Python · FastF1 · pandas · NumPy · Arduino C++ · Serial · Wokwi

## Purpose

The project explores how real Formula 1 timing data can be transformed into concise, actionable race-engineering alerts using statistical analysis and embedded hardware.

## Running locally

```bash
pip install -r requirements.txt
python main.py
```

## Hardware

* Arduino UNO
* 1602 LCD with I2C
* Piezo buzzer


## Serial protocol

The hardware interface can also be tested using Wokwi.
Example: `HIGH_DEG|VER: HIGH DEG|+0.15s/lap L16`

## Running in Wokwi

Wokwi needs a **compiled** firmware, not the raw `.ino`:

\`\`\`bash
arduino-cli core install arduino:avr
arduino-cli lib install "LiquidCrystal I2C"
arduino-cli compile --fqbn arduino:avr:uno --output-dir build sketch.ino
\`\`\`

Then start the simulation and, in another terminal:

\`\`\`bash
python serial_bridge.py
\`\`\`

**Status:** software and Wokwi circuit simulation are working and verified. The physical build is in progress — components ordered, assembly pending delivery.
