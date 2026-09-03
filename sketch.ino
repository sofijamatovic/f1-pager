#include <Wire.h>

#define LCD_ADDR 0x27
#define BUZZER_PIN 8

// PCF8574 -> LCD mapping used by Wokwi:
// P0 = RS
// P1 = RW
// P2 = E
// P3 = Backlight
// P4 = D4
// P5 = D5
// P6 = D6
// P7 = D7

byte backlight = 0x08;

void pcfWrite(byte value) {
  Wire.beginTransmission(LCD_ADDR);
  Wire.write(value | backlight);
  Wire.endTransmission();
}

void pulseEnable(byte value) {
  pcfWrite(value | 0x04);
  delayMicroseconds(1);
  pcfWrite(value & ~0x04);
  delayMicroseconds(50);
}

void lcdWrite4Bits(byte value) {
  pcfWrite(value);
  pulseEnable(value);
}

void lcdCommand(byte command) {
  byte high = command & 0xF0;
  byte low = (command << 4) & 0xF0;

  lcdWrite4Bits(high);
  lcdWrite4Bits(low);
}

void lcdData(byte data) {
  byte high = (data & 0xF0) | 0x01;
  byte low = ((data << 4) & 0xF0) | 0x01;

  lcdWrite4Bits(high);
  lcdWrite4Bits(low);
}

void lcdPrint(const char* text) {
  while (*text) {
    lcdData(*text);
    text++;
  }
}

void lcdSetCursor(byte col, byte row) {
  byte address;

  if (row == 0) {
    address = 0x00 + col;
  } else {
    address = 0x40 + col;
  }

  lcdCommand(0x80 | address);
}

void lcdInit() {
  delay(50);

  // Force LCD into 4-bit mode.
  lcdWrite4Bits(0x30);
  delay(5);

  lcdWrite4Bits(0x30);
  delayMicroseconds(150);

  lcdWrite4Bits(0x30);
  delayMicroseconds(150);

  lcdWrite4Bits(0x20);
  delayMicroseconds(150);

  // 4-bit, 2 lines, 5x8 font
  lcdCommand(0x28);

  // Display OFF
  lcdCommand(0x08);

  // Clear display
  lcdCommand(0x01);
  delay(2);

  // Entry mode
  lcdCommand(0x06);

  // Display ON, cursor OFF, blink OFF
  lcdCommand(0x0C);
}

bool lcdDetected() {
  Wire.beginTransmission(LCD_ADDR);
  return Wire.endTransmission() == 0;
}

void setup() {
  Serial.begin(115200);
  Wire.begin();

  pinMode(BUZZER_PIN, OUTPUT);

  Serial.println();
  Serial.println("====================");
  Serial.println("F1 PAGER LCD TEST");
  Serial.println("====================");

  if (lcdDetected()) {
    Serial.println("LCD FOUND AT 0x27");

    tone(BUZZER_PIN, 1500, 150);

    lcdInit();

    lcdSetCursor(0, 0);
    lcdPrint("F1 PAGER TEST");

    lcdSetCursor(0, 1);
    lcdPrint("LCD IS WORKING");
  } else {
    Serial.println("LCD NOT FOUND!");

    // Five low beeps = LCD not detected
    for (int i = 0; i < 5; i++) {
      tone(BUZZER_PIN, 500, 100);
      delay(150);
    }
  }
}

void loop() {
}