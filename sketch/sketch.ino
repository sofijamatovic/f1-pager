#include <Wire.h>
#include <LiquidCrystal_I2C.h>

#define LCD_ADDRESS 0x27
#define LCD_COLUMNS 16
#define LCD_ROWS 2

#define BUZZER_PIN 8

#define DISPLAY_HOLD_MS 3500
#define QUEUE_SIZE 5

LiquidCrystal_I2C lcd(LCD_ADDRESS, LCD_COLUMNS, LCD_ROWS);

struct PagerAlert {
  char type[16];
  char line1[17];
  char line2[17];
};

PagerAlert alertQueue[QUEUE_SIZE];

uint8_t queueHead = 0;
uint8_t queueTail = 0;
uint8_t queueCount = 0;

bool isDisplayingAlert = false;
unsigned long alertStartTime = 0;

String serialBuffer = "";


void setup() {
  Serial.begin(115200);

  pinMode(BUZZER_PIN, OUTPUT);
  digitalWrite(BUZZER_PIN, LOW);

  Wire.begin();

  lcd.init();
  lcd.backlight();
  lcd.clear();

  showStandbyScreen();

  // Startup beep so we know the Arduino has initialized.
  tone(BUZZER_PIN, 1000, 150);
}


void loop() {
  readSerialData();
  manageDisplayQueue();
}


void readSerialData() {
  while (Serial.available() > 0) {
    char c = Serial.read();

    if (c == '\n') {
      parseAndEnqueue(serialBuffer);
      serialBuffer = "";
    }
    else if (c != '\r') {
      serialBuffer += c;
    }

    // Prevent an accidentally huge serial message from
    // consuming memory.
    if (serialBuffer.length() > 100) {
      serialBuffer = "";
    }
  }
}


void parseAndEnqueue(String rawMessage) {
  rawMessage.trim();

  if (rawMessage.length() == 0) {
    return;
  }

  /*
    Expected format:

    TYPE|LINE1|LINE2

    Examples:

    DNF|VER: DNF|RETIRED L29
    HIGH_DEG|VER: HIGH DEG|+0.15s/lap L16
    PIT_WINDOW|VER: PIT WINDOW|+2.10s L26
  */

  int firstPipe = rawMessage.indexOf('|');

  if (firstPipe == -1) {
    return;
  }

  int secondPipe = rawMessage.indexOf('|', firstPipe + 1);

  if (secondPipe == -1) {
    return;
  }

  // Drop the oldest message if the queue is full.
  if (queueCount >= QUEUE_SIZE) {
    queueHead = (queueHead + 1) % QUEUE_SIZE;
    queueCount--;
  }

  PagerAlert alert;

  String typeStr =
    rawMessage.substring(0, firstPipe);

  String line1Str =
    rawMessage.substring(firstPipe + 1, secondPipe);

  String line2Str =
    rawMessage.substring(secondPipe + 1);

  typeStr.trim();
  line1Str.trim();
  line2Str.trim();

  // Copy event type.
  typeStr.toCharArray(
    alert.type,
    sizeof(alert.type)
  );

  // Copy LCD line 1.
  line1Str.toCharArray(
    alert.line1,
    sizeof(alert.line1)
  );

  // Copy LCD line 2.
  line2Str.toCharArray(
    alert.line2,
    sizeof(alert.line2)
  );

  alertQueue[queueTail] = alert;

  queueTail =
    (queueTail + 1) % QUEUE_SIZE;

  queueCount++;
}


void manageDisplayQueue() {
  unsigned long now = millis();

  // Finish currently displayed alert.
  if (isDisplayingAlert) {
    if (now - alertStartTime >= DISPLAY_HOLD_MS) {
      isDisplayingAlert = false;

      if (queueCount == 0) {
        showStandbyScreen();
      }
    }
  }

  // Display the next queued alert.
  if (!isDisplayingAlert && queueCount > 0) {
    PagerAlert currentAlert =
      alertQueue[queueHead];

    queueHead =
      (queueHead + 1) % QUEUE_SIZE;

    queueCount--;

    renderAlert(currentAlert);

    triggerBuzzer(currentAlert.type);

    alertStartTime = millis();
    isDisplayingAlert = true;
  }
}


void renderAlert(const PagerAlert& alert) {
  lcd.clear();

  lcd.setCursor(0, 0);
  lcd.print(alert.line1);

  lcd.setCursor(0, 1);
  lcd.print(alert.line2);
}


void showStandbyScreen() {
  lcd.clear();

  lcd.setCursor(0, 0);
  lcd.print("F1 PITWALL");

  lcd.setCursor(0, 1);
  lcd.print("PAGER READY");
}


void triggerBuzzer(const char* type) {

  // DNF = highest priority.
  if (strcmp(type, "DNF") == 0) {

    tone(BUZZER_PIN, 2200, 100);
    delay(130);

    tone(BUZZER_PIN, 2200, 100);
    delay(130);

    tone(BUZZER_PIN, 2200, 100);
  }

  // Pit strategy alert.
  else if (
    strcmp(type, "PIT") == 0 ||
    strcmp(type, "PIT_WINDOW") == 0
  ) {

    tone(BUZZER_PIN, 1500, 120);
    delay(160);

    tone(BUZZER_PIN, 1500, 120);
  }

  // All other strategy alerts.
  else {

    tone(BUZZER_PIN, 900, 100);
  }
}