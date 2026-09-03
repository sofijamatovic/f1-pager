#include <Wire.h>
#include <LiquidCrystal_I2C.h>

// Dynamic I2C address (0x27 or 0x3F)
LiquidCrystal_I2C lcd(0x27, 16, 2);

#define BUZZER_PIN 8
#define DISPLAY_HOLD_MS 3500

struct PagerAlert {
  char type[4];
  char line1[17];
  char line2[17];
};

#define QUEUE_SIZE 5
PagerAlert alertQueue[QUEUE_SIZE];
uint8_t queueHead = 0;
uint8_t queueTail = 0;
uint8_t queueCount = 0;

unsigned long alertStartTime = 0;
bool isDisplayingAlert = false;
String serialBuffer = "";

void setup() {
  Serial.begin(115200);
  pinMode(BUZZER_PIN, OUTPUT);

  Wire.begin();
  lcd.init();
  lcd.backlight();
  lcd.clear();
  
  showStandbyScreen();
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
    } else if (c != '\r') {
      serialBuffer += c;
    }
  }
}

void parseAndEnqueue(String rawMessage) {
  rawMessage.trim();
  if (rawMessage.length() == 0) return;

  int firstPipe = rawMessage.indexOf('|');
  int secondPipe = rawMessage.indexOf('|', firstPipe + 1);

  if (firstPipe == -1 || secondPipe == -1) return;

  if (queueCount < QUEUE_SIZE) {
    PagerAlert alert;
    
    String typeStr = rawMessage.substring(0, firstPipe);
    String l1Str   = rawMessage.substring(firstPipe + 1, secondPipe);
    String l2Str   = rawMessage.substring(secondPipe + 1);

    typeStr.toCharArray(alert.type, sizeof(alert.type));
    
    snprintf(alert.line1, sizeof(alert.line1), "%-16s", l1Str.c_str());
    snprintf(alert.line2, sizeof(alert.line2), "%-16s", l2Str.c_str());

    alertQueue[queueTail] = alert;
    queueTail = (queueTail + 1) % QUEUE_SIZE;
    queueCount++;
  }
}

void manageDisplayQueue() {
  unsigned long currentMillis = millis();

  if (isDisplayingAlert) {
    if (currentMillis - alertStartTime >= DISPLAY_HOLD_MS) {
      isDisplayingAlert = false;
      if (queueCount == 0) {
        showStandbyScreen();
      }
    }
  }

  if (!isDisplayingAlert && queueCount > 0) {
    PagerAlert currentAlert = alertQueue[queueHead];
    queueHead = (queueHead + 1) % QUEUE_SIZE;
    queueCount--;

    renderAlert(currentAlert);
    triggerBuzzer(currentAlert.type);

    alertStartTime = currentMillis;
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

void triggerBuzzer(const char* type) {
  if (strcmp(type, "DNF") == 0) {
    for (int i = 0; i < 3; i++) {
      tone(BUZZER_PIN, 2200, 80);
      delay(100);
    }
  } else if (strcmp(type, "PIT") == 0) {
    tone(BUZZER_PIN, 1500, 120);
    delay(160);
    tone(BUZZER_PIN, 1500, 120);
  } else {
    tone(BUZZER_PIN, 900, 100);
  }
}

void showStandbyScreen() {
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print(F("F1 PITWALL PAGER"));
  lcd.setCursor(0, 1);
  lcd.print(F("REPLAY MODE IDLE"));
}