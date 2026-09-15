#include <LiquidCrystal.h>

// Inicijalizacija LCD-a sa pinovima: RS, E, D4, D5, D6, D7
// RS -> Pin 12
// E  -> Pin 11
// D4 -> Pin 5
// D5 -> Pin 4
// D6 -> Pin 3
// D7 -> Pin 2
LiquidCrystal lcd(12, 11, 5, 4, 3, 2);

const int buzzerPin = 8;
String inputString = "";
bool stringComplete = false;

void setup() {
  Serial.begin(9600); // Proveri da li tvoj Python kod koristi ovaj baud rate
  
  // Inicijalizacija 16x2 ekrana
  lcd.begin(16, 2);
  pinMode(buzzerPin, OUTPUT);
  
  // Testni ispis prilikom paljenja
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("F1 Pager Ready");
  delay(1000); // Zadrži poruku kratko, onda briši
  lcd.clear();
}

void loop() {
  if (stringComplete) {
    inputString.trim(); // Ukloni \n sa kraja
    
    if (inputString.length() > 0) {
      processCommand(inputString);
    }
    
    // Očisti string za sledeći paket
    inputString = "";
    stringComplete = false;
  }
}

// Funkcija za obradu TYPE|LINE1|LINE2 poruke
void processCommand(String command) {
  int firstPipe = command.indexOf('|');
  int secondPipe = command.indexOf('|', firstPipe + 1);

  if (firstPipe != -1 && secondPipe != -1) {
    String type = command.substring(0, firstPipe);
    String line1 = command.substring(firstPipe + 1, secondPipe);
    String line2 = command.substring(secondPipe + 1);

    // Očisti ekran i ispiši tekst
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print(line1);
    lcd.setCursor(0, 1);
    lcd.print(line2);

    // Logika za buzzer prema tipu poruke
    if (type == "DNF") {
      // 2200Hz triple beep
      tone(buzzerPin, 2200, 200);
      delay(300);
      tone(buzzerPin, 2200, 200);
      delay(300);
      tone(buzzerPin, 2200, 200);
    } 
    else if (type == "PIT_WINDOW") {
      // 1500Hz double beep
      tone(buzzerPin, 1500, 200);
      delay(300);
      tone(buzzerPin, 1500, 200);
    } 
    else {
      // PACE_DROP, PACE_GAIN, HIGH_DEG -> 900Hz single beep
      tone(buzzerPin, 900, 300);
    }
  }
}

// Funkcija koja prikuplja karaktere sa Serial porta u pozadini
void serialEvent() {
  while (Serial.available()) {
    char inChar = (char)Serial.read();
    inputString += inChar;
    // Ako naiđemo na novi red (\n), to je kraj poruke
    if (inChar == '\n') {
      stringComplete = true;
    }
  }
}