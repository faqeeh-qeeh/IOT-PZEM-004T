#include <WiFi.h>  
#include <PubSubClient.h>  
#include <PZEM004Tv30.h>  

// Konfigurasi Wi-Fi  
const char* ssid = "Nama Wifi";  
const char* password = "PasswordWifi";  

// Konfigurasi MQTT  
const char* mqttServer = "mqtt.my.id";  
const int mqttPort = 1883;  

// Topik untuk publish dan subscribe berbeda  
const char* publishTopic = "polindra/matkuliot/actuator/kel4TI2C";  
const char* subscribeTopic = "polindra/matkuliot/relay/kel4TI2C";  

// Pin relay  
const int relayPin = 4; // Pin GPIO untuk relay  

// Pin PZEM-004T  
#define RX2_PIN 16  
#define TX2_PIN 17  

// Inisialisasi objek  
WiFiClient espClient;  
PubSubClient client(espClient);  
PZEM004Tv30 pzem(&Serial2, RX2_PIN, TX2_PIN); // Inisialisasi dengan Serial2 dan pin RX/TX  

// Fungsi koneksi Wi-Fi  
void connectToWiFi() {  
  Serial.print("Menghubungkan ke WiFi");  
  WiFi.begin(ssid, password);  
  while (WiFi.status() != WL_CONNECTED) {  
    delay(500);  
    Serial.print(".");  
  }  
  Serial.println("\nWiFi terhubung!");  
}  

// Fungsi koneksi ke MQTT  
void connectToMQTT() {  
  while (!client.connected()) {  
    Serial.println("Menghubungkan ke MQTT...");  
    if (client.connect("ESP32Client")) {  
      Serial.println("MQTT terhubung!");  
      client.subscribe(subscribeTopic);  
    } else {  
      Serial.print("Gagal, rc=");  
      Serial.print(client.state());  
      Serial.println(" mencoba lagi dalam 5 detik...");  
      delay(5000);  
    }  
  }  
}  

// Fungsi callback untuk menangani pesan MQTT  
void mqttCallback(char* topic, byte* payload, unsigned int length) {  
  String message;  
  for (unsigned int i = 0; i < length; i++) {  
    message += (char)payload[i];  
  }  
  Serial.print("Pesan diterima [");  
  Serial.print(topic);  
  Serial.print("]: ");  
  Serial.println(message);  

  // Kontrol relay berdasarkan pesan dari topik subscribe  
  if (String(topic) == subscribeTopic) {  
    if (message == "OFF") {  
      digitalWrite(relayPin, HIGH);  
      Serial.println("Relay ON");  
    } else if (message == "ON") {  
      digitalWrite(relayPin, LOW);  
      Serial.println("Relay OFF");  
    }  
  }  
}  

void setup() {  
  // Serial untuk debug  
  Serial.begin(115200);  
  
  // Inisialisasi Serial2 untuk PZEM  
  Serial2.begin(9600, SERIAL_8N1, RX2_PIN, TX2_PIN);  

  // Inisialisasi pin relay  
  pinMode(relayPin, OUTPUT);  
  digitalWrite(relayPin, LOW); // Pastikan relay mati pada awal  

  // Koneksi Wi-Fi  
  connectToWiFi();  

  // Konfigurasi MQTT  
  client.setServer(mqttServer, mqttPort);  
  client.setCallback(mqttCallback);  
}  

void loop() {  
  // Pastikan koneksi MQTT tetap aktif  
  if (!client.connected()) {  
    connectToMQTT();  
  }  
  client.loop();  

  // Baca data dari PZEM-004T  
  float voltage = pzem.voltage();  
  float current = pzem.current();  
  float power = pzem.power();  
  float energy = pzem.energy();  
  float frequency = 50.0; // PZEM-004T tidak membaca frekuensi secara langsung  
  float pf = pzem.pf();  

  // Cek validasi pembacaan sensor  
  if (!isnan(voltage) && !isnan(current) && !isnan(power) && !isnan(pf)) {  
    // Menampilkan data di Serial Monitor  
    Serial.print("Voltage: "); Serial.print(voltage); Serial.println(" V");  
    Serial.print("Current: "); Serial.print(current); Serial.println(" A");  
    Serial.print("Power: "); Serial.print(power); Serial.println(" W");  
    Serial.print("Energy: "); Serial.print(energy); Serial.println(" Wh");  
    Serial.print("Power Factor: "); Serial.print(pf); Serial.println("\n");  

    // Mengirim data ke MQTT  
    String payload = "{\"voltage\":" + String(voltage) +  
                     ",\"current\":" + String(current) +  
                     ",\"power\":" + String(power) +  
                     ",\"energy\":" + String(energy) +  
                     ",\"frequency\":" + String(frequency) +  
                     ",\"pf\":" + String(pf) + "}";  
    client.publish(publishTopic, payload.c_str());  
  } else {  
    Serial.println("Error reading PZEM-004T");  
  }  

  // Delay 3 detik  
  delay(3000);  
}