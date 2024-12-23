import paho.mqtt.client as mqtt  
import pymysql  
from datetime import datetime, timedelta  
import json  
import time  
import logging  

# Konfigurasi logging  
logging.basicConfig(level=logging.INFO)  

# Konfigurasi MQTT  
BROKER = "mqtt.my.id"  
TOPIC = "polindra/matkuliot/actuator/kel4TI2C"  

# Konfigurasi Database  
DB_CONFIG = {  
    'host': 'localhost',  
    'user': 'root',  
    'password': '',  
    'database': 'big_project_proyek2_app'  
}  

# Constant for PLN Costs  
PLN_COST_PER_KWH = 1500  # Cost per KWh in Rupiah  

# Fungsi untuk menyimpan data ke database  
def save_to_database(data):  
    connection = pymysql.connect(**DB_CONFIG)  
    try:  
        cursor = connection.cursor()  
        query = """  
        INSERT INTO sensor_data (timestamp, voltage, current, power, energy, frequency, power_factor)  
        VALUES (%s, %s, %s, %s, %s, %s, %s)  
        """  
        cursor.execute(query, (  
            data['timestamp'],  
            data['voltage'],  
            data['current'],  
            data['power'],  
            data['energy'],  
            data['frequency'],  
            data['pf']  
        ))  
        connection.commit()  
    finally:  
        connection.close()  

# Fungsi untuk menghitung biaya listrik  
def calculate_cost(power_data, duration_hours):  
    total_energy_kwh = (sum(power_data) / 1000) * duration_hours  # Convert to KWh  
    total_cost = total_energy_kwh * PLN_COST_PER_KWH  
    return total_cost  

# Fungsi untuk menyimpan biaya agregat  
def save_aggregate_cost(cost, period_type, timestamp):  
    connection = pymysql.connect(**DB_CONFIG)  
    try:  
        cursor = connection.cursor()  
        query = """  
        INSERT INTO cost_agregate (period_type, timestamp, total_cost)  
        VALUES (%s, %s, %s)  
        ON DUPLICATE KEY UPDATE total_cost=VALUES(total_cost)  
        """  
        cursor.execute(query, (period_type, timestamp, cost))  
        connection.commit()  
    finally:  
        connection.close()  

# Callback saat menerima pesan MQTT  
def on_message(client, userdata, msg):  
    try:  
        payload = json.loads(msg.payload.decode())  
        logging.info("Data diterima: %s", payload)  

        payload['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
        save_to_database(payload)  

    except json.JSONDecodeError as e:  
        logging.error(f"Error decoding JSON: {e}")  
    except Exception as e:  
        logging.error(f"Error processing message: {e}")  

# Fungsi untuk menghitung dan mengagregasi data  
def aggregate_data():  
    while True:  
        time.sleep(3)  # Menunggu 3 detik  

        connection = pymysql.connect(**DB_CONFIG)  
        try:  
            cursor = connection.cursor()  
            current_time = datetime.now()  
            three_seconds_ago = current_time - timedelta(seconds=3)  

            # Ambil data untuk 3 detik terakhir  
            cursor.execute("""  
            SELECT power FROM sensor_data WHERE timestamp >= %s  
            """, (three_seconds_ago,))  
            power_data = cursor.fetchall()  

            if power_data:  
                power_data = [p[0] for p in power_data]  

                # Hitung biaya per menit  
                minute_cost = calculate_cost(power_data, 1 / 60)  # Dalam 1 menit  
                save_aggregate_cost(minute_cost, 'minute', current_time.replace(second=0, microsecond=0))  

                # Hitung biaya per jam  
                # Ambil data untuk 1 jam terakhir  
                one_hour_ago = current_time - timedelta(hours=1)  
                cursor.execute("""  
                SELECT power FROM sensor_data WHERE timestamp >= %s  
                """, (one_hour_ago,))  
                hourly_power_data = cursor.fetchall()  
                hourly_power_data = [p[0] for p in hourly_power_data]  
                hour_cost = calculate_cost(hourly_power_data, 1)  # Dalam 1 jam  
                save_aggregate_cost(hour_cost, 'hour', current_time.replace(minute=0, second=0, microsecond=0))  

                # Hitung biaya per hari  
                # Ambil data untuk 1 hari terakhir  
                one_day_ago = current_time - timedelta(days=1)  
                cursor.execute("""  
                SELECT power FROM sensor_data WHERE timestamp >= %s  
                """, (one_day_ago,))  
                daily_power_data = cursor.fetchall()  
                daily_power_data = [p[0] for p in daily_power_data]  
                day_cost = calculate_cost(daily_power_data, 24)  # Dalam 24 jam  
                save_aggregate_cost(day_cost, 'day', current_time.replace(hour=0, minute=0, second=0, microsecond=0))  

                # Hitung biaya per bulan  
                # Ambil data untuk 1 bulan terakhir  
                one_month_ago = current_time - timedelta(days=30)  
                cursor.execute("""  
                SELECT power FROM sensor_data WHERE timestamp >= %s  
                """, (one_month_ago,))  
                monthly_power_data = cursor.fetchall()  
                monthly_power_data = [p[0] for p in monthly_power_data]  
                month_cost = calculate_cost(monthly_power_data, 24 * 30)  # Asumsi 30 hari  
                save_aggregate_cost(month_cost, 'month', current_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0))  

                # Hapus data perdetik untuk yang sudah dipakai  
                cursor.execute("""  
                DELETE FROM sensor_data WHERE timestamp < %s  
                """, (three_seconds_ago,))  
                connection.commit()  
        
        except Exception as e:  
            logging.error(f"Error aggregating data: {e}")  
        finally:  
            connection.close() 

# Koneksi ke MQTT  
client = mqtt.Client()  
client.on_message = on_message  
client.connect(BROKER, 1883, 60)  
client.subscribe(TOPIC)  

# Mulai loop data agregasi dalam thread terpisah  
import threading  
threading.Thread(target=aggregate_data, daemon=True).start()  

# Mulai loop MQTT  
logging.info("Menunggu data...")  
client.loop_forever()