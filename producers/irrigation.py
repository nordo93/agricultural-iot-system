#!/usr/bin/env python3
"""
Producer: Irrigatori
Invia dati su umidità del suolo e stato dell'irrigazione
"""

import pika
import json
import sys
import time
import os
from datetime import datetime
import random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *

class IrrigationProducer:
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
    
    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=self.credentials,
                    connection_attempts=5,
                    retry_delay=2
                )
            )
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=EXCHANGE_TYPE,
                durable=True
            )
            print("✅ Connesso a RabbitMQ")
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
            sys.exit(1)
    
    def send_irrigation_data(self, zone_id, soil_moisture, pump_status):
        """Invia dati irrigazione"""
        message = {
            'timestamp': datetime.now().isoformat(),
            'device': 'irrigation',
            'zone_id': zone_id,
            'soil_moisture': soil_moisture,
            'pump_status': pump_status,
            'status': 'ok'
        }
        
        routing_key = f'agriculture.irrigation.zone_{zone_id}'
        
        try:
            self.channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            status_symbol = "💧" if pump_status == "on" else "⏸️"
            print(f"{status_symbol} [{routing_key}] Umidità: {soil_moisture}%")
        except Exception as e:
            print(f"❌ Errore invio: {e}")
    
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

def main():
    producer = IrrigationProducer()
    producer.connect()
    
    print("=" * 50)
    print("💧 IRRIGATORI PRODUCER")
    print("=" * 50)
    print("Invio dati ogni 8 secondi...")
    
    try:
        counter = 0
        while True:
            # Simula 4 zone di irrigazione
            for zone_id in range(1, 5):
                moisture = random.randint(20, 80)
                pump = "on" if moisture < 40 else "off"
                producer.send_irrigation_data(zone_id, moisture, pump)
                time.sleep(1)
            
            counter += 1
            time.sleep(8)
            print(f"--- Ciclo {counter} ---")
    
    except KeyboardInterrupt:
        print("\n👋 Producer fermato")
        producer.close()

if __name__ == "__main__":
    main()