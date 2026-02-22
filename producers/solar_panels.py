#!/usr/bin/env python3
"""
Producer: Pannelli Solari
Invia dati sulla generazione di energia solare
"""

import pika
import json
import sys
import os
import time
from datetime import datetime
import random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *

class SolarPanelsProducer:
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
    

    def connect(self):
        try:
            print(f"🔌 Connessione a RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=self.credentials,
                    connection_attempts=5,
                    retry_delay=2,
                    client_properties={'connection_name': 'SolarPanelsProducer'}
                )
            )
            print("✅ Connesso a RabbitMQ")
            
            self.channel = self.connection.channel()
            print(f"📡 Exchange: {EXCHANGE_NAME} (type: {EXCHANGE_TYPE})")
            
            self.channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=EXCHANGE_TYPE,
                durable=True
            )
            print("✅ Exchange dichiarato")
        
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def send_solar_data(self, panel_id, power_watts, temperature):
        """Invia dati pannelli solari"""
        message = {
            'timestamp': datetime.now().isoformat(),
            'device': 'solar_panel',
            'panel_id': panel_id,
            'power_watts': power_watts,
            'temperature': temperature,
            'status': 'ok' if power_watts > 100 else 'low_power'
        }
        
        routing_key = f'agriculture.solar_panels.{panel_id}'
        
        try:
            print(f"📤 Invio a exchange: {EXCHANGE_NAME}")
            print(f"   Routing key: {routing_key}")
            print(f"   Messaggio: {json.dumps(message)}")
            
            self.channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"✅ ☀️ [{routing_key}] {power_watts}W @ {temperature}°C")
        except Exception as e:
            print(f"❌ Errore invio: {e}")
            import traceback
            traceback.print_exc()

def main():
    producer = SolarPanelsProducer()
    producer.connect()
    
    print("=" * 50)
    print("☀��� PANNELLI SOLARI PRODUCER")
    print("=" * 50)
    print("Invio dati ogni 10 secondi...")
    
    try:
        counter = 0
        while True:
            # Simula 3 pannelli solari
            for panel_id in range(1, 4):
                power = random.randint(50, 1000)
                temp = random.randint(25, 60)
                producer.send_solar_data(panel_id, power, temp)
                time.sleep(1)
            
            counter += 1
            time.sleep(10)
            print(f"--- Ciclo {counter} ---")
    
    except KeyboardInterrupt:
        print("\n👋 Producer fermato")
        producer.close()

if __name__ == "__main__":
    main()