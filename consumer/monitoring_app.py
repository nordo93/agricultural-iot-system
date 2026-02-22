#!/usr/bin/env python3
"""
Consumer: Applicazione di Monitoraggio
Riceve tutti gli eventi e li mostra in tempo reale
"""

import pika
import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *

class MonitoringApp:
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
                    client_properties={'connection_name': 'MonitoringApp'}
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
            
            # Dichiara e collega la queue
            queue_name = QUEUES['monitoring']['name']
            binding_key = QUEUES['monitoring']['binding_key']
            
            print(f"📋 Dichiarazione queue: {queue_name}")
            self.channel.queue_declare(
                queue=queue_name,
                durable=True
            )
            print("✅ Queue dichiarata")
            
            print(f"🔗 Binding: {queue_name} → {EXCHANGE_NAME} (key: {binding_key})")
            self.channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=queue_name,
                routing_key=binding_key
            )
            print("✅ Queue bindato")
            
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def callback(self, ch, method, properties, body):
        """Processa i messaggi ricevuti"""
        try:
            message = json.loads(body.decode())
            timestamp = message.get('timestamp', '')
            device = message.get('device', '')
            
            print(f"✨ Messaggio ricevuto da {device} con routing key: {method.routing_key}")
            
            # Formatta output in base al tipo di dispositivo
            if device == 'solar_panel':
                self.print_solar(message)
            elif device == 'irrigation':
                self.print_irrigation(message)
            elif device == 'alarm_system':
                self.print_alarm(message)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"❌ Errore processing: {e}")
            import traceback
            traceback.print_exc()
    
    def print_solar(self, msg):
        print(f"���️ PANNELLO {msg['panel_id']} | {msg['power_watts']:4d}W @ {msg['temperature']:2d}°C | {msg['status']}")
    
    def print_irrigation(self, msg):
        status = "💧 ON " if msg['pump_status'] == 'on' else "⏸️ OFF"
        print(f"{status} | ZONA {msg['zone_id']} | Umidità: {msg['soil_moisture']:3d}% | {msg['status']}")
    
    def print_alarm(self, msg):
        emoji = {'low': '⚠️', 'medium': '⚡', 'high': '🚨', 'critical': '🔴'}
        icon = emoji.get(msg['severity'], '❓')
        print(f"{icon} ALLARME [{msg['severity'].upper()}] {msg['description']}")
    
    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        queue_name = QUEUES['monitoring']['name']
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.callback
        )
        
        print("=" * 70)
        print("📊 APPLICAZIONE DI MONITORAGGIO")
        print("=" * 70)
        print(f"⏳ In ascolto su queue: {queue_name}")
        print("⏳ In attesa di eventi... (Premi Ctrl+C per uscire)\n")
        
        self.channel.start_consuming()
    
    def close(self):
        if self.channel and not self.channel.is_closed:
            self.channel.stop_consuming()
        if self.connection and not self.connection.is_closed:
            self.connection.close()

def main():
    app = MonitoringApp()
    app.connect()
    
    try:
        app.start_consuming()
    except KeyboardInterrupt:
        print("\n\n👋 Monitoraggio fermato")
        app.close()

if __name__ == "__main__":
    main()