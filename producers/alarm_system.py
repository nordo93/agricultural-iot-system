#!/usr/bin/env python3
"""
Producer: Sistema di Allarme
Invia notifiche di problemi e anomalie ( al momento solo random )
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

class AlarmSystemProducer:
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
    
    def send_alarm(self, alarm_type, severity, description):
        """Invia allarme"""
        message = {
            'timestamp': datetime.now().isoformat(),
            'device': 'alarm_system',
            'type': alarm_type,
            'severity': severity,  # low, medium, high, critical
            'description': description,
            'status': 'active'
        }
        
        routing_key = f'agriculture.alarm.{severity}'
        
        try:
            self.channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            emoji = {'low': '⚠️', 'medium': '⚡', 'high': '🚨', 'critical': '🔴'}
            print(f"{emoji.get(severity, '❓')} [{routing_key}] {description}")
        except Exception as e:
            print(f"❌ Errore invio: {e}")
    
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

def main():
    producer = AlarmSystemProducer()
    producer.connect()
    
    print("=" * 50)
    print("🚨 SISTEMA DI ALLARME PRODUCER")
    print("=" * 50)
    print("Monitoraggio attivo...")
    
    alarms = [
        ('temperature', 'high', 'Temperatura pannello sopra soglia'),
        ('water_leak', 'medium', 'Perdita d\'acqua rilevata'),
        ('pump_failure', 'critical', 'Guasto pompa zona 2'),
        ('frost_warning', 'medium', 'Allarme gelo'),
        ('power_loss', 'high', 'Perdita alimentazione zona 1'),
    ]
    
    try:
        counter = 0
        while True:
            # Occasionalmente invia un allarme
            if random.random() > 0.7:  # 30% probabilità
                alarm = random.choice(alarms)
                producer.send_alarm(*alarm)
            
            time.sleep(15)
            counter += 1
            print(f"--- Controllo {counter} ---")
    
    except KeyboardInterrupt:
        print("\n👋 Producer fermato")
        producer.close()

if __name__ == "__main__":
    main()