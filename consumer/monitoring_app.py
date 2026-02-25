#!/usr/bin/env python3
"""
Consumer: Monitoraggio in tempo reale
Riceve tutti gli eventi e li valida
"""

import pika
import json
import sys
import os
import threading
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import *
from validator import MessageValidator

# ✅ Lock per thread safety
stats_lock = threading.Lock()

# Statistiche locali
stats = {
    'solar_panels': defaultdict(lambda: {
        'measurements': [],
        'count': 0,
        'last_update': None
    }),
    'irrigation': defaultdict(lambda: {
        'measurements': [],
        'count': 0,
        'last_update': None
    }),
    'alarms': defaultdict(lambda: {'count': 0}),
    'total_messages': 0,
    'start_time': datetime.now()
}

class RabbitMQConsumer:
    """Consumer che monitora i messaggi in tempo reale"""
    
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
        self.connected = False
    
    def connect(self):
        """Connessione a RabbitMQ con Dead Letter Exchange"""
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
            
            # Dichiara exchange normale
            self.channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=EXCHANGE_TYPE,
                durable=True
            )
            
            # Dichiara exchange per dead letters
            self.channel.exchange_declare(
                exchange=DEAD_LETTER_EXCHANGE,
                exchange_type='direct',
                durable=True
            )
            
            # Dichiara la queue CON dead letter exchange
            self.channel.queue_declare(
                queue=QUEUES['monitoring']['name'],
                durable=True,
                arguments={
                    'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE,
                    'x-dead-letter-routing-key': 'dead_letter'
                }
            )
            
            # Collega alla queue
            self.channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=QUEUES['monitoring']['name'],
                routing_key=QUEUES['monitoring']['binding_key']
            )
            
            self.connected = True
            print("✅ [MONITORING] Connesso a RabbitMQ")
            print(f"   Exchange: {EXCHANGE_NAME}")
            print(f"   Queue: {QUEUES['monitoring']['name']}")
            print(f"   Dead Letter Exchange: {DEAD_LETTER_EXCHANGE}")
            return True
        
        except Exception as e:
            print(f"❌ [MONITORING] Errore connessione: {e}")
            import traceback
            traceback.print_exc()
            self.connected = False
            return False
    
    def callback(self, ch, method, properties, body):
        """Processa i messaggi ricevuti CON VALIDAZIONE"""
        try:
            message = json.loads(body.decode())
            device = message.get('device', '')
            
            # ✅ STEP 1: VALIDA il messaggio
            is_valid, error_msg = MessageValidator.validate(message)
            
            if not is_valid:
                # ❌ Messaggio invalido → NACK per inviare a DLQ
                print(f"❌ [MONITORING] Validazione fallita: {error_msg}")
                print(f"   Device: {device}")
                print(f"   Data: {json.dumps(message)}\n")
                
                # Nack SENZA requeue (va direttamente a DLQ)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            # ✅ STEP 2: Messaggio valido, elaboralo
            with stats_lock:
                if device == 'solar_panel':
                    self.process_solar_data(message)
                elif device == 'irrigation':
                    self.process_irrigation_data(message)
                elif device == 'alarm_system':
                    self.process_alarm_data(message)
                
                stats['total_messages'] += 1
            
            # ✅ STEP 3: ACK il messaggio (successo)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Debug
            if stats['total_messages'] % 50 == 0:
                print(f"✅ [MONITORING] Messaggi elaborati: {stats['total_messages']}")
        
        except json.JSONDecodeError as e:
            # Messaggio non è JSON valido
            print(f"❌ [MONITORING] JSON non valido: {body}")
            print(f"   Errore: {e}\n")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        except Exception as e:
            print(f"❌ [MONITORING] Errore processing: {e}")
            import traceback
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def process_solar_data(self, msg):
        """Elabora dati pannello solare"""
        panel_id = msg.get('panel_id')
        power = msg.get('power_watts', 0)
        temp = msg.get('temperature', 0)
        timestamp = datetime.fromisoformat(msg.get('timestamp'))
        
        s = stats['solar_panels'][panel_id]
        s['measurements'].append({
            'timestamp': timestamp,
            'power': power,
            'temp': temp
        })
        s['count'] += 1
        s['last_update'] = datetime.now().isoformat()
        
        # Mantieni solo ultima ora
        one_hour_ago = datetime.now() - __import__('datetime').timedelta(hours=1)
        s['measurements'] = [m for m in s['measurements'] if m['timestamp'] > one_hour_ago]
        
        # Stampa nel terminale
        print(f"✅ ☀️ [MONITORING] Pannello {panel_id}: {power}W @ {temp}°C")
    
    def process_irrigation_data(self, msg):
        """Elabora dati irrigatore"""
        zone_id = msg.get('zone_id')
        moisture = msg.get('soil_moisture', 0)
        pump_status = msg.get('pump_status', 'off')
        timestamp = datetime.fromisoformat(msg.get('timestamp'))
        
        s = stats['irrigation'][zone_id]
        s['measurements'].append({
            'timestamp': timestamp,
            'moisture': moisture,
            'pump_status': pump_status
        })
        s['count'] += 1
        s['last_update'] = datetime.now().isoformat()
        
        # Mantieni solo ultima ora
        one_hour_ago = datetime.now() - __import__('datetime').timedelta(hours=1)
        s['measurements'] = [m for m in s['measurements'] if m['timestamp'] > one_hour_ago]
        
        # Stampa nel terminale
        pump_txt = "💧 ON" if pump_status == 'on' else "⏸️ OFF"
        print(f"✅ 💧 [MONITORING] Zona {zone_id}: {moisture}% {pump_txt}")
    
    def process_alarm_data(self, msg):
        """Elabora dati allarme"""
        severity = msg.get('severity', 'unknown')
        description = msg.get('description', '')
        
        s = stats['alarms'][severity]
        s['count'] += 1
        
        # Stampa nel terminale
        emoji_map = {
            'critical': '🔴',
            'high': '🚨',
            'medium': '⚡',
            'low': '⚠️'
        }
        emoji = emoji_map.get(severity, '❓')
        
        print(f"✅ {emoji} [MONITORING] [{severity.upper()}] {description}")
    
    def start_consuming(self):
        """Inizia a consumare messaggi"""
        self.channel.basic_qos(prefetch_count=10)
        self.channel.basic_consume(
            queue=QUEUES['monitoring']['name'],
            on_message_callback=self.callback
        )
        
        print("=" * 70)
        print("📊 CONSUMER: MONITORAGGIO IN TEMPO REALE")
        print("=" * 70)
        print("⏳ In ascolto da RabbitMQ...")
        print(f"📁 Queue: {QUEUES['monitoring']['name']}")
        print(f"🔧 Dead Letter Exchange: {DEAD_LETTER_EXCHANGE}\n")
        
        self.channel.start_consuming()
    
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("✅ [MONITORING] Connessione RabbitMQ chiusa")

def main():
    consumer = RabbitMQConsumer()
    
    if not consumer.connect():
        print("❌ Impossibile connettersi a RabbitMQ. Esco.")
        return
    
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("\n\n👋 [MONITORING] Consumer fermato")
        consumer.close()

if __name__ == "__main__":
    main()