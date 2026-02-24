#!/usr/bin/env python3
"""
Consumer: Applicazione di Monitoraggio
Riceve tutti gli eventi e li mostra in tempo reale
"""

import pika
import json
import sys
import os
import threading       
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *
from validator import MessageValidator  # ← IMPORT

# ✅ AGGIUNGI: Lock per thread safety
stats_lock = threading.Lock()

class MonitoringApp:
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
    
    def connect(self):
        """Connessione con supporto per Dead Letter Queue"""
        try:
            self.connection = pika.BlockingConnection(...)
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
            print("✅ Connesso a RabbitMQ con Dead Letter Exchange")
            return True
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
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
                print(f"   Data: {json.dumps(message)}")
                
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
            
            # ✅ STEP 3: ACK il messaggio (successo)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            print(f"✅ [MONITORING] Messaggio elaborato: {device}")
        
        except json.JSONDecodeError:
            # Messaggio non è JSON valido
            print(f"❌ [MONITORING] JSON non valido: {body}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        except Exception as e:
            print(f"❌ [MONITORING] Errore processing: {e}")
            import traceback
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
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