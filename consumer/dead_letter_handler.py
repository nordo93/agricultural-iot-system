#!/usr/bin/env python3
"""
Dead Letter Queue Handler
Gestisce i messaggi che non passano la validazione
"""

import pika
import json
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *

class DeadLetterHandler:
    """Consume messaggi invalidi dalla Dead Letter Queue"""
    
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
        self.connected = False
        
        # File di log per errori
        self.error_log_file = 'logs/dead_letter_errors.log'
        os.makedirs('logs', exist_ok=True)
    
    def connect(self):
        """Connessione a RabbitMQ"""
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
            
            # Dichiara l'exchange per dead letters
            self.channel.exchange_declare(
                exchange=DEAD_LETTER_EXCHANGE,
                exchange_type='direct',
                durable=True
            )
            
            # Dichiara la dead letter queue
            self.channel.queue_declare(
                queue=QUEUES['dead_letter']['name'],
                durable=True
            )
            
            # Collega la queue all'exchange
            self.channel.queue_bind(
                exchange=DEAD_LETTER_EXCHANGE,
                queue=QUEUES['dead_letter']['name'],
                routing_key='dead_letter'
            )
            
            self.connected = True
            print(f"✅ [DLQ] Connesso a RabbitMQ")
            print(f"   Exchange: {DEAD_LETTER_EXCHANGE}")
            print(f"   Queue: {QUEUES['dead_letter']['name']}")
            return True
        
        except Exception as e:
            print(f"❌ [DLQ] Errore connessione: {e}")
            self.connected = False
            return False
    
    def callback(self, ch, method, properties, body):
        """Processa i messaggi invalidi"""
        try:
            message = json.loads(body.decode())
            
            # Estrai informazioni dal messaggio
            device = message.get('device', 'unknown')
            timestamp = datetime.now().isoformat()
            
            # Leggi il motivo dell'errore (se disponibile nelle properties)
            error_reason = "Validazione fallita"
            if properties.headers:
                error_reason = properties.headers.get('x-error', error_reason)
            
            # Scrivi nel file di log
            self._log_error(device, message, error_reason, timestamp)
            
            # Stampa nel terminale
            print(f"❌ [DLQ] Messaggio invalido ricevuto:")
            print(f"   Device: {device}")
            print(f"   Reason: {error_reason}")
            print(f"   Data: {json.dumps(message, indent=2)}")
            print(f"   Timestamp: {timestamp}\n")
            
            # ACK il messaggio (lo rimuove dalla DLQ)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        except Exception as e:
            print(f"❌ [DLQ] Errore nel callback: {e}")
            # Nack e requeue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def _log_error(self, device, message, reason, timestamp):
        """Scrivi l'errore nel file di log"""
        try:
            with open(self.error_log_file, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*70}\n")
                f.write(f"⏰ Timestamp: {timestamp}\n")
                f.write(f"📱 Device: {device}\n")
                f.write(f"❌ Reason: {reason}\n")
                f.write(f"📊 Data: {json.dumps(message, indent=2)}\n")
                f.write(f"{'='*70}\n")
        except Exception as e:
            print(f"⚠️ [DLQ] Errore nella scrittura del log: {e}")
    
    def start_consuming(self):
        """Inizia a consumare dalla Dead Letter Queue"""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=QUEUES['dead_letter']['name'],
            on_message_callback=self.callback
        )
        
        print("=" * 70)
        print("🚨 DEAD LETTER QUEUE HANDLER")
        print("=" * 70)
        print("⏳ In ascolto da RabbitMQ Dead Letter Queue...")
        print(f"📁 Log file: {self.error_log_file}\n")
        
        self.channel.start_consuming()
    
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

if __name__ == '__main__':
    handler = DeadLetterHandler()
    if handler.connect():
        try:
            handler.start_consuming()
        except KeyboardInterrupt:
            print("\n👋 DLQ Handler fermato")
            handler.close()