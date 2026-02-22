#!/usr/bin/env python3
"""
Consumer: Logger
Salva tutti gli eventi su file per tracciamento storico
"""

import pika
import json
import sys
import os
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *

class LoggerApp:
    def __init__(self, log_file='agricultural_events.log'):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
        self.log_file = log_file
    
    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=self.credentials,
                    connection_attempts=5,
                    retry_delay=2,
                    client_properties={'connection_name': 'logger_app'}  # nome del canale channel
                )
            )
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=EXCHANGE_TYPE,
                durable=True
            )
            
            # Dichiara e collega la queue
            self.channel.queue_declare(
                queue=QUEUES['logger']['name'],
                durable=True
            )
            self.channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=QUEUES['logger']['name'],
                routing_key=QUEUES['logger']['binding_key']
            )
            print("✅ Connesso a RabbitMQ")
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
            sys.exit(1)
    
    def callback(self, ch, method, properties, body):
        """Log del messaggio su file"""
        try:
            message = json.loads(body.decode())
            
            # Scrivi su file in formato JSON formattato
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(message, indent=2) + '\n')
                f.write('-' * 80 + '\n')
            
            print(f"📝 Evento loggato | Device: {message.get('device')} | {method.routing_key}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"❌ Errore logging: {e}")
    
    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=QUEUES['logger']['name'],
            on_message_callback=self.callback
        )
        
        print("=" * 70)
        print(f"📁 LOGGER - Salvataggio su: {self.log_file}")
        print("=" * 70)
        print("⏳ In attesa di eventi... (Premi Ctrl+C per uscire)\n")
        
        self.channel.start_consuming()
    
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

def main():
    app = LoggerApp()
    app.connect()
    
    try:
        app.start_consuming()
    except KeyboardInterrupt:
        print("\n\n👋 Logger fermato")
        app.close()

if __name__ == "__main__":
    main()