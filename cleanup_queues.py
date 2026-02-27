#!/usr/bin/env python3
"""
Elimina tutte le queue per ricominciare da zero
"""

import pika
from config import *

def cleanup():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
        )
        channel = connection.channel()
        
        # Elimina le queue
        queues_to_delete = [
            'monitoring_queue',
            'logger_queue',
            'stats_queue',
            'web_dashboard_queue',
            'dead_letter_queue'
        ]
        
        for queue_name in queues_to_delete:
            try:
                channel.queue_delete(queue=queue_name)
                print(f"✅ Eliminata queue: {queue_name}")
            except Exception as e:
                print(f"⚠️ Queue non trovata: {queue_name}")
        
        connection.close()
        print("\n✅ Cleanup completato!")
    
    except Exception as e:
        print(f"❌ Errore: {e}")
        print("Ricordati prima di usare i comandi nel file cleanup.txt")

if __name__ == '__main__':
    cleanup()