#!/usr/bin/env python3
"""
Consumer: Dashboard Statistiche
Mostra statistiche in tempo reale dei dispositivi agricoli
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
            
            # Dichiara e collega la queue
            self.channel.queue_declare(
                queue='stats_queue',
                durable=True
            )
            self.channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue='stats_queue',
                routing_key='agriculture.#'
            )
            print("✅ Connesso a RabbitMQ")
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
            sys.exit(1) 
"""

import pika
import json
import sys
import os
import time
from datetime import datetime
from collections import defaultdict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *

class StatsDashboard:
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
        
        # Statistiche
        self.stats = {
            'solar_panels': defaultdict(lambda: {'power': [], 'temp': [], 'count': 0}),
            'irrigation': defaultdict(lambda: {'moisture': [], 'pump_on': 0, 'pump_off': 0, 'count': 0}),
            'alarms': defaultdict(lambda: {'count': 0}),
            'total_messages': 0,
            'start_time': datetime.now()
        }
    
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
            
            # ✅ Usa QUEUES['stats'] dalla config
            self.channel.queue_declare(
                queue=QUEUES['stats']['name'],
                durable=True
            )
            self.channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=QUEUES['stats']['name'],
                routing_key=QUEUES['stats']['binding_key']
            )
            print("✅ Connesso a RabbitMQ")
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
            sys.exit(1)

    
    def callback(self, ch, method, properties, body):
        """Processa i messaggi e aggiorna le statistiche"""
        try:
            message = json.loads(body.decode())
            device = message.get('device', '')
            
            # Aggiorna statistiche in base al dispositivo
            if device == 'solar_panel':
                self.update_solar_stats(message)
            elif device == 'irrigation':
                self.update_irrigation_stats(message)
            elif device == 'alarm_system':
                self.update_alarm_stats(message)
            
            self.stats['total_messages'] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Mostra dashboard ogni 30 messaggi
            if self.stats['total_messages'] % 30 == 0:
                self.print_dashboard()
        
        except Exception as e:
            print(f"❌ Errore processing: {e}")
    
    def update_solar_stats(self, msg):
        """Aggiorna statistiche pannelli solari"""
        panel_id = msg.get('panel_id')
        power = msg.get('power_watts', 0)
        temp = msg.get('temperature', 0)
        
        stats = self.stats['solar_panels'][panel_id]
        stats['power'].append(power)
        stats['temp'].append(temp)
        stats['count'] += 1
        
        # Mantieni solo gli ultimi 100 valori
        if len(stats['power']) > 100:
            stats['power'] = stats['power'][-100:]
            stats['temp'] = stats['temp'][-100:]
    
    def update_irrigation_stats(self, msg):
        """Aggiorna statistiche irrigatori"""
        zone_id = msg.get('zone_id')
        moisture = msg.get('soil_moisture', 0)
        pump_status = msg.get('pump_status', 'off')
        
        stats = self.stats['irrigation'][zone_id]
        stats['moisture'].append(moisture)
        stats['count'] += 1
        
        if pump_status == 'on':
            stats['pump_on'] += 1
        else:
            stats['pump_off'] += 1
        
        # Mantieni solo gli ultimi 100 valori
        if len(stats['moisture']) > 100:
            stats['moisture'] = stats['moisture'][-100:]
    
    def update_alarm_stats(self, msg):
        """Aggiorna statistiche allarmi"""
        severity = msg.get('severity', 'unknown')
        self.stats['alarms'][severity]['count'] += 1
    
    def calculate_average(self, values):
        """Calcola media"""
        if not values:
            return 0
        return sum(values) / len(values)
    
    def calculate_min_max(self, values):
        """Calcola min e max"""
        if not values:
            return 0, 0
        return min(values), max(values)
    
    def clear_screen(self):
        """Pulisce lo schermo"""
        import os
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def print_dashboard(self):
        """Stampa il dashboard con tutte le statistiche"""
        self.clear_screen()
        
        uptime = datetime.now() - self.stats['start_time']
        uptime_str = f"{int(uptime.total_seconds() // 60)} min"
        
        print("=" * 100)
        print("📊 DASHBOARD STATISTICHE SISTEMA AGRICOLO".center(100))
        print("=" * 100)
        print(f"⏱️ Tempo di esecuzione: {uptime_str} | 📨 Messaggi totali: {self.stats['total_messages']}")
        print("=" * 100)
        
        # PANNELLI SOLARI
        print("\n☀️  PANNELLI SOLARI")
        print("-" * 100)
        
        if self.stats['solar_panels']:
            print(f"{'ID':<5} {'Potenza Media (W)':<20} {'Min (W)':<15} {'Max (W)':<15} {'Temp Media (°C)':<20} {'Misurazioni':<15}")
            print("-" * 100)
            
            for panel_id in sorted(self.stats['solar_panels'].keys()):
                stats = self.stats['solar_panels'][panel_id]
                avg_power = self.calculate_average(stats['power'])
                min_power, max_power = self.calculate_min_max(stats['power'])
                avg_temp = self.calculate_average(stats['temp'])
                
                print(f"{panel_id:<5} {avg_power:>18.1f} {min_power:>13} {max_power:>13} {avg_temp:>18.1f} {stats['count']:>15}")
        else:
            print("⏳ Nessun dato disponibile")
        
        # IRRIGATORI
        print("\n\n💧 IRRIGATORI")
        print("-" * 100)
        
        if self.stats['irrigation']:
            print(f"{'Zona':<5} {'Umidità Media (%)':<20} {'Min (%)':<15} {'Max (%)':<15} {'Pompa ON':<15} {'Pompa OFF':<15} {'Misurazioni':<15}")
            print("-" * 100)
            
            for zone_id in sorted(self.stats['irrigation'].keys()):
                stats = self.stats['irrigation'][zone_id]
                avg_moisture = self.calculate_average(stats['moisture'])
                min_moisture, max_moisture = self.calculate_min_max(stats['moisture'])
                pump_on_pct = (stats['pump_on'] / stats['count'] * 100) if stats['count'] > 0 else 0
                pump_off_pct = (stats['pump_off'] / stats['count'] * 100) if stats['count'] > 0 else 0
                
                print(f"{zone_id:<5} {avg_moisture:>18.1f} {min_moisture:>13} {max_moisture:>13} {pump_on_pct:>13.1f}% {pump_off_pct:>13.1f}% {stats['count']:>15}")
        else:
            print("⏳ Nessun dato disponibile")
        
        # ALLARMI
        print("\n\n🚨 ALLARMI")
        print("-" * 100)
        
        if self.stats['alarms']:
            print(f"{'Severità':<15} {'Conteggio':<15}")
            print("-" * 100)
            
            severity_colors = {
                'low': '⚠️ ',
                'medium': '⚡ ',
                'high': '🚨 ',
                'critical': '🔴'
            }
            
            total_alarms = sum(s['count'] for s in self.stats['alarms'].values())
            
            for severity in ['critical', 'high', 'medium', 'low']:
                if severity in self.stats['alarms']:
                    count = self.stats['alarms'][severity]['count']
                    emoji = severity_colors.get(severity, '❓')
                    print(f"{emoji} {severity.upper():<12} {count:<15}")
            
            print("-" * 100)
            print(f"{'TOTALE':<15} {total_alarms:<15}")
        else:
            print("✅ Nessun allarme rilevato")
        
        print("\n" + "=" * 100)
        print("⏳ In ascolto... Dashboard aggiornato ogni 30 messaggi (Premi Ctrl+C per uscire)".center(100))
        print("=" * 100)
    
    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue='stats_queue',
            on_message_callback=self.callback
        )
        
        print("=" * 100)
        print("📊 DASHBOARD STATISTICHE - AVVIO IN CORSO".center(100))
        print("=" * 100)
        print("⏳ Raccogliendo dati... Il dashboard verrà mostrato dopo i primi 30 messaggi\n")
        
        self.channel.start_consuming()
    
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

def main():
    dashboard = StatsDashboard()
    dashboard.connect()
    
    try:
        dashboard.start_consuming()
    except KeyboardInterrupt:
        print("\n\n👋 Dashboard fermato")
        dashboard.close()

if __name__ == "__main__":
    main()