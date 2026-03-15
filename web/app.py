#!/usr/bin/env python3
"""
Server Web con Flask + WebSocket
Dashboard in tempo reale per il sistema agricolo
"""

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import pika
import json
import os
import threading
import sys
import time
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import *

app = Flask(__name__, template_folder='templates', static_folder='static')
app.config['SECRET_KEY'] = 'agricultural-iot-secret-2026'
socketio = SocketIO(app, cors_allowed_origins="*")

# Statistiche globali
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

# Lock per thread safety
stats_lock = threading.Lock()

class RabbitMQConsumer:
    """Consumer che riceve messaggi da RabbitMQ"""
    
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = None
        self.channel = None
        self.connected = False
    
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
            
            # Dichiara exchange
            self.channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=EXCHANGE_TYPE,
                durable=True
            )
            
            # Dichiara queue con Dead Letter Exchange
            self.channel.queue_declare(
                queue=QUEUES['web_dashboard']['name'],
                durable=True,
                arguments={
                    'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE,
                    'x-dead-letter-routing-key': 'dead_letter'
                }
            )
            
            # Collega la queue all'exchange
            self.channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=QUEUES['web_dashboard']['name'],
                routing_key=QUEUES['web_dashboard']['binding_key']
            )
            
            self.connected = True
            print(f"✅ [WEB] Connesso a RabbitMQ")
            print(f"   Exchange: {EXCHANGE_NAME}")
            print(f"   Queue: {QUEUES['web_dashboard']['name']}")
            return True
        
        except Exception as e:
            print(f"❌ [WEB] Errore connessione: {e}")
            import traceback
            traceback.print_exc()
            self.connected = False
            return False

    def callback(self, ch, method, properties, body):
        """Processa ogni messaggio ricevuto"""
        try:
            message = json.loads(body.decode())
            device = message.get('device', '')
            
            with stats_lock:
                if device == 'solar_panel':
                    self.update_solar_stats(message)
                elif device == 'irrigation':
                    self.update_irrigation_stats(message)
                elif device == 'alarm_system':
                    self.update_alarm_stats(message)
                
                stats['total_messages'] += 1
                messages_count = stats['total_messages']
            
            # Invia a WebSocket
            socketio.emit('new_message', {
                'device': device,
                'data': message,
                'timestamp': datetime.now().isoformat()
            }, skip_sid=None)
            
            # Aggiorna stats ogni 10 messaggi
            if messages_count % 10 == 0:
                self.broadcast_stats()
            
            # ACK del messaggio
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if messages_count % 50 == 0:
                print(f"📨 [WEB] Messaggi consumati: {messages_count}")
        
        except Exception as e:
            print(f"❌ [WEB] Errore processing: {e}")
            import traceback
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def update_solar_stats(self, msg):
        """Aggiorna le statistiche dei pannelli solari"""
        panel_id = msg.get('panel_id')
        power = msg.get('power_watts', 0)
        temp = msg.get('temperature', 0)
        timestamp = datetime.fromisoformat(msg.get('timestamp', datetime.now().isoformat()))
        
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
    
    def update_irrigation_stats(self, msg):
        """Aggiorna le statistiche degli irrigatori"""
        zone_id = msg.get('zone_id')
        moisture = msg.get('soil_moisture', 0)
        pump_status = msg.get('pump_status', 'off')
        timestamp = datetime.fromisoformat(msg.get('timestamp', datetime.now().isoformat()))
        
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
    
    def update_alarm_stats(self, msg):
        """Aggiorna le statistiche degli allarmi"""
        severity = msg.get('severity', 'unknown')
        stats['alarms'][severity]['count'] += 1
    
    def broadcast_stats(self):
        """Calcola e invia le statistiche ai client WebSocket"""
        with stats_lock:
            uptime = datetime.now() - stats['start_time']
            uptime_minutes = int(uptime.total_seconds() // 60)
            
            # ========== PANNELLI SOLARI ==========
            solar_data = {}
            for panel_id in sorted(stats['solar_panels'].keys()):
                s = stats['solar_panels'][panel_id]
                
                if s['measurements']:
                    powers = [m['power'] for m in s['measurements']]
                    temps = [m['temp'] for m in s['measurements']]
                    
                    avg_power = sum(powers) / len(powers) if powers else 0
                    avg_temp = sum(temps) / len(temps) if temps else 0
                    min_power = min(powers) if powers else 0
                    max_power = max(powers) if powers else 0
                    
                    last_measurement = s['measurements'][-1]
                    last_power = last_measurement['power']
                    last_temp = last_measurement['temp']
                    last_timestamp = last_measurement['timestamp'].isoformat()
                else:
                    avg_power = avg_temp = min_power = max_power = 0
                    last_power = last_temp = last_timestamp = None
                
                solar_data[str(panel_id)] = {
                    'avg_power': round(avg_power, 1),
                    'avg_temp': round(avg_temp, 1),
                    'min_power': int(min_power),
                    'max_power': int(max_power),
                    'last_power': int(last_power) if last_power else 0,
                    'last_temp': round(last_temp, 1) if last_temp else 0,
                    'last_timestamp': last_timestamp,
                    'count': s['count']
                }
            
            # ========== IRRIGATORI ==========
            irrigation_data = {}
            for zone_id in sorted(stats['irrigation'].keys()):
                s = stats['irrigation'][zone_id]
                
                if s['measurements']:
                    moistures = [m['moisture'] for m in s['measurements']]
                    avg_moisture = sum(moistures) / len(moistures)
                    min_moisture = min(moistures)
                    max_moisture = max(moistures)
                    
                    pump_on_count = sum(1 for m in s['measurements'] if m['pump_status'] == 'on')
                    pump_on_pct = (pump_on_count / len(s['measurements'])) * 100
                else:
                    avg_moisture = min_moisture = max_moisture = pump_on_pct = 0
                
                irrigation_data[str(zone_id)] = {
                    'avg_moisture': round(avg_moisture, 1),
                    'min_moisture': int(min_moisture),
                    'max_moisture': int(max_moisture),
                    'pump_on_pct': round(pump_on_pct, 1),
                    'count': s['count']
                }
            
            # ========== ALLARMI ==========
            alarms_data = {}
            total_alarms = 0
            for severity in ['critical', 'high', 'medium', 'low']:
                count = stats['alarms'][severity]['count']
                alarms_data[severity] = count
                total_alarms += count
            
            # Invia ai client WebSocket
            socketio.emit('stats_update', {
                'total_messages': stats['total_messages'],
                'uptime_minutes': uptime_minutes,
                'solar_panels': solar_data,
                'irrigation': irrigation_data,
                'alarms': alarms_data,
                'total_alarms': total_alarms
            }, skip_sid=None)

    def start_consuming(self):
        """Inizia a consumare messaggi"""
        self.channel.basic_qos(prefetch_count=10)
        self.channel.basic_consume(
            queue=QUEUES['web_dashboard']['name'],
            on_message_callback=self.callback
        )
        
        print("=" * 70)
        print("📊 [WEB] In ascolto da RabbitMQ...")
        print(f"📁 Queue: {QUEUES['web_dashboard']['name']}\n")
        
        self.channel.start_consuming()
    
    def close(self):
        """Chiude la connessione"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("✅ [WEB] Connessione RabbitMQ chiusa")

# ============================================================
# THREAD CONSUMER
# ============================================================

consumer = None
consumer_thread = None

def start_consumer_thread():
    """Avvia il consumer in un thread separato"""
    global consumer, consumer_thread
    
    consumer = RabbitMQConsumer()
    if not consumer.connect():
        print("❌ [WEB] Impossibile connettersi a RabbitMQ")
        return False
    
    consumer_thread = threading.Thread(
        target=consumer.start_consuming,
        daemon=True,
        name="RabbitMQConsumer"
    )
    consumer_thread.start()
    print("✅ [WEB] Thread consumer RabbitMQ avviato\n")
    return True

# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
def index():
    """Pagina principale"""
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    """API REST per le statistiche"""
    with stats_lock:
        return {
            'total_messages': stats['total_messages'],
            'uptime': (datetime.now() - stats['start_time']).total_seconds(),
            'solar_panels': dict(stats['solar_panels']),
            'irrigation': dict(stats['irrigation']),
            'alarms': dict(stats['alarms'])
        }

# ============================================================
# WEBSOCKET EVENTS
# ============================================================

@socketio.on('connect')
def handle_connect():
    """Client WebSocket connesso"""
    client_ip = request.remote_addr if request else 'unknown'
    print(f"✅ [WEB] Client connesso da: {client_ip}")
    
    with stats_lock:
        uptime = datetime.now() - stats['start_time']
        uptime_minutes = int(uptime.total_seconds() // 60)
        
        # Prepara dati pannelli solari
        solar_data = {}
        for panel_id in sorted(stats['solar_panels'].keys()):
            s = stats['solar_panels'][panel_id]
            
            if s['measurements']:
                powers = [m['power'] for m in s['measurements']]
                temps = [m['temp'] for m in s['measurements']]
                
                avg_power = sum(powers) / len(powers)
                avg_temp = sum(temps) / len(temps)
                min_power = min(powers)
                max_power = max(powers)
                
                last_measurement = s['measurements'][-1]
                last_power = last_measurement['power']
                last_temp = last_measurement['temp']
                last_timestamp = last_measurement['timestamp'].isoformat()
            else:
                avg_power = avg_temp = min_power = max_power = 0
                last_power = last_temp = last_timestamp = None
            
            solar_data[str(panel_id)] = {
                'avg_power': round(avg_power, 1),
                'avg_temp': round(avg_temp, 1),
                'min_power': int(min_power),
                'max_power': int(max_power),
                'last_power': int(last_power) if last_power else 0,
                'last_temp': round(last_temp, 1) if last_temp else 0,
                'last_timestamp': last_timestamp,
                'count': s['count']
            }
        
        # Prepara dati irrigatori
        irrigation_data = {}
        for zone_id in sorted(stats['irrigation'].keys()):
            s = stats['irrigation'][zone_id]
            
            if s['measurements']:
                moistures = [m['moisture'] for m in s['measurements']]
                avg_moisture = sum(moistures) / len(moistures)
                min_moisture = min(moistures)
                max_moisture = max(moistures)
                
                pump_on_count = sum(1 for m in s['measurements'] if m['pump_status'] == 'on')
                pump_on_pct = (pump_on_count / len(s['measurements'])) * 100
            else:
                avg_moisture = min_moisture = max_moisture = pump_on_pct = 0
            
            irrigation_data[str(zone_id)] = {
                'avg_moisture': round(avg_moisture, 1),
                'min_moisture': int(min_moisture),
                'max_moisture': int(max_moisture),
                'pump_on_pct': round(pump_on_pct, 1),
                'count': s['count']
            }
        
        # Prepara dati allarmi
        alarms_dict = {}
        total_alarms = 0
        for severity in ['critical', 'high', 'medium', 'low']:
            count = stats['alarms'][severity]['count']
            alarms_dict[severity] = count
            total_alarms += count
        
        emit('stats_update', {
            'total_messages': stats['total_messages'],
            'uptime_minutes': uptime_minutes,
            'solar_panels': solar_data,
            'irrigation': irrigation_data,
            'alarms': alarms_dict,
            'total_alarms': total_alarms
        })

@socketio.on('disconnect')
def handle_disconnect():
    """Client WebSocket disconnesso"""
    print("❌ [WEB] Client disconnesso")

@socketio.on_error_default
def default_error_handler(e):
    """Gestione errori WebSocket"""
    print(f"⚠️ [WEB] Errore WebSocket: {e}")
    import traceback
    traceback.print_exc()

# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print("=" * 70)
    print("🌐 SERVER WEB - DASHBOARD AGRICOLO IoT")
    print("=" * 70)
    
    if not start_consumer_thread():
        print("❌ Errore nell'avvio del consumer")
        sys.exit(1)
    
    print("🚀 Avvio server Flask su:")
    print("   http://localhost:5000")
    print("   http://0.0.0.0:5000")
    print("\n⏳ In attesa di connessioni WebSocket...\n")
    
    try:
        socketio.run(
            app,
            host='0.0.0.0',
            port=5000,
            debug=False,
            use_reloader=False,
            log_output=True
        )
    except KeyboardInterrupt:
        print("\n\n👋 Server Flask fermato")
        if consumer:
            consumer.close()
    except Exception as e:
        print(f"❌ Errore: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)