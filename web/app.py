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
        'measurements': [],  # Lista di {'timestamp': ..., 'power': ..., 'temp': ...}
        'count': 0,
        'last_update': None
    }),
    'irrigation': defaultdict(lambda: {
        'measurements': [],  # Lista di {'timestamp': ..., 'moisture': ..., 'pump_status': ...}
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
        """Connessione a RabbitMQ e dichiarazione queue/exchange"""
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
            
            # Dichiara e collega la queue
            self.channel.queue_declare(
                queue=QUEUES['web_dashboard']['name'],
                durable=True
            )
            self.channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=QUEUES['web_dashboard']['name'],
                routing_key=QUEUES['web_dashboard']['binding_key']
            )
            
            self.connected = True
            print(f"✅ [WEB] Connesso a RabbitMQ")
            print(f"   Exchange: {EXCHANGE_NAME}")
            print(f"   Queue: {QUEUES['web_dashboard']['name']}")
            print(f"   Binding: {QUEUES['web_dashboard']['binding_key']}")
            return True
        except Exception as e:
            print(f"❌ [WEB] Errore connessione: {e}")
            import traceback
            traceback.print_exc()
            self.connected = False
            return False

    def callback(self, ch, method, properties, body):
        """
        Callback che elabora ogni messaggio ricevuto
        Viene chiamato CONTINUAMENTE per ogni nuovo messaggio
        """
        try:
            message = json.loads(body.decode())
            device = message.get('device', '')
            
            with stats_lock:
                # Aggiorna le statistiche in base al tipo di dispositivo
                if device == 'solar_panel':
                    self.update_solar_stats(message)
                elif device == 'irrigation':
                    self.update_irrigation_stats(message)
                elif device == 'alarm_system':
                    self.update_alarm_stats(message)
                
                stats['total_messages'] += 1
                messages_count = stats['total_messages']
            
            # Invia il nuovo messaggio ai client WebSocket (tempo reale)
            socketio.emit('new_message', {
                'device': device,
                'data': message,
                'timestamp': datetime.now().isoformat()
            }, skip_sid=None)
            
            # Invia aggiornamento statistiche ogni 10 messaggi
            if messages_count % 10 == 0:
                self.broadcast_stats()
            
            # Acknowledge del messaggio (confermo di averlo consumato)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Debug - stampa ogni 50 messaggi
            if messages_count % 50 == 0:
                print(f"📨 [WEB] Messaggi consumati: {messages_count}")
        
        except Exception as e:
            print(f"❌ [WEB] Errore processing: {e}")
            import traceback
            traceback.print_exc()
            # Nack e requeue il messaggio se c'è errore
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def update_solar_stats(self, msg):
        """Aggiorna le statistiche dei pannelli solari"""
        panel_id = msg.get('panel_id')
        power = msg.get('power_watts', 0)
        temp = msg.get('temperature', 0)
        timestamp = datetime.fromisoformat(msg.get('timestamp', datetime.now().isoformat()))
        
        s = stats['solar_panels'][panel_id]
        
        # Aggiungi la misurazione con timestamp
        s['measurements'].append({
            'timestamp': timestamp,
            'power': power,
            'temp': temp
        })
        
        s['count'] += 1
        s['last_update'] = datetime.now().isoformat()
        
        # Mantieni solo le misurazioni dell'ultima ora
        one_hour_ago = datetime.now() - __import__('datetime').timedelta(hours=1)
        s['measurements'] = [m for m in s['measurements'] if m['timestamp'] > one_hour_ago]
    
    def update_irrigation_stats(self, msg):
        """Aggiorna le statistiche degli irrigatori"""
        zone_id = msg.get('zone_id')
        moisture = msg.get('soil_moisture', 0)
        pump_status = msg.get('pump_status', 'off')
        
        s = stats['irrigation'][zone_id]
        s['moisture'].append(moisture)
        s['count'] += 1
        s['last_update'] = datetime.now().isoformat()
        
        if pump_status == 'on':
            s['pump_on'] += 1
        else:
            s['pump_off'] += 1
        
        # Mantieni solo gli ultimi 100 valori (finestra mobile)
        if len(s['moisture']) > 100:
            s['moisture'] = s['moisture'][-100:]
    
    def update_alarm_stats(self, msg):
        """Aggiorna le statistiche degli allarmi"""
        severity = msg.get('severity', 'unknown')
        stats['alarms'][severity]['count'] += 1
    
    def broadcast_stats(self):
        """
        Calcola e invia le statistiche aggregate ai client WebSocket
        Viene chiamato ogni 10 messaggi
        """
        print(f"📊 [WEB] Calcolo e invio statistiche ai client...")
        
        with stats_lock:
            uptime = datetime.now() - stats['start_time']
            uptime_minutes = int(uptime.total_seconds() // 60)
            
            # ========== PANNELLI SOLARI ==========
            solar_data = {}
            for panel_id in sorted(stats['solar_panels'].keys()):
                s = stats['solar_panels'][panel_id]
                
                # Se ci sono misurazioni, calcola statistiche
                if s['measurements']:
                    # Media dell'ultima ora
                    powers = [m['power'] for m in s['measurements']]
                    temps = [m['temp'] for m in s['measurements']]
                    
                    avg_power = sum(powers) / len(powers) if powers else 0
                    avg_temp = sum(temps) / len(temps) if temps else 0
                    min_power = min(powers) if powers else 0
                    max_power = max(powers) if powers else 0
                    
                    # Ultima misurazione
                    last_measurement = s['measurements'][-1]  # L'ultimo elemento
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
                
                # Calcola media, min, max dell'umidità
                avg_moisture = sum(s['moisture']) / len(s['moisture']) if s['moisture'] else 0
                min_moisture = min(s['moisture']) if s['moisture'] else 0
                max_moisture = max(s['moisture']) if s['moisture'] else 0
                
                # Calcola percentuale pompa accesa
                pump_on_pct = (s['pump_on'] / s['count'] * 100) if s['count'] > 0 else 0
                
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
                count = stats['alarms'].get(severity, {}).get('count', 0)
                alarms_data[severity] = count
                total_alarms += count
            
            # Invia i dati aggregati a TUTTI i client WebSocket
            socketio.emit('stats_update', {
                'total_messages': stats['total_messages'],
                'uptime_minutes': uptime_minutes,
                'solar_panels': solar_data,
                'irrigation': irrigation_data,
                'alarms': alarms_data,
                'total_alarms': total_alarms
            }, skip_sid=None)

    def start_consuming(self):
        """
        Inizia il consuming dei messaggi da RabbitMQ
        Questo metodo BLOCCA il thread e rimane in ascolto continuamente
        """
        # Configurazione QoS (Quality of Service)
        # prefetch_count=10 significa che il consumer riceve 10 messaggi per volta
        self.channel.basic_qos(prefetch_count=10)
        
        # Configura il callback per questa queue
        self.channel.basic_consume(
            queue=QUEUES['web_dashboard']['name'],
            on_message_callback=self.callback
        )
        
        print(f"⏳ [WEB] In ascolto da RabbitMQ...")
        print(f"📨 Il consuming avviene CONTINUAMENTE su ogni nuovo messaggio")
        print(f"📊 Le statistiche vengono calcolate ogni 10 messaggi\n")
        
        # Questo rimane in ascolto indefinitamente
        self.channel.start_consuming()
    
    def close(self):
        """Chiude la connessione a RabbitMQ"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("✅ [WEB] Connessione RabbitMQ chiusa")

# ============================================================
# THREAD CONSUMER
# ============================================================

consumer = None
consumer_thread = None

def start_consumer_thread():
    """Avvia il consumer RabbitMQ in un thread separato dal server Flask"""
    global consumer, consumer_thread
    
    consumer = RabbitMQConsumer()
    if not consumer.connect():
        print("❌ [WEB] Impossibile connettersi a RabbitMQ")
        return False
    
    # Avvia il consumer in un thread daemon (termina quando muore il processo principale)
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
    """Pagina principale - Ritorna il template HTML"""
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    """API REST per ottenere le statistiche (fallback se WebSocket non funziona)"""
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
    """Evento: nuovo client WebSocket connesso"""
    client_ip = request.remote_addr if request else 'unknown'
    print(f"✅ [WEB] Client WebSocket connesso da: {client_ip}")
    
    with stats_lock:
        # ========== Prepara dati pannelli solari ==========
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
        
        # ========== Prepara dati irrigatori ==========
        irrigation_data = {}
        for zone_id in sorted(stats['irrigation'].keys()):
            s = stats['irrigation'][zone_id]
            avg_moisture = sum(s['moisture']) / len(s['moisture']) if s['moisture'] else 0
            min_moisture = min(s['moisture']) if s['moisture'] else 0
            max_moisture = max(s['moisture']) if s['moisture'] else 0
            pump_on_pct = (s['pump_on'] / s['count'] * 100) if s['count'] > 0 else 0
            
            irrigation_data[str(zone_id)] = {
                'avg_moisture': round(avg_moisture, 1),
                'min_moisture': int(min_moisture),
                'max_moisture': int(max_moisture),
                'pump_on_pct': round(pump_on_pct, 1),
                'count': s['count']
            }
        
        # ========== Prepara dati allarmi ==========
        alarms_dict = {}
        for severity in ['critical', 'high', 'medium', 'low']:
            if severity in stats['alarms']:
                alarms_dict[severity] = stats['alarms'][severity]['count']
            else:
                alarms_dict[severity] = 0
        
        # Invia le statistiche iniziali al nuovo client
        emit('stats_update', {
            'total_messages': stats['total_messages'],
            'uptime_minutes': int((datetime.now() - stats['start_time']).total_seconds() // 60),
            'solar_panels': solar_data,
            'irrigation': irrigation_data,
            'alarms': alarms_dict,
            'total_alarms': sum(alarms_dict.values())
        })

@socketio.on('disconnect')
def handle_disconnect():
    """Evento: client WebSocket disconnesso"""
    print("❌ [WEB] Client WebSocket disconnesso")

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
    
    # Avvia il consumer RabbitMQ in un thread separato
    if not start_consumer_thread():
        print("❌ Errore nell'avvio del consumer RabbitMQ")
        sys.exit(1)
    
    # Avvia il server Flask con WebSocket
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
        print(f"❌ Errore nell'avvio del server: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)