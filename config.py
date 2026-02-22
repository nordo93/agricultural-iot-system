"""
Configurazione centralizzata
"""

# RabbitMQ Configuration
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'admin'

# Exchange Configuration
EXCHANGE_NAME = 'agricultural_events'
EXCHANGE_TYPE = 'topic'  # Permette routing basato su pattern

# Topics (Routing Keys)
TOPICS = {
    'solar_panels': 'agriculture.solar_panels.#',
    'irrigation': 'agriculture.irrigation.#',
    'alarm': 'agriculture.alarm.#',
    'all': 'agriculture.#', # ← attenzione agriculture.* è diverso da  agriculture.# , il segno # ascolta tutto
}

#✅ agriculture.# lo riceve (monitoraggio, logger)
#❌ agriculture.* NON lo riceve (web_dashboard)
#Perché agriculture.* matcha solo:
#agriculture.solar_panels ✅
#agriculture.irrigation ✅

#Ma NON agriculture.solar_panels.1 ❌

# Queue Configuration  centralizzo il nome delle code se cambiano , web e monitoring app hanno dei codici commentati su come fosse prima il metodoto connect 
QUEUES = {
    'monitoring': {
        'name': 'monitoring_queue',
        'binding_key': 'agriculture.#'   # Ascolta tutto
    },
    'logger': {
        'name': 'logger_queue',
        'binding_key': 'agriculture.#'   # Ascolta tutto
    },
    'stats': {                          
        'name': 'stats_queue',
        'binding_key': 'agriculture.#'
    },
    'web_dashboard': {                  
        'name': 'web_dashboard_queue',
        'binding_key': 'agriculture.#'
    }
    }