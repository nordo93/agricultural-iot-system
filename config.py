"""
Configurazione centralizzata
"""
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
EXCHANGE_TYPE = 'topic'
DEAD_LETTER_EXCHANGE = 'agricultural_events_dlx'  # ← NUOVO: Exchange per errori

# Topics (Routing Keys)
TOPICS = {
    'solar_panels': 'agriculture.solar_panels.#',
    'irrigation': 'agriculture.irrigation.#',
    'alarm': 'agriculture.alarm.#',
    'all': 'agriculture.#',
}

# Queue Configuration con Dead Letter
QUEUES = {
    'monitoring': {
        'name': 'monitoring_queue',
        'binding_key': 'agriculture.#',
        'dead_letter_exchange': DEAD_LETTER_EXCHANGE  # ← NUOVO
    },
    'logger': {
        'name': 'logger_queue',
        'binding_key': 'agriculture.#',
        'dead_letter_exchange': DEAD_LETTER_EXCHANGE  # ← NUOVO
    },
    'stats': {
        'name': 'stats_queue',
        'binding_key': 'agriculture.#',
        'dead_letter_exchange': DEAD_LETTER_EXCHANGE  # ← NUOVO
    },
    'web_dashboard': {
        'name': 'web_dashboard_queue',
        'binding_key': 'agriculture.#',
        'dead_letter_exchange': DEAD_LETTER_EXCHANGE  # ← NUOVO
    },
    'dead_letter': {                                  # ← NUOVO: Queue per errori
        'name': 'dead_letter_queue',
        'binding_key': 'agriculture.*',
        'dead_letter_exchange': None
    }
}

# Vincoli validazione (come richiesto dal prof)
VALIDATION_RULES = {
    'solar_panel': {
        'panel_id': {'min': 0, 'max': 10},
        'power_watts': {'min': 0, 'max': 600},
        'temperature': {'min': -10, 'max': 60}
    },
    'irrigation': {
        'zone_id': {'min': 1, 'max': 10},
        'soil_moisture': {'min': 0, 'max': 100},
        'pump_status': {'allowed': ['on', 'off']}
    },
    'alarm_system': {
        'severity': {'allowed': ['critical', 'high', 'medium', 'low']},
        'description': {'max_length': 500}
    }
}