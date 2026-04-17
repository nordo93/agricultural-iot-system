#!/usr/bin/env python3
"""
Validazione centralizzata dei messaggi
: validazione nel consumer, non nel producer

Producer (solar_panels.py)
    ↓ invia messaggio
Exchange: agricultural_events
    ├─→ Queue: monitoring_queue
    │       ├─→ Consumer valida
    │       ├─→ ✅ Valido? → elabora + ACK
    │       └─→ ❌ Invalido? → NACK → DLQ
    │
    └─→ Queue: dead_letter_queue
            ↓
            Dead Letter Handler
            ↓
            logs/dead_letter_errors.log
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import VALIDATION_RULES

class MessageValidator:
    """Valida i messaggi in ingresso al middleware"""
    
    @staticmethod
    def validate(message):
        """
        Valida un messaggio completo
        
        Returns:
            tuple: (is_valid: bool, error_message: str)
        """
        # Step 1: Controlla campo 'device'
        device = message.get('device')
        if not device:
            return False, "Campo 'device' mancante"
        
        # Step 2: Valida in base al tipo di dispositivo
        if device == 'solar_panel':
            return MessageValidator._validate_solar_panel(message)
        
        elif device == 'irrigation':
            return MessageValidator._validate_irrigation(message)
        
        elif device == 'alarm_system':
            return MessageValidator._validate_alarm(message)
        
        else:
            return False, f"Device sconosciuto: {device}"
    
    @staticmethod
    def _validate_solar_panel(msg):
        """Valida messaggio pannello solare"""
        rules = VALIDATION_RULES['solar_panel']
        
        # panel_id
        panel_id = msg.get('panel_id')
        if panel_id is None:
            return False, "Campo 'panel_id' mancante"
        if not isinstance(panel_id, int):
            return False, f"panel_id deve essere int, ricevuto {type(panel_id)}"
        if not (rules['panel_id']['min'] <= panel_id <= rules['panel_id']['max']):
            return False, f"panel_id {panel_id} fuori range [{rules['panel_id']['min']}-{rules['panel_id']['max']}]"
        
        # power_watts
        power_watts = msg.get('power_watts')
        if power_watts is None:
            return False, "Campo 'power_watts' mancante"
        if not isinstance(power_watts, (int, float)):
            return False, f"power_watts deve essere numero, ricevuto {type(power_watts)}"
        if not (rules['power_watts']['min'] <= power_watts <= rules['power_watts']['max']):
            return False, f"power_watts {power_watts}W fuori range [{rules['power_watts']['min']}-{rules['power_watts']['max']}]W"
        
        # temperature
        temperature = msg.get('temperature')
        if temperature is None:
            return False, "Campo 'temperature' mancante"
        if not isinstance(temperature, (int, float)):
            return False, f"temperature deve essere numero, ricevuto {type(temperature)}"
        if not (rules['temperature']['min'] <= temperature <= rules['temperature']['max']):
            return False, f"temperature {temperature}°C fuori range [{rules['temperature']['min']}-{rules['temperature']['max']}]°C"
        
        # timestamp
        timestamp = msg.get('timestamp')
        if not timestamp:
            return False, "Campo 'timestamp' mancante"
        
        # ✅ Tutto valido
        return True, "OK"
    
    @staticmethod
    def _validate_irrigation(msg):
        """Valida messaggio irrigatore"""
        rules = VALIDATION_RULES['irrigation']
        
        # zone_id
        zone_id = msg.get('zone_id')
        if zone_id is None:
            return False, "Campo 'zone_id' mancante"
        if not isinstance(zone_id, int):
            return False, f"zone_id deve essere int, ricevuto {type(zone_id)}"
        if not (rules['zone_id']['min'] <= zone_id <= rules['zone_id']['max']):
            return False, f"zone_id {zone_id} fuori range [{rules['zone_id']['min']}-{rules['zone_id']['max']}]"
        
        # soil_moisture
        soil_moisture = msg.get('soil_moisture')
        if soil_moisture is None:
            return False, "Campo 'soil_moisture' mancante"
        if not isinstance(soil_moisture, (int, float)):
            return False, f"soil_moisture deve essere numero, ricevuto {type(soil_moisture)}"
        if not (rules['soil_moisture']['min'] <= soil_moisture <= rules['soil_moisture']['max']):
            return False, f"soil_moisture {soil_moisture}% fuori range [{rules['soil_moisture']['min']}-{rules['soil_moisture']['max']}]%"
        
        # pump_status
        pump_status = msg.get('pump_status')
        if pump_status is None:
            return False, "Campo 'pump_status' mancante"
        if pump_status not in rules['pump_status']['allowed']:
            return False, f"pump_status '{pump_status}' non valido. Consentiti: {rules['pump_status']['allowed']}"
        
        # timestamp
        timestamp = msg.get('timestamp')
        if not timestamp:
            return False, "Campo 'timestamp' mancante"
        
        # ✅ Tutto valido
        return True, "OK"
    
    @staticmethod
    def _validate_alarm(msg):
        """Valida messaggio allarme"""
        rules = VALIDATION_RULES['alarm_system']
        
        # severity
        severity = msg.get('severity')
        if severity is None:
            return False, "Campo 'severity' mancante"
        if severity not in rules['severity']['allowed']:
            return False, f"severity '{severity}' non valido. Consentiti: {rules['severity']['allowed']}"
        
        # description
        description = msg.get('description')
        if not description:
            return False, "Campo 'description' mancante"
        if not isinstance(description, str):
            return False, f"description deve essere stringa, ricevuto {type(description)}"
        if len(description) > rules['description']['max_length']:
            return False, f"description troppo lunga ({len(description)} > {rules['description']['max_length']})"
        
        # timestamp
        timestamp = msg.get('timestamp')
        if not timestamp:
            return False, "Campo 'timestamp' mancante"
        
        # ✅ Tutto valido
        return True, "OK"