# 🌾 Agricultural IoT System - RabbitMQ

Sistema completo di gestione messaggi per impianti agricoli con pannelli solari, irrigatori e sistema di allarme.

## 🏗️ Architettura

```
Pannelli Solari ──┐
Irrigatori ────┼──→ RabbitMQ (Topic Exchange) ──→ Monitoraggio
Sistema Allarme──┘                            └──→ Logger
```

## 📋 Requisiti

- Docker Desktop installato
- Python 3.7+

## 🚀 Avvio rapido

### 1️⃣ Preparazione

```bash
pip install -r requirements.txt
docker-compose up -d
```

### 2️⃣ Avviare in terminali separati

**Terminal 1 - Monitoraggio (Real-time)**
```bash
python consumers/monitoring_app.py
```

**Terminal 2 - Logger (Storico)**
```bash
python consumers/logger_app.py
```

**Terminal 3 - Pannelli Solari**
```bash
python producers/solar_panels.py
```

**Terminal 4 - Irrigatori**
```bash
python producers/irrigation.py
```

**Terminal 5 - Sistema di Allarme**
```bash
python producers/alarm_system.py
```

## 📊 Output atteso

### Monitoraggio
```
☀️ PANNELLO 1 | 456W @ 32°C | ok
💧 ON  | ZONA 1 | Umidità:  35% | ok
⚨ ALLARME [HIGH] Temperatura pannello sopra soglia
```

### Logger
```
📝 Evento loggato | Device: solar_panel | agriculture.solar_panels.1
```

Controlla `agricultural_events.log` per lo storico completo.

## 🔍 Management UI

http://localhost:15672

- Username: `admin`
- Password: `rabbitpwd`

Puoi visualizzare:
- **Exchanges**: `agricultural_events`
- **Queues**: `monitoring_queue`, `logger_queue`
- **Bindings**: Pattern di routing

## 📝 File di Log

Il file `agricultural_events.log` contiene tutti gli eventi in JSON:

```json
{
  "timestamp": "2026-02-20T10:54:00+01:00",
  "device": "solar_panel",
  "panel_id": 1,
  "power_watts": 456,
  "temperature": 32,
  "status": "ok"
}
```

## 🛑 Arresto

```bash
# Ferma i producer/consumer (Ctrl+C)
# Arresta RabbitMQ
docker-compose down
```

## 🔧 Personalizzazione

### Aggiungere un nuovo dispositivo

1. Crea un nuovo producer in `producers/`
2. Usa `EXCHANGE_NAME` e routing key pattern: `agriculture.type.*`
3. I consumer riceveranno automaticamente gli eventi!

### Modificare topic di routing

Edita `config.py`:

```python
TOPICS = {
    'mio_dispositivo': 'agriculture.mio_dispositivo.#',
}
```

## 📈 Caso d'uso reale

Questo sistema è ideale per:
- ✅ Monitorare sensori agricoli in tempo reale
- ✅ Registrare dati storici per analisi
- ✅ Ricevere alerting su anomalie
- ✅ Scalare a centinaia di dispositivi
- ✅ Integrare con dashboard web/mobile

## 💡 Suggerimenti avanzati

- **Clustering**: Aggiungi più consumer per load balancing
- **Persistenza**: I dati rimangono in RabbitMQ fino a consumo
- **DLQ**: Dead Letter Queue per errori
- **Metriche**: Integra Prometheus per monitoring