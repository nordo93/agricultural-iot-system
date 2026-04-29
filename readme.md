
# 🌾 Agricultural IoT System — RabbitMQ

Sistema distribuito di raccolta, instradamento e monitoraggio di eventi IoT per impianti agricoli.
Gestisce dati provenienti da pannelli solari, irrigatori e sistema di allarme tramite RabbitMQ,
con Web App integrata, logging strutturato e gestione degli errori via Dead Letter Queue.

---

## 🏗️ Architettura
<img width="730" height="433" alt="image" src="https://github.com/user-attachments/assets/468531d4-332d-4f50-a02f-406d06259fd8" />

---

## 📋 Requisiti

- Docker Desktop
- Python 3.7+
- pip install -r requirements.txt

---

## 🚀 Avvio rapido

### 1️⃣ Installazione dipendenze e avvio RabbitMQ

```bash
pip install -r requirements.txt
cd docker-compose
docker-compose up -d
cd ..
```

Verifica RabbitMQ Management UI: [http://localhost:15672](http://localhost:15672)
- Username: `admin` | Password: `admin`

---

### 2️⃣ Avviare i componenti (terminali separati)

**Terminal 1 — Monitoraggio real-time**
```bash
python consumer/monitoring_app.py
```

**Terminal 2 — Logger storico (JSON)**
```bash
python consumer/logger_app.py
```

**Terminal 3 — Dead Letter Handler (errori)**
```bash
python consumer/dead_letter_handler.py
```

**Terminal 4 — Producer: Pannelli Solari**
```bash
python producers/solar_panels.py
```

**Terminal 5 — Producer: Irrigatori**
```bash
python producers/irrigation.py
```

**Terminal 6 — Producer: Sistema di Allarme**
```bash
python producers/alarm_system.py
```

---

## 🌐 Interfacce Web

| Interfaccia | URL | Credenziali |
|---|---|---|
| RabbitMQ Management | http://localhost:15672 | admin / admin |
| Web App Flask | NON necessario e non ANCORA IMPLEMENTATO |  NON necessario e non ANCORA IMPLEMENTATO | 

una futura web app potrebbe rendere possibile visualizzare eventi in tempo reale, code attive, binding e statistiche.

---

## 📊 Output atteso

**monitoring_app.py**


<img width="916" height="161" alt="image" src="https://github.com/user-attachments/assets/ce49e640-0240-43b1-8870-4b1edd957fc0" />

