# 1. Installa dipendenze
pip install -r requirements.txt

# 2. Avvia RabbitMQ
docker-compose up -d

# 3. Avvia la webapp
python web/app.py

# 4. Avvia i producer in altri terminali
python producers/solar_panels.py
python producers/irrigation.py
python producers/alarm_system.py

# 5. Apri il browser
# http://localhost:5000


---

## 🎯 Piano d'azione per completare:

**Priorità ALTA (essenziale per il voto):**
1. ✅ Completa implementazione (FATTO)
2. ⚠️ **Scrivi rapporto tecnico** (20% del voto)
3. ⚠️ **Aggiungi validazione input e error handling**
4. ⚠️ **Documenta scenari di failure**

**Priorità MEDIA (miglior voto):**
5. 🔒 **Implementa HTTPS + autenticazione web**
6. 📊 **Aggiungi test automatizzati**
7. 📈 **Misura performance**

**Priorità BASSA (extra):**
8. 🔐 **Crittografia end-to-end messaggi**
9. 📊 **Dashboard metriche RabbitMQ**
10. 🚀 **Deploy su cloud**

---

## 💡 Suggerimento finale:

**Il tuo codice è BUONO**, ma le specifiche richiedono:
- **40% Implementazione** (tu sei qui ✅)
- **40% Documentazione + Rapporto** (manca ❌)
- **20% Test + Scenari** (parziale ⚠️)

**Non sottovalutare il rapporto!** È metà del progetto.

---

Vuoi che ti aiuti a scrivere il **rapporto tecnico** o a implementare **test e scenari di failure**? 🚀