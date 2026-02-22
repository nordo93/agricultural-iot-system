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