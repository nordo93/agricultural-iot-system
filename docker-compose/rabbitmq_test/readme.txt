 Installare Docker su Windows 11
Opzione 1: Docker Desktop (Consigliato - All-in-one)
Step 1: Scarica Docker Desktop
Vai su: https://www.docker.com/products/docker-desktop
Clicca su "Download for Windows"
Scarica il file .exe (circa 500 MB)
Step 2: Installa
Esegui il file scaricato
Seleziona le opzioni:
✅ WSL 2 backend (Windows Subsystem for Linux 2)
✅ Add Docker to PATH (importante!)
Completa l'installazione e riavvia il PC
Step 3: Verifica l'installazione
Apri PowerShell o Prompt dei comandi e digita:

bash
docker --version
docker-compose --version
Dovresti vedere qualcosa come:

Code
Docker version 25.x.x
Docker Compose version 2.x.x
⚙️ Configurazione WSL 2 (Necessaria)
Se non è già installato, Windows 11 potrebbe chiederti di installare WSL 2:

bash
# In PowerShell (come Amministratore)
wsl --install
Poi riavvia e completa l'installazione.

🚀 Primo avvio
Apri Docker Desktop (comparirà un'icona nella taskbar)
Attendi che si avvii completamente (spesso ci vogliono 1-2 minuti)
L'icona diventerà verde quando è pronto
✅ Test finale
Apri PowerShell e digita:

bash
docker run hello-world
Se vedi un messaggio di successo, sei pronto! ✨

📁 Adesso puoi usare il tuo RabbitMQ
bash
# Vai nella cartella del progetto
cd docker-compose/rabbitmq/

# Avvia RabbitMQ
docker-compose up -d

# Verifica
docker-compose ps
Poi accedi a: http://localhost:15672 con admin / admin