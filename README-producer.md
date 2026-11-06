# StreamForge — Kafka Event Producer

## Install (run from the StreamForge project root)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, run:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## Publish 30 events
```powershell
python src/producer.py --events 30 --interval 0.5
```

## Publish continuously
```powershell
python src/producer.py --events 0 --interval 1
```
Press Ctrl+C to stop.

## Consume events in another terminal
```powershell
docker exec -it streamforge-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic ecommerce-events `
  --from-beginning
```

Windows uses `localhost:29092`; containers use `kafka:9092`.
The producer uses `user_id` as the Kafka key, so events for the same user
are consistently routed to the same partition.
