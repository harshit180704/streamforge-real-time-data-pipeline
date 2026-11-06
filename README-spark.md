# StreamForge Spark milestone

## Add the Spark service

Open the existing root `docker-compose.yml`.

Add the `spark:` service shown in `docker-compose-spark-addition.yml`
under `services:` at the same indentation level as `kafka:` and `postgres:`.

Then add `spark_checkpoints:` under the existing top-level `volumes:` section.
Do not replace the existing compose file.

## Start Spark

From the project root:

```powershell
docker compose -f .\docker-compose.yml config
docker compose -f .\docker-compose.yml build spark
docker compose -f .\docker-compose.yml up -d spark
docker compose -f .\docker-compose.yml logs -f spark
```

The first Spark build/download can take several minutes.

## Query the output

After Spark has completed at least one five-minute event-time window and
persisted it, query PostgreSQL:

```powershell
docker exec -it streamforge-postgres psql -U streamforge -d streamforge
```

Then:

```sql
SELECT *
FROM streamforge_event_metrics
ORDER BY window_start DESC
LIMIT 20;
```

Exit with `\q`.

## Important behavior

- Spark reads Kafka from the Docker network address `kafka:9092`.
- The Windows producer continues using `localhost:29092`.
- Five-minute event-time windows use a ten-minute watermark.
- Append mode emits a window after Spark considers it complete.
- Checkpoints are persisted in a Docker named volume.
- This starter uses append-only JDBC writes. For production-grade exactly-once
  database semantics, add a unique window key and PostgreSQL upsert/merge logic.
