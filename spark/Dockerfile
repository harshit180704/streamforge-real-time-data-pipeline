FROM apache/spark:4.0.1

USER root

RUN pip install --no-cache-dir psycopg2-binary==2.9.10

RUN mkdir -p /opt/spark/work-dir/.ivy2 /opt/spark-checkpoints \
    && chown -R spark:spark /opt/spark/work-dir /opt/spark-checkpoints

WORKDIR /opt/streamforge

COPY --chown=spark:spark src/stream_job.py /opt/streamforge/stream_job.py

USER spark

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--conf", "spark.jars.ivy=/opt/spark/work-dir/.ivy2", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.7.7", "/opt/streamforge/stream_job.py"]