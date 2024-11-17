import json
import pyodbc
from confluent_kafka import Consumer

# Configuration Kafka et Base de données
KAFKA_BROKER = "localhost:9092"
TOPIC = "metrics_data"


def consume_and_load():
    """
    Consomme les messages Kafka et les insère dans une base de données SQL Server.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "metrics_group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    # Connexion à la base de données
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=DESKTOP-DRMJM5I\\SQLEXPRESS;"
        "Database=MetricsDB;"
        "Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Création de la table si elle n'existe pas
    cursor.execute(
        """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Metrics' AND xtype='U')
    CREATE TABLE Metrics (
        id INT IDENTITY(1,1) PRIMARY KEY,
        cpu_percent FLOAT,
        memory_percent FLOAT,
        disk_usage_percent FLOAT,
        timestamp FLOAT
    )
    """
    )
    conn.commit()

    try:
        while True:
            msg = consumer.poll(1.0)  # Attendre un message pendant 1 seconde
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            print(f"Consumed: {data}")

            # Insertion dans la base de données
            cursor.execute(
                """
            INSERT INTO Metrics (cpu_percent, memory_percent, disk_usage_percent, timestamp)
            VALUES (?, ?, ?, ?)
            """,
                data["cpu_percent"],
                data["memory_percent"],
                data["disk_usage_percent"],
                data["timestamp"],
            )
            conn.commit()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
