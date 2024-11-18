import json
import pyodbc
from confluent_kafka import Consumer
from datetime import datetime

# Configuration Kafka et Base de données
KAFKA_BROKER = "localhost:9092"
TOPIC_SYSTEM = "metrics_system"
TOPIC_WIFI = "metrics_wifi"
TOPIC_PROCESS= "metrics_process"

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "metrics_group",
        "auto.offset.reset": "earliest",
    }
)

# Connexion à la base de données
conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-DRMJM5I\\SQLEXPRESS;"
    "Database=MetricsDB;"
    "Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)

def consume_and_load_wifi():
    global consumer, conn
    """
    Consomme les messages Kafka pour les métriques Wi-Fi et les insère dans une base de données SQL Server.
    """

    consumer.subscribe([TOPIC_WIFI])
    
    cursor = conn.cursor()

    # Création de la table si elle n'existe pas
    cursor.execute(
        """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WifiMetrics' AND xtype='U')
    CREATE TABLE WifiMetrics (
        id INT IDENTITY(1,1) PRIMARY KEY,
        bytes_sent BIGINT,
        bytes_recv BIGINT,
        packets_sent BIGINT,
        packets_recv BIGINT,
        signal NVARCHAR(10),
        timestamp DATETIME DEFAULT GETDATE()
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
            INSERT INTO WifiMetrics (bytes_sent, bytes_recv, packets_sent, packets_recv, signal)
            VALUES (?, ?, ?, ?, ?)
            """,
                data["bytes_sent"],
                data["bytes_recv"],
                data["packets_sent"],
                data["packets_recv"],
                data["signal"],
            )
            conn.commit()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()       

def consume_and_load_system():
    global consumer, conn
    """
    Consomme les messages Kafka et les insère dans une base de données SQL Server.
    """

    consumer.subscribe([TOPIC_SYSTEM])
    
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
        timestamp DATETIME  
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
                datetime.fromtimestamp(data["timestamp"]),
            )
            conn.commit()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        
def consume_and_load_process0():
    global consumer, conn
    """
    Consomme les messages Kafka pour les métriques des processus et les insère dans une base de données SQL Server.
    """

    consumer.subscribe([TOPIC_PROCESS])
    
    cursor = conn.cursor()
    # Supprimer la table si elle existe
    cursor.execute("IF EXISTS (SELECT * FROM sysobjects WHERE name='ProcessMetrics' AND xtype='U') DROP TABLE ProcessMetrics")
    conn.commit()

    # Créer à nouveau la table
    cursor.execute(
        """
        CREATE TABLE ProcessMetrics (
            id INT IDENTITY(1,1) PRIMARY KEY,
            pid INT,
            name NVARCHAR(255),
            username NVARCHAR(255),
            cpu_percent FLOAT,
            memory_percent FLOAT,
            rss_memory BIGINT,
            vms_memory BIGINT,
            threads INT,
            status NVARCHAR(50),
            create_time DATETIME,
            timestamp DATETIME DEFAULT GETDATE()
        )
        """
    )
    conn.commit()

    try:
        while True:
            msg = consumer.poll(30.0)  # Attendre un message pendant 1 seconde
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            print(f"Consumed: {data}")
            
            # Boucle pour insérer chaque entrée de la liste data
            for entry in data:
                cursor.execute(
                    """
                    INSERT INTO ProcessMetrics (pid, name, username, cpu_percent, memory_percent, rss_memory, 
                    vms_memory, threads, status, create_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    entry["pid"],
                    entry["name"],
                    entry["username"] if entry["username"] else None,  # Gérer le cas où username est None
                    entry["cpu_percent"],
                    entry["memory_percent"],
                    entry["rss_memory"],
                    entry["vms_memory"],
                    entry["threads"],
                    entry["status"],
                    datetime.fromtimestamp(entry["create_time"])
                    
                )
            
            conn.commit()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()         


def consume_and_load_process():
    global consumer, conn
    """
    Consomme les messages Kafka pour les métriques des processus et les insère dans une base de données SQL Server.
    """

    consumer.subscribe([TOPIC_PROCESS])
    
    cursor = conn.cursor()

    # Supprimer la table si elle existe
    cursor.execute("IF OBJECT_ID('dbo.ProcessMetrics', 'U') IS NOT NULL DROP TABLE dbo.ProcessMetrics")
    conn.commit()

    # Créer à nouveau la table
    cursor.execute(
        """
        CREATE TABLE ProcessMetrics (
            id INT IDENTITY(1,1) PRIMARY KEY,
            pid INT,
            name NVARCHAR(255),
            username NVARCHAR(255),
            cpu_percent FLOAT,
            memory_percent FLOAT,
            rss_memory BIGINT,
            vms_memory BIGINT,
            threads INT,
            status NVARCHAR(50),
            create_time DATETIME  ,
            timestamp DATETIME DEFAULT GETDATE()
        )
        """
    )
    conn.commit()

    try:
        while True:
            msg = consumer.poll(5.0)  # Attendre un message pendant 5 secondes
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Charger les données JSON
            try:
                data = json.loads(msg.value().decode("utf-8"))
                print(f"Consumed: {data}")
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                continue

            # Vérifier si data est une liste
            if not isinstance(data, list):
                print(f"Unexpected data format: {data}")
                continue

            # Préparer les valeurs pour insertion en batch
            values = [
                (
                    entry["pid"],
                    entry["name"],
                    entry["username"] if entry["username"] else None,  # Gérer le cas où username est None
                    entry["cpu_percent"],
                    entry["memory_percent"],
                    entry["rss_memory"],
                    entry["vms_memory"],
                    entry["threads"],
                    entry["status"],
                    datetime.fromtimestamp(entry["create_time"])
                )
                for entry in data
            ]

            # Insertion batch
            try:
                cursor.executemany(
                    """
                    INSERT INTO ProcessMetrics (pid, name, username, cpu_percent, memory_percent, rss_memory, 
                    vms_memory, threads, status, create_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values
                )
                conn.commit()
            except Exception as e:
                print(f"Database error during batch insert: {e}")
                conn.rollback()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        cursor.close()
        conn.close()
        consumer.close()

