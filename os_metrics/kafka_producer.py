import json
import time
from confluent_kafka import Producer
from metrics_collector import *

# Configuration Kafka
KAFKA_BROKER = 'localhost:9092'
TOPIC_WIFI = 'metrics_wifi'
TOPIC_SYSTEM = 'metrics_system'
TOPIC_PROCESS = 'metrics_process'

def produce_metrics_system():
    """
    Produit les messages dans Kafka en collectant les métriques système toutes les 5 secondes.
    """
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    try:
        while True:
            metrics = collect_system_metrics()
            message = json.dumps(metrics)
            try:
                producer.produce(TOPIC_SYSTEM, message)
                producer.flush()  # Envoi des messages
                print(f"Produced: {metrics}")
            except Exception as e:
                print(f"System : Error producing message: {e}")
            
            time.sleep(5)  # Envoi toutes les 5 secondes
    except KeyboardInterrupt:
        print("Producer stopped.")


def produce_metrics_wifi():
    """
    Produit les messages dans Kafka en collectant les métriques WI-FI toutes les 5 secondes.
    """
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    try:
        while True:
            metrics = collect_wifi_metrics()
            message = json.dumps(metrics)
            try:
                producer.produce(TOPIC_WIFI, message)
                producer.flush()  # Envoi des messages
                print(f"Produced: {metrics}")
            except Exception as e:
                print(f"Wi-Fi : Error producing message: {e}")
            
            time.sleep(5)  # Envoi toutes les 5 secondes
    except KeyboardInterrupt:
        print("Producer stopped.")


def produce_metrics_process():
    """
    Produit les messages dans Kafka en collectant les métriques process toutes les 1 minutes.
    """
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    try:
        while True:
            metrics = collect_process_metrics()
            message = json.dumps(metrics)
            try:
                producer.produce(TOPIC_PROCESS, message)
                producer.flush()  # Envoi des messages
                print(f"Produced: {metrics}")
            except Exception as e:
                print(f"Process: Error producing message: {e}")
            
            time.sleep(60)  # Envoi toutes les 60 secondes
    except KeyboardInterrupt:
        print("Producer stopped.")
