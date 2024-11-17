import json
import time
from confluent_kafka import Producer
from metrics_collector import collect_metrics

# Configuration Kafka
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'metrics_data'

def produce_metrics():
    """
    Produit les messages dans Kafka en collectant les métriques système toutes les 5 secondes.
    """
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    try:
        while True:
            metrics = collect_metrics()
            message = json.dumps(metrics)
            try:
                producer.produce(TOPIC, message)
                producer.flush()  # Envoi des messages
                print(f"Produced: {metrics}")
            except Exception as e:
                print(f"Error producing message: {e}")
            
            time.sleep(5)  # Envoi toutes les 5 secondes
    except KeyboardInterrupt:
        print("Producer stopped.")
