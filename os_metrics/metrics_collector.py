import psutil
import time
import json

def collect_metrics():
    """
    Collecte les métriques système : CPU, mémoire, disque, et horodatage.
    """
    metrics = {
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_usage_percent": psutil.disk_usage('/').percent,
        "timestamp": time.time()
    }
    return metrics
print(json.dumps(collect_metrics()))
