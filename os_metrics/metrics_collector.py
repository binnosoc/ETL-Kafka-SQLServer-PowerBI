import psutil
import time
import subprocess
import re

def get_wifi_signal():
    try:
        # Exécute la commande netsh avec encodage explicite
        result = subprocess.run(
            ["netsh", "wlan", "show", "interfaces"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True
        )
        output = result.stdout
        
        if not output:  # Vérifie si la sortie est vide
            print("Aucune sortie n'a été retournée par la commande.")
            return None

        # Analyse le résultat pour extraire les informations
        metrics = {}
        for line in output.splitlines():
            if "SSID" in line and not "BSSID" in line:  # Nom du réseau
                metrics["SSID"] = line.split(":")[1].strip()
            elif "Signal" in line:  # Puissance du signal
                metrics["Signal"] = line.split(":")[1].strip()

        return metrics["Signal"]
    
    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'exécution de la commande netsh :", e)
        return None
    except Exception as ex:
        print("Une erreur inattendue s'est produite :", ex)
        return None

def collect_wifi_metrics():
    """
    Récupère les métriques de l'interface Wi-Fi : octets et paquets envoyés/reçus.
    """
    try:
        # Liste des interfaces réseau
        net_io = psutil.net_io_counters(pernic=True)

        # Trouver l'interface Wi-Fi (à adapter si nécessaire)
        wifi_interface = None
        for interface in net_io.keys():
            if "Wi-Fi" in interface or "wlan" in interface.lower():
                wifi_interface = interface
                break

        if not wifi_interface:
            raise ValueError("Interface Wi-Fi introuvable")

        # Récupérer les statistiques
        stats = net_io[wifi_interface]
        metrics = {
            "bytes_sent": stats.bytes_sent,
            "bytes_recv": stats.bytes_recv,
            "packets_sent": stats.packets_sent,
            "packets_recv": stats.packets_recv,
            "signal": get_wifi_signal(),
        }

        return metrics
    except Exception as e:
        print(f"Erreur lors de la collecte des métriques Wi-Fi: {e}")
        return None

def collect_system_metrics():
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

def collect_process_metrics():
    """
    Collecte les métriques des processus actifs.
    """
    metrics = []
    for proc in psutil.process_iter(attrs=['pid', 'name', 'username']):
        try:
            process_info = {
                "pid": proc.info['pid'],
                "name": proc.info['name'],
                "username": proc.info['username'],
                "cpu_percent": proc.cpu_percent(interval=0.1),
                "memory_percent": proc.memory_percent(),
                "rss_memory": proc.memory_info().rss,
                "vms_memory": proc.memory_info().vms,
                "threads": proc.num_threads(),
                "status": proc.status(),
                "create_time": proc.create_time(),
            }
            metrics.append(process_info)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Ignorer les processus qui ne peuvent pas être accédés
            continue
    return metrics

# print(collect_process_metrics())