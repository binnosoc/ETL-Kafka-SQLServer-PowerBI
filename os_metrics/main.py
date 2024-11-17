from kafka_producer import *
from kafka_consumer import *
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', choices=['producer-wifi','producer-system','producer-process', 'consumer-wifi', 'consumer-system', 'consumer-process'], required=True, help="Specify role: producer or consumer")
    args = parser.parse_args()

    if args.role == 'producer-wifi':
        produce_metrics_wifi()
    elif args.role == 'producer-system':
        produce_metrics_system()
    elif args.role == 'producer-process':
        produce_metrics_process()
    elif args.role == 'consumer-wifi':
        consume_and_load_wifi()
    elif args.role == 'consumer-system':
        consume_and_load_system()
    elif args.role == 'consumer-process':
        consume_and_load_process()
