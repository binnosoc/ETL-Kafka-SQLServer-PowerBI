from kafka_producer import produce_metrics
from kafka_consumer import consume_and_load
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', choices=['producer', 'consumer'], required=True, help="Specify role: producer or consumer")
    args = parser.parse_args()

    if args.role == 'producer':
        produce_metrics()
    elif args.role == 'consumer':
        consume_and_load()
