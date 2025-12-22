#!/usr/bin/env python3
"""
Kafka Producer - Simulates User Events

This script generates fake user events and sends them to a Kafka topic.
Events include: click, view, purchase, login, logout

Usage:
    python producer.py [--bootstrap-servers localhost:9092] [--topic user_events] [--interval 1.0]
"""

import json
import random
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer


# Event types with their relative weights (probability)
EVENT_TYPES = {
    'click': 40,
    'view': 35,
    'purchase': 5,
    'login': 10,
    'logout': 10
}


def generate_event():
    """Generate a random user event."""
    event_type = random.choices(
        list(EVENT_TYPES.keys()),
        weights=list(EVENT_TYPES.values()),
        k=1
    )[0]
    
    event = {
        'user_id': random.randint(1, 1000),
        'event_type': event_type,
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        'page_id': random.randint(1, 100) if event_type in ['click', 'view'] else None,
        'product_id': random.randint(1, 500) if event_type == 'purchase' else None,
        'session_id': f"sess_{random.randint(10000, 99999)}"
    }
    
    # Remove None values for cleaner JSON
    return {k: v for k, v in event.items() if v is not None}


def create_producer(bootstrap_servers):
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )


def main():
    parser = argparse.ArgumentParser(description='Kafka User Event Producer')
    parser.add_argument(
        '--bootstrap-servers',
        default='kafka:29092',
        help='Kafka bootstrap servers (default: kafka:29092 for Docker)'
    )
    parser.add_argument(
        '--topic',
        default='user_events',
        help='Kafka topic name (default: user_events)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Interval between events in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=0,
        help='Number of events to produce (0 = infinite, default: 0)'
    )
    
    args = parser.parse_args()
    
    print(f"Starting Kafka producer...")
    print(f"  Bootstrap servers: {args.bootstrap_servers}")
    print(f"  Topic: {args.topic}")
    print(f"  Interval: {args.interval}s")
    print(f"  Count: {'infinite' if args.count == 0 else args.count}")
    print("-" * 50)
    
    # Wait for Kafka to be ready
    producer = None
    retries = 0
    max_retries = 30
    
    while producer is None and retries < max_retries:
        try:
            producer = create_producer(args.bootstrap_servers)
            print("Connected to Kafka!")
        except Exception as e:
            retries += 1
            print(f"Waiting for Kafka... ({retries}/{max_retries})")
            time.sleep(2)
    
    if producer is None:
        print("ERROR: Could not connect to Kafka after multiple retries")
        return 1
    
    try:
        events_sent = 0
        while True:
            event = generate_event()
            
            # Use user_id as the partition key for ordering
            key = str(event['user_id'])
            
            # Send event to Kafka
            future = producer.send(args.topic, key=key, value=event)
            
            # Wait for the send to complete
            record_metadata = future.get(timeout=10)
            
            events_sent += 1
            print(f"[{events_sent}] Sent: {event['event_type']} from user {event['user_id']} "
                  f"-> partition {record_metadata.partition}, offset {record_metadata.offset}")
            
            # Check if we've reached the count limit
            if args.count > 0 and events_sent >= args.count:
                print(f"\nReached event limit ({args.count}). Stopping.")
                break
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n\nInterrupted! Total events sent: {events_sent}")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")
    
    return 0


if __name__ == '__main__':
    exit(main())
