#!/usr/bin/env python3
from prometheus_client import start_http_server, Gauge, Counter
import requests
import time
import os
import sys

# Конфигурация
KAFKA_CONNECT_URL = os.getenv('KAFKA_CONNECT_URL', 'http://kafka-connect:8083')
PORT = int(os.getenv('PORT', '8084'))
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '10'))

# Метрики
connect_up = Gauge('kafka_connect_up', 'Kafka Connect availability')
connector_state = Gauge('kafka_connect_connector_state',
                       'Connector state (1=RUNNING, 0=other)',
                       ['connector', 'type', 'state'])
task_count = Gauge('kafka_connect_task_count',
                  'Number of tasks per connector',
                  ['connector'])
task_state = Gauge('kafka_connect_task_state',
                  'Task state (1=RUNNING, 0=other)',
                  ['connector', 'task_id'])
connector_health = Gauge('kafka_connect_connector_healthy',
                        'Connector health status',
                        ['connector'])

def get_connectors():
    """Get list of all connectors"""
    try:
        resp = requests.get(f'{KAFKA_CONNECT_URL}/connectors', timeout=5)
        resp.raise_for_status()
        connect_up.set(1)
        return resp.json()
    except Exception as e:
        print(f"Error getting connectors: {e}")
        connect_up.set(0)
        return []

def get_connector_status(connector):
    """Get connector status"""
    try:
        resp = requests.get(f'{KAFKA_CONNECT_URL}/connectors/{connector}/status', timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error getting status for {connector}: {e}")
        return None

def get_connector_config(connector):
    """Get connector config"""
    try:
        resp = requests.get(f'{KAFKA_CONNECT_URL}/connectors/{connector}', timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error getting config for {connector}: {e}")
        return None

def collect_metrics():
    """Collect all metrics"""
    connectors = get_connectors()

    # Reset metrics
    for metric in [connector_state, task_count, task_state, connector_health]:
        metric.clear()

    for connector in connectors:
        # Get connector status
        status = get_connector_status(connector)
        if not status:
            continue

        # Get connector config
        config = get_connector_config(connector)
        connector_type = 'unknown'
        if config and 'config' in config:
            connector_type = config['config'].get('connector.class', 'unknown').split('.')[-1]

        # Connector state
        state = status['connector']['state']
        connector_state.labels(
            connector=connector,
            type=connector_type,
            state=state
        ).set(1 if state == 'RUNNING' else 0)

        # Connector health
        health = 1 if state == 'RUNNING' else 0
        connector_health.labels(connector=connector).set(health)

        # Task count
        tasks = status.get('tasks', [])
        task_count.labels(connector=connector).set(len(tasks))

        # Task states
        for task in tasks:
            task_id = str(task['id'])
            task_status = task['state']
            task_state.labels(
                connector=connector,
                task_id=task_id
            ).set(1 if task_status == 'RUNNING' else 0)

def main():
    print(f"Starting Kafka Connect Exporter on port {PORT}")
    print(f"Scraping Kafka Connect at {KAFKA_CONNECT_URL}")

    # Start HTTP server
    start_http_server(PORT)

    # Main loop
    while True:
        try:
            collect_metrics()
        except Exception as e:
            print(f"Error in collect_metrics: {e}")

        time.sleep(SCRAPE_INTERVAL)

if __name__ == '__main__':
    main()