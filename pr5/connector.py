import requests
import json

config = {
    "name": "postgres-cdc-final",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "user",
      "database.password": "password",
      "database.dbname": "test",
      "database.server.name": "final-server",
      "topic.prefix": "final-server",
      "table.include.list": "public.users,public.orders",
      "plugin.name": "pgoutput",
      "slot.name": "final_slot_01"
    }
  }

response = requests.post(
    "http://localhost:8089/connectors",
    headers={"Content-Type": "application/json"},
    json=config
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")