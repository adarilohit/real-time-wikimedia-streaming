import json
import requests
import sseclient
from kafka import KafkaProducer

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Wikimedia SSE stream
URL = "https://stream.wikimedia.org/v2/stream/recentchange"
response = requests.get(URL, stream=True)
client = sseclient.SSEClient(response.raw)

print("Streaming Wikimedia events → Kafka...")

for event in client.events():
    if event.event == "message":
        data = json.loads(event.data)
        producer.send("wikimedia.recent change", data)
