from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'teste',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Lê a partir do início do tópico
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("📡 Consumidor conectado! Aguardando mensagens...\n")

for msg in consumer:
    print(f"Mensagem recebida no tópico {msg.topic}:")
    print(msg.value)
    print("-" * 50)
