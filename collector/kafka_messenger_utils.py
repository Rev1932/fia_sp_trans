import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

def _on_send_success(record_metadata):
    logging.debug(f"Msg OK -> Tópico: {record_metadata.topic} [Partição {record_metadata.partition}]")

def _on_send_error(excp):
    logging.error("Falha ao enviar msg para o Kafka", exc_info=excp)

class KafkaMessenger:
    def __init__(self, kafka_servers, topic):
        self.producer = None
        self.kafka_servers = kafka_servers
        self.topic = topic

        try:
            self.producer = KafkaProducer(
                bootstrap_servers = self.kafka_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Confirmação de recebimento
                retries=5,   # Tentar novamente 5 vezes em caso de falha
                linger_ms=20 # Espera 20ms para agrupar mais msgs em um batch
            )
            logging.info("KafkaProducer conectado com sucesso.")
        except KafkaError as e:
            logging.fatal(f"Não foi possível conectar ao Kafka: {e}")
            raise

    def send_message(self, messages: list):
        """
        Send messages to Kafka Broker.

        Args:
            topic (str): The Kafka Topic to send the message.
            message (bytes): The serialized Avro message to send.

        Returns:
            bool: True if the message was sent successfully, False otherwise.

        Example:
            send_message(topic='att_stop', message=b'\x01\x02\x03')
            True
        """
        if not self.producer:
            logging.error("Producer não inicializado. Mensagens não enviadas.")
            return

        if not messages:
            logging.warning(f"Nenhuma mensagem para enviar ao tópico {self.topic}.")
            return

        logging.info(f"Enviando {len(messages)} mensagem(ns) para o tópico: {self.topic}")
        try:
            for msg in messages:
                self.producer.send(self.topic, value=msg).add_callback(_on_send_success).add_errback(_on_send_error)
        except KafkaError as e:
            logging.error(f"Erro ao enviar mensagens para {self.topic}: {e}")

    def flush(self):
        """
        Força o envio de todas as mensagens no buffer.
        Bloqueia até que todas as mensagens sejam enviadas.
        """
        if self.producer:
            logging.info("Forçando envio de mensagens (flush)...")
            self.producer.flush()
            logging.info("Flush concluído.")

    def close(self):
        """Close the Kafka Producer"""
        if self.producer:
            self.producer.close()

mensagem = [{
  "prefixo_veiculo": "2 1778",
  "horario_ult_posicao_api": "2025-11-05T16:20:15Z",
  "latitude": -23.55052,
  "longitude": -46.633301,
  "acessivel_deficiente": "true",
  "letreiro_linha": "5111-10",
  "codigo_linha": 345,
  "horario_captura_api": "16:20",
  "horario_processamento": "2025-11-05T16:21:33.450123"
}]
kafka_messenger = KafkaMessenger('localhost:9092', 'teste')
kafka_messenger.send_message(mensagem)
kafka_messenger.flush()
kafka_messenger.close()