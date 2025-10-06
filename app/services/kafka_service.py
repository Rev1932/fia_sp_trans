import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from .log import Logger


url_api = 'https://api-logs-2.datawake.cloud:9401'

sistema = 'Data-Bee'
cliente = 'Paulinia'

# Load .env
load_dotenv()
user = 'datawakeroot' # os.getenv("USERNAME_API")
password = 'tFnCc2*bEH885K@BxV86' # os.getenv('PASSWORD_API')
logger = Logger(url_api, user, password)


class KafkaService:
    """Class for handling Kafka related services"""

    def __init__(self, server, name_key, name_crt, protocolo):
        self.producer = None

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=server,
                security_protocol=protocolo,
                sasl_mechanism='PLAIN',
                sasl_plain_username='datawake',
                sasl_plain_password='cOBuv2glsIzu',
                ssl_check_hostname=False,
                ssl_cafile=name_crt,
                ssl_certfile=name_crt,
                ssl_keyfile=name_key,
                # compression_type="lz4"
            )
        except KafkaError as e:
            print('AQUII', e)
            logger.log_erro(sistema=sistema, rotina='[kafka_service] - Error: Conexao com o kafka',
                            mensagem=f'Erro ao conectar com o broker do kafka: {e}', cliente=cliente)

    def send_message(self, topic: str, message: bytes) -> bool:
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
            logger.log_erro(sistema=sistema,
                            rotina='[kafka_service] - Error: KafkaProducer is not initialized. Cannot send messages',
                            mensagem=f'Erro ao manter conexao com o Kafka', topico=topic,
                            tabela=topic.replace('paranoa_', ""), cliente=cliente)
            KafkaService("broker-1.datawake.cloud:49093",
                         'wildcard_datadriven_cloud.key',
                         'wildcard_datadriven_cloud.crt', 'SASL_SSL')
            print("[kafka_service] - Error: KafkaProducer is not initialized. Cannot send messages")
            return False

        try:
            future = self.producer.send(topic=topic, value=message)
            future.get(timeout=30)
            return True
        except KafkaError as e:
            logger.log_erro(sistema=sistema, rotina='[kafka_service] - Error: Enviar mensagem',
                            mensagem=f'Erro ao enviar a mensagem pro tpico do kafka: {e}', topico=topic,
                            tabela=topic.replace('paranoa_', ""), cliente=cliente)
            print('Erro ao enviar a mensagem pro tpico do kafka: {e}')
            return False

    def close(self):
        """Close the Kafka Producer"""
        if self.producer:
            self.producer.close()


if __name__ == "__main__":
    kafka = KafkaService(os.getenv('KAFKA_BOOTSTRAP_SERVER'))

    # Testando envio de mensagem com bytes serializados (exemplo fictício)
    test_message = b'\x01\x02\x03'  # Mensagem já serializada com Avro
    kafka.send_message('michel-testes', test_message)

    kafka.close()
