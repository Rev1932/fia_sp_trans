from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from confluent_kafka.cimpl import KafkaError


class KafkaAdm():
    def __init__(self, broker_endpoint, topic_name):
        self.broker_endpoint = broker_endpoint
        self.topic_name = topic_name
        self.num_partition = 1
        self.replication_factor = 1

    def criar_topico(self):
        """
        Tenta criar um novo tópico no cluster Kafka.
        """
        
        admin_client = AdminClient({'bootstrap.servers': self.broker_endpoint})
        novo_topico = NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partition,
            replication_factor=self.replication_factor
        )
        resultados_futuros = admin_client.create_topics([novo_topico])
        
        try:
            # 4. Espera o resultado da criação para este tópico específico
            future = resultados_futuros[self.topic_name]
            future.result()  # Bloqueia até a operação ser concluída ou falhar
            print(f"Tópico '{self.topic_name}' foi criado.")
            
        except KafkaException as e:
            # 5. Trata os erros mais comuns
            erro = e.args[0]
            if erro.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"\n--- INFO ---")
                print(f"O tópico '{self.topic_name}' já existe. Nenhuma ação foi tomada.")
            elif erro.code() == KafkaError.INVALID_REPLICATION_FACTOR:
                print(f"\n--- ERRO DE CONFIGURAÇÃO ---")
                print(f"Fator de replicação inválido ({self.replication_factor}).")
            else:
                print(f"\n--- FALHA! ---")
                print(f"Não foi possível criar o tópico: {erro}")
        except Exception as e:
            print(f"\n--- ERRO INESPERADO! ---")
            print(f"Erro: {e}")


kafka = KafkaAdm('localhost:9092', 'teste')
print("Criando topico teste")
kafka.criar_topico()