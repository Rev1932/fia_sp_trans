from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from confluent_kafka.cimpl import KafkaError

# O endereço do seu broker (o mesmo do teste)
BROKER_ADDRESS = 'localhost:9092'

def criar_topico(broker_address, nome_topico, num_particoes=1, fator_replicacao=1):
    """
    Tenta criar um novo tópico no cluster Kafka.
    
    :param broker_address: Endereço do broker (ex: 'localhost:9092')
    :param nome_topico: O nome do tópico a ser criado
    :param num_particoes: Número de partições (default=1)
    :param fator_replicacao: Fator de replicação (default=1)
    """
    
    # 1. Configura e cria o AdminClient
    admin_client = AdminClient({'bootstrap.servers': broker_address})
    
    # 2. Define a especificação do novo tópico
    novo_topico = NewTopic(
        topic=nome_topico,
        num_partitions=num_particoes,
        replication_factor=fator_replicacao
    )
    
    print(f"Tentando criar o tópico '{nome_topico}' com {num_particoes} partição(ões) e replicação {fator_replicacao}...")
    
    # 3. Envia a requisição para criar o tópico
    #    create_topics retorna um dicionário de "futures"
    resultados_futuros = admin_client.create_topics([novo_topico])
    
    try:
        # 4. Espera o resultado da criação para este tópico específico
        future = resultados_futuros[nome_topico]
        future.result()  # Bloqueia até a operação ser concluída ou falhar
        
        print(f"\n--- SUCESSO! ---")
        print(f"Tópico '{nome_topico}' foi criado.")
        
    except KafkaException as e:
        # 5. Trata os erros mais comuns
        erro = e.args[0]
        if erro.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            print(f"\n--- INFO ---")
            print(f"O tópico '{nome_topico}' já existe. Nenhuma ação foi tomada.")
        elif erro.code() == KafkaError.INVALID_REPLICATION_FACTOR:
            print(f"\n--- ERRO DE CONFIGURAÇÃO ---")
            print(f"Fator de replicação inválido ({fator_replicacao}).")
            print("Provavelmente seu cluster só tem 1 broker, então o 'fator_replicacao' deve ser 1.")
        else:
            print(f"\n--- FALHA! ---")
            print(f"Não foi possível criar o tópico: {erro}")
    except Exception as e:
        print(f"\n--- ERRO INESPERADO! ---")
        print(f"Erro: {e}")

# --- Como usar a função ---
if __name__ == "__main__":
    
    # O tópico que você quer criar para sua API
    TOPICO_DA_API = "dados-da-api"
    
    # Aviso: O fator_replicacao DEVE ser 1, pois seu docker-compose só tem 1 broker.
    PARTICOES = 1
    REPLICACAO = 1 
    
    criar_topico(BROKER_ADDRESS, TOPICO_DA_API, PARTICOES, REPLICACAO)