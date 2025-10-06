from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from .log import Logger
from cryptography.fernet import Fernet
from io import StringIO
from .CRIP import chave_
import os

sistema = 'Data-Bee'
cliente = 'Paulinia'


def descriptografar_env(chave):
    fernet = Fernet(chave)
    # ..\.env
    with open('.env', 'rb') as arquivo_criptografado:
        conteudo_criptografado = arquivo_criptografado.read()

    conteudo_descriptografado = fernet.decrypt(conteudo_criptografado)

    with StringIO(conteudo_descriptografado.decode()) as temp_env:
        load_dotenv(stream=temp_env)


descriptografar_env(chave_)
load_dotenv()
url_api = 'https://api-logs-2.datawake.cloud:9401'

user = 'datawakeroot' # os.getenv("USERNAME_API")
password = 'tFnCc2*bEH885K@BxV86' # os.getenv('PASSWORD_API')
logger = Logger(url_api, user, password)


class KafkaAdmin:
    def __init__(self, server, name_key, name_crt, protocolo):
        try:
            self.admin_client = AdminClient({
                'bootstrap.servers': server,
                'security.protocol': protocolo,
                'sasl.mechanism': 'PLAIN',
                'sasl.username': 'datawake',
                'sasl.password': 'cOBuv2glsIzu',
                'ssl.ca.location': name_crt,    # name_crt
                'ssl.certificate.location': name_crt,
                'ssl.key.location': name_key

            })
            print('con')
        except Exception as e:
            print(e)
            logger.log_erro(sistema=sistema, rotina='[kafka_confluent] - Conexao com o kafka',
                            mensagem=f'Erro ao conectar com o broker do kafka: {e}', cliente=cliente)

    def criar_topico(self, topico, num_particoes=1, num_replicas=1):
        novo_topico = [NewTopic(topico, num_particoes, num_replicas)]
        resposta = self.admin_client.create_topics(novo_topico)

        for topico, futuro in resposta.items():
            try:
                futuro.result()
                print(f'Tópico {topico} criado com sucesso.')
                return True
            except Exception as e:
                print(f'Falha ao criar o tópico {topico}: {e}')
                return False

    def deletar_topico(self, topico):
        resposta = self.admin_client.delete_topics([topico])

        for topico, futuro in resposta.items():
            try:
                futuro.result()
                print(f'Tópico {topico} deletado com sucesso.')
                return True
            except Exception as e:
                print(f'Falha ao deletar o tópico {topico}: {e}')
                return False

    def listar_topicos_paranoa(self, empresa):
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            topicos = [t for t in metadata.topics.keys() if t.startswith(empresa)]
            print(f'Tópicos que começam com "{empresa}": {topicos}')
            return topicos
        except Exception as e:
            print('AQUI', e)
            logger.log_erro(sistema=sistema, rotina='[kafka_confluent] - Listar tópicos',
                            mensagem=f'Erro ao listar tópicos: {e}', cliente=cliente)
            return []

    def deletar_topicos_por_prefixo(self, prefixo):
        try:
            todos_topicos = self.admin_client.list_topics()

            topicos_para_deletar = [topico for topico in todos_topicos if topico.startswith(prefixo)]

            if not topicos_para_deletar:
                print(f'Nenhum tópico encontrado com o prefixo "{prefixo}".')
                return False

            resposta = self.admin_client.delete_topics(topicos_para_deletar)

            sucesso = True
            for topico, futuro in resposta.items():
                try:
                    futuro.result()
                    print(f'Tópico {topico} deletado com sucesso.')
                except Exception as e:
                    print(f'Falha ao deletar o tópico {topico}: {e}')
                    sucesso = False

            return sucesso

        except Exception as e:
            print(f'Erro ao tentar deletar os tópicos com prefixo "{prefixo}": {e}')
            return False


if __name__ == '__main__':
    # kafka_admin = KafkaAdmin(os.getenv('KAFKA_BOOTSTRAP_SERVER'))
    kafka_admin = KafkaAdmin('broker-1.datawake.cloud:49093', 'broker-1.key', 'broker-1.crt', 'SASL_SSL')
    ts = []

    for t in ts:
        kafka_admin.deletar_topico(t)
    # Apagar um tópico
    # kafka_admin.deletar_topico('test')

    # Criar um tópico
    # kafka_admin.criar_topico('test', 1, 1)

    # Consumir um topico
    # kafka_admin.visualizar_topico('seg_dw_turno')
    kafka_admin.listar_topicos_paranoa('Paulinia')
