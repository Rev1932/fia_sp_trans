import os
import pickle
import requests
from cryptography.fernet import Fernet
from .CRIP import chave_
from .kafka_confluent import KafkaAdmin
from .kafka_service import KafkaService
from .avro_registry import Avro
from .cache import Cache

user = 'datawakeroot' # os.getenv("USERNAME_API")
password = 'tFnCc2*bEH885K@BxV86' # os.getenv('PASSWORD_API')


class DatawakeFetcher:
    def __init__(self):
        self.url_token = 'https://dw-frm-api-dev.datawake.cloud/api/User/token'
        self.url = 'https://services.datawake.cloud/api/Action/run/9cbd6b21-1d70-4674-b0d1-34d3545e4990'

        self.paramentros = {
            "tenanty": "unipac",
            "email": "datawake@datawake.com.br",
            "password": "Datawake@123"
        }
        self.headers = {"Content-Type": "application/json"}
        self.token = self.get_token()
        self.headers_auth = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        # TODO AQUI VAI PRECISAR MUDAR QUANDO TROCAR OS CERTIFICADOS

        self.kafka_ali = KafkaService("broker-1.datawake.cloud:49093",
                                      'wildcard_datadriven_cloud.key',
                                      'wildcard_datadriven_cloud.crt', 'SASL_SSL')
        self.cache = Cache()
        self.avro_ali = Avro("https://registry-1.datawake.cloud:49086", user, password)
        self.kafka_confluent_ali = KafkaAdmin('broker-1.datawake.cloud:49093',
                                              'wildcard_datadriven_cloud.key',
                                              'wildcard_datadriven_cloud.crt', 'SASL_SSL')

    def get_token(self):
        response = requests.post(self.url_token, json=self.paramentros, headers=self.headers, verify=False)
        # print("Response text:", response.text)
        if response.status_code == 200:
            token = response.json().get('data', {}).get('accessToken')
            if token:
                print("Token obtido com sucesso.")
                return token
        raise Exception(f"Erro ao obter token: {response.status_code} - {response.text}")

    def fetch_data(self, url, schema_name):
        body = [
            {
                "schemaName": f"{schema_name}",
                "companySiteId": "da275923-b8c0-43fa-b310-a53287c68e81"     # todo paulinia
            }
        ]
        response = requests.post(url, headers=self.headers_auth, verify=False, json=body)
        print("Response text:", response.text)
        print(response.status_code)

        if response.status_code == 200:
            data = response.json().get("data", {})
            # Usa o schema_name para pegar a chave certa
            registros = data.get(schema_name, {}).get("data", [])
            print(f"Registros encontrados em {schema_name}: {registros}")
            return registros
        else:
            print(f"Erro ao consultar {url}: {response.status_code} - {response.text}")
        return []

    @staticmethod
    def descriptografar_env_local():
        fernet = Fernet(chave_)
        with open('.env', 'rb') as f:
            return fernet.decrypt(f.read()).decode()

    @staticmethod
    def criptografar_env_local(novo_conteudo):
        fernet = Fernet(chave_)
        criptografado = fernet.encrypt(novo_conteudo.encode())
        with open('.env', 'wb') as f:
            f.write(criptografado)
        print(".env criptografado novamente.")

    def atualizar_env_se_necessario(self, valor_novo_env):
        if not valor_novo_env:
            print("Nenhum valor retornado para atualização do .env.")
            return

        # Normaliza quebras de linha e mantém a ordem original recebida da API
        linhas = valor_novo_env.replace("\r\n", "\n").strip().splitlines()
        novo_conteudo_env = "\n".join(linhas)

        # Sempre sobrescreve, apagando tudo que tinha antes
        self.criptografar_env_local(novo_conteudo_env)
        print(".env sobrescrito com o novo conteúdo.")

    def reprocessa(self, empresa, tabelas_extra):
        if not tabelas_extra:
            print("Nenhuma tabela encontrada para reprocessamento.")
            return

        if not os.path.exists("cache.pkl"):
            print("Arquivo de cache não encontrado.")
            return

        with open("cache.pkl", "rb") as f:
            data = pickle.load(f)

        for topico in tabelas_extra:
            if topico != '*':
                topico_completo = f"{empresa}_{topico}"
                print(f"Reprocessando: {topico_completo}")

                self.kafka_confluent_ali.deletar_topico(topico_completo)
                self.kafka_confluent_ali.criar_topico(topico_completo, 1, 1)
                self.avro_ali.delete_schema(topico_completo.replace(f"{empresa}_", ""))

                if f"{topico_completo}_id" in data:
                    del data[f"{topico_completo}_id"]
                    print(f"Removido: {topico_completo}_id")

                if f"{topico_completo}_dt_atualizacao" in data:
                    del data[f"{topico_completo}_dt_atualizacao"]
                    print(f"Removido: {topico_completo}_dt_atualizacao")
            else:
                print("Reprocessando todos os tópicos por prefixo...")
                self.cache.clear()

        with open("cache.pkl", "wb") as f:
            pickle.dump(data, f)
        print("Reprocessamento finalizado.")

    def processar_integracoes(self):
        itens_env = self.fetch_data(self.url, 'TabelasEnv')
        itens_reprocessa = self.fetch_data(self.url, 'TabelasDataBee')
        for item in itens_env:
            valor_env = item.get("env", "")
            if valor_env:
                print(f"Atualizando .env com valor: {valor_env}")
                self.atualizar_env_se_necessario(valor_env)

        for item in itens_reprocessa:
            integracao = item.get("integracao")
            if integracao:
                tabelas = item.get('tabelas')
                print(f"Reprocessando cache/tópico para integracao: {integracao}")
                self.reprocessa("paulinia", [tabelas])


if __name__ == "__main__":
    fetcher = DatawakeFetcher()
    fetcher.processar_integracoes()
