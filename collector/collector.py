import os
import requests
import json
from datetime import datetime
from kafka_messenger_utils import KafkaMessenger
from kafka_topic_utils import KafkaAdm
from dotenv import load_dotenv

#TODO: salvar os dados da API diretamente dentro de um dataframe, ao inves de dicionario
class ApiConection():
    def __init__(self, api_token, api_base_url, api_get_url):
        self.api_token = api_token
        self.api_base_url = api_base_url
        self.api_get_url = api_get_url



    def auth(self) -> requests.Session:
        """
        Autentica na API da SPTrans 
        """
        auth_url = f'{self.api_base_url}/Login/Autenticar?token={self.api_token}'
        session = requests.Session()
        
        try:
            response = session.post(auth_url)
            # Levanta um erro caso a requisição falhe (ex: status 4xx ou 5xx)
            response.raise_for_status() 
            
            if response.text == 'true':
                print("Autenticação bem-sucedida!")
                return session
            else:
                print(f"Falha na autenticação. Resposta: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão durante a autenticação: {e}")
            return None
        
    def get_data(self, session: requests.Session) -> list:
        if not session:
            print("Sessão de autenticação inválida")
            return None
        
        posicao_url = f'{self.api_base_url}/{self.api_get_url}'

        try:
            response = session.get(posicao_url)
            response.raise_for_status()
            dados = response.json()
            return dados
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar posição dos veículos: {e}")
        except json.JSONDecodeError:
            print("Erro ao decodificar a resposta JSON da API.")
            
        return None

    def save_data(self, dados:dict): 
        """
        Salva os dados retornados pela API em um arquivo JSON.
        O nome do arquivo inclui a data e hora da coleta.
        """
        if not dados:
            print("Nenhum dado para salvar.")
            return

        kafka_topic = KafkaAdm('localhost:9092', self.api_get_url)
        kafka_messenger = KafkaMessenger('localhost:9092', self.api_get_url)

        print(f"Criando topico {self.api_get_url}")
        kafka_topic.criar_topico()
        print("Enviando dados para o Kafka")
        kafka_messenger.send_message(dados)
        kafka_messenger.flush()
        kafka_messenger.close()