import os
import requests
import json
import logging
from datetime import datetime
from kafka_messenger_utils import KafkaMessenger
from kafka_topic_utils import KafkaAdm
from dotenv import load_dotenv
import pandas as pd

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
        
    def get_data(self, session: requests.Session, params=None) -> list:
        if not session:
            print("Sessão de autenticação inválida")
            return None
        posicao_url = f'{self.api_base_url}/{self.api_get_url}'
        print(posicao_url)
        if not params:
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
        else:
            try:
                response = session.get(posicao_url, params=params)
                response.raise_for_status()
                dados = response.json()
                return dados
            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar posição dos veículos: {e}")
            except json.JSONDecodeError:
                print("Erro ao decodificar a resposta JSON da API.")
                
            return None

    def transform_data(self, dados, chave_conteudo=None):
        """
        Função genérica para converter o JSON da API em um DataFrame Pandas,
        usando pd.json_normalize para achatamento.
        """
        if not dados:
            print("Get sem dados retornados")
            return None
        else:
            hora_api = None
            hora_proc = datetime.now().isoformat()
            df = pd.DataFrame() # DataFrame vazio por padrão
            if not chave_conteudo:
                df = pd.json_normalize(dados)
                print("Mostrando Data Frame \n")
                print(df)
                df['data_coleta'] = hora_proc
                transformed_data = df.to_dict('records')
                return transformed_data
            else:
                df = pd.json_normalize(dados[f'{chave_conteudo}'])
                print("Mostrando Data Frame \n")
                print(df)
                df['data_coleta'] = hora_proc
                transformed_data = df.to_dict('records')
                return transformed_data


    def save_data(self, dados:dict, topic_name): 
        """
        Salva os dados retornados pela API em um arquivo JSON.
        O nome do arquivo inclui a data e hora da coleta.
        """
        if not dados:
            print("Nenhum dado para salvar.")
            return

        kafka_topic = KafkaAdm('localhost:9092', topic_name)
        kafka_messenger = KafkaMessenger('localhost:9092', topic_name)

        print(f"Criando topico {topic_name}")
        kafka_topic.criar_topico()
        print("Enviando dados para o Kafka")
        kafka_messenger.send_message(dados)
        kafka_messenger.flush()
        kafka_messenger.close()