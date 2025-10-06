import requests
from typing import Optional
import concurrent.futures
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv


class Logger:
    def __init__(self, url: str, user: str, password: str):
        self.url = url
        self.auth = HTTPBasicAuth(user, password)
        self.headers = {"Content-Type": "application/json"}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def _verificar_log_existente(self, dados: dict):
        """
        Verifica se já existe um log com os mesmos valores de sistema, rotina, cliente, tabela e topico.
        """
        params = {
            "sistema": dados["sistema"],
            "rotina": dados["rotina"],
            "cliente": dados["cliente"],
            "tabela": dados["tabela"],
            "topico": dados["topico"]
        }


        try:
            response = requests.get(
                f"{self.url}/buscar",
                params=params,
                timeout=5,
                verify=False,
                auth=self.auth
            )
            response.raise_for_status()
            resultado = response.json()

            if isinstance(resultado, list) and resultado:
                return resultado[0]["id"]

        except requests.exceptions.RequestException as e:
            print(f"Erro ao verificar log existente: {e}")

        return None

    def _enviar_log(self, dados: dict):
        """
        Envia ou atualiza um log.
        """
        log_id = self._verificar_log_existente(dados)

        try:
            if log_id:
                requests.put(
                    f"{self.url}/{log_id}",
                    json=dados,
                    headers=self.headers,
                    timeout=5,
                    verify=False,
                    auth=self.auth
                )
                print(f"Log atualizado (ID: {log_id}).")
            else:
                requests.post(
                    f"{self.url}/logs",
                    json=dados,
                    headers=self.headers,
                    timeout=5,
                    verify=False,
                    auth=self.auth
                )
                print("Novo log inserido.")

        except requests.exceptions.RequestException as e:
            print(f"Falha ao enviar log: {e}")

    def enviar_log_async(self, dados: dict):
        """Executa `_enviar_log` em uma thread separada."""
        self.executor.submit(self._enviar_log, dados)

    # --------------------- Métodos de log ---------------------
    def log_info(self, sistema: Optional[str] = None, rotina: Optional[str] = None, username: Optional[str] = None,
                 mensagem: Optional[str] = None, cliente: Optional[str] = None, tabela: Optional[str] = None,
                 topico: Optional[str] = None, op: Optional[str] = None, unidade_producao: Optional[str] = None):
        dados = {
            "tipo": "Info",
            "sistema": sistema,
            "rotina": rotina,
            "username": username,
            "mensagem": mensagem,
            "cliente": cliente,
            "tabela": tabela,
            "topico": topico,
            "op": op,
            "unidade_producao": unidade_producao
        }
        self.enviar_log_async(dados)

    def log_erro(self, sistema: Optional[str] = None, rotina: Optional[str] = None, username: Optional[str] = None,
                 mensagem: Optional[str] = None, cliente: Optional[str] = None, tabela: Optional[str] = None,
                 topico: Optional[str] = None, op: Optional[str] = None, unidade_producao: Optional[str] = None):
        dados = {
            "tipo": "Erro",
            "sistema": sistema,
            "rotina": rotina,
            "username": username,
            "mensagem": mensagem,
            "cliente": cliente,
            "tabela": tabela,
            "topico": topico,
            "op": op,
            "unidade_producao": unidade_producao
        }
        self.enviar_log_async(dados)

    def log_aviso(self, sistema: Optional[str] = None, rotina: Optional[str] = None, username: Optional[str] = None,
                  mensagem: Optional[str] = None, cliente: Optional[str] = None, tabela: Optional[str] = None,
                  topico: Optional[str] = None, op: Optional[str] = None, unidade_producao: Optional[str] = None):
        dados = {
            "tipo": "Aviso",
            "sistema": sistema,
            "rotina": rotina,
            "username": username,
            "mensagem": mensagem,
            "cliente": cliente,
            "tabela": tabela,
            "topico": topico,
            "op": op,
            "unidade_producao": unidade_producao
        }
        self.enviar_log_async(dados)

    def log_debug(self, sistema: Optional[str] = None, rotina: Optional[str] = None, username: Optional[str] = None,
                  mensagem: Optional[str] = None, cliente: Optional[str] = None, tabela: Optional[str] = None,
                  topico: Optional[str] = None, op: Optional[str] = None, unidade_producao: Optional[str] = None):
        dados = {
            "tipo": "Debug",
            "sistema": sistema,
            "rotina": rotina,
            "username": username,
            "mensagem": mensagem,
            "cliente": cliente,
            "tabela": tabela,
            "topico": topico,
            "op": op,
            "unidade_producao": unidade_producao
        }
        self.enviar_log_async(dados)


if __name__ == '__main__':
    load_dotenv()
    # url_api = 'https://api-logs-2.datawake.cloud:9401'
    url_api = 'http://localhost:9200'

    user = os.getenv("USERNAME_API")
    password = os.getenv('PASSWORD_API')

    logger = Logger(url_api, user, password)

    logger.log_info("Data-bee", rotina="envio pro topico", mensagem="Enviado com sucesso ", cliente="Bruning",
                    topico="TESTE_paranoa_dw_oee")

