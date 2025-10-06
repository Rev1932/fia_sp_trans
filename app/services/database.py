import os
import pymssql
from dotenv import load_dotenv
from .log import Logger

# url_api = "http://127.0.0.1:8000"

sistema = 'Data-Bee'
cliente = 'Paulinia'

load_dotenv()
user = 'datawakeroot' # os.getenv("USERNAME_API")
password = 'tFnCc2*bEH885K@BxV86' # os.getenv('PASSWORD_API')
url_api = 'https://api-logs-2.datawake.cloud:9401'
logger = Logger(url_api, user, password)


class Database:
    """Class to connect and run querys in database"""
    def __init__(self) -> None:
        self._server = os.getenv('DB_SERVER')
        self._port = os.getenv('DB_PORT')
        self._database = os.getenv('DB_NAME')
        self._user = os.getenv('DB_USER')
        self._pwd = os.getenv('DB_PWD')
        self._driver = os.getenv('DB_DRIVER')
        self.conn = self._connect()

    def _connect(self):
        try:
            conn = pymssql.connect(
                server=self._server,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._pwd
            )
            logger.log_info(sistema=sistema, rotina='[database] - Conexao com o bd',
                            mensagem='Conexao com o banco de dados bem sucessida', cliente=cliente)
            return conn
        except pymssql.Error as e:
            logger.log_erro(sistema=sistema, rotina='[database] - Error: conexao com o bd',
                            mensagem=f'Erro ao conectar com o banco de dados: {e}')


if __name__ == '__main__':
    db = Database()
