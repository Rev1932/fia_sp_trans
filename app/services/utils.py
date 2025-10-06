from .cache import Cache
from .log import Logger
import os
from dotenv import load_dotenv


url_api = 'https://api-logs-2.datawake.cloud:9401'
# url_api = "http://127.0.0.1:8000"
user = 'datawakeroot' # os.getenv("USERNAME_API")
password = 'tFnCc2*bEH885K@BxV86' # os.getenv('PASSWORD_API')
logger = Logger(url_api, user, password)

sistema = 'Data-Bee'
cliente = 'Paulinia'
cache = Cache()


def get_last_id(key: str, topic: str) -> int:
    """get last id in cache file"""
    last_id = 0 if cache.get(key) is None else cache.get(key)
    logger.log_info(sistema=sistema, rotina='[utils] - get id',
                    mensagem=f'Ultimo id {last_id}, extraido do arquivo cache', cliente=cliente, topico=topic,
                    tabela=topic.replace('paulinia_', ""))
    return last_id


def get_last_date(key: str, topic: str) -> str:
    last_date = '2000-01-01' if cache.get(key) is None else cache.get(key)
    logger.log_info(sistema=sistema, rotina='[utils] - get data atualizacao',
                    mensagem=f'Ultima dt atualizacao {last_date}, extraido do arquivo cache', cliente=cliente,
                    topico=topic, tabela=topic.replace('paulinia_', ""))
    return last_date
