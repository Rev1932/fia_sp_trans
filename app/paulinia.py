from services.utils import get_last_date
from services.kafka_confluent import KafkaAdmin
from services.kafka_service import KafkaService
from services.avro_registry import Avro
from services.database import Database
from cryptography.fernet import Fernet
from services.cache import Cache
from services.log import Logger
from services.CRIP import chave_
from services.api_rep import DatawakeFetcher
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
import os


url_api = 'https://api-logs-2.datawake.cloud:9401'

sistema = 'Data-Bee'
cliente = 'Paulinia'


def descriptografar_env(chave):
    fernet = Fernet(chave)

    with open('.env', 'rb') as arquivo_criptografado:
        conteudo_criptografado = arquivo_criptografado.read()

    conteudo_descriptografado = fernet.decrypt(conteudo_criptografado)

    with StringIO(conteudo_descriptografado.decode()) as temp_env:
        load_dotenv(stream=temp_env)


descriptografar_env(chave_)
load_dotenv()
user = 'datawakeroot' # os.getenv("USERNAME_API")
password = 'tFnCc2*bEH885K@BxV86' # os.getenv('PASSWORD_API')
logger = Logger(url_api, user, password)


class Paulinia:
    """ Class of client """
    def __init__(self) -> None:
        self.db = Database()
        self.kafka_ali = KafkaService("broker-1.datawake.cloud:49093",
                                      'wildcard_datadriven_cloud.key',
                                      'wildcard_datadriven_cloud.crt', 'SASL_SSL')
        # self.kafka_ali = KafkaService('broker-1.datawake.cloud:49093', 'broker-1.key', 'broker-1.crt', 'SASL_SSL')
        self.cache = Cache()
        self.avro_ali = Avro("https://registry-1.datawake.cloud:49086", user, password)
        self.kafka_confluent_ali = KafkaAdmin('broker-1.datawake.cloud:49093',
                                              'wildcard_datadriven_cloud.key',
                                              'wildcard_datadriven_cloud.crt', 'SASL_SSL')
        # self.kafka_confluent_ali = KafkaAdmin('broker-1.datawake.cloud:49093', 'broker-1.key', 'broker-1.crt', 'SASL_SSL')

    @staticmethod
    def reprocessa():
        fetcher = DatawakeFetcher()
        fetcher.processar_integracoes()

    def get_data_query(self, query):
        """

        Get data in database by query, return pandas dataframe

        :param query: Consulta que sera realizada na instancia do pandas;
        :return: datafrma
        """
        conn = self.db.conn

        try:
            df = pd.read_sql(query, conn)
            return df
        except pd.errors.DatabaseError as e:
            logger.log_erro(sistema=sistema, rotina='[paulinia] - Error execucao query',
                            mensagem=f'Erro ao executar a query {e}', cliente=cliente)

    def get_tables_from_db(self):
        """
        Retrieve all table names from the database

        :return: Nome de todas as tabelas do banco|lista vazia
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name NOT IN ('dw_cadeia_ajuda_regras', 'dw_andon_peso');
        """
        # TODO WHERE table_name NOT IN ('tabela_excluida1', 'tabela_excluida2'); com isso podemos limitar as tabelas
        #   q estamos puxando do banco
        conn = self.db.conn
        try:
            tables = pd.read_sql(query, conn)['table_name'].tolist()
            return tables
        except pd.errors.DatabaseError as e:
            logger.log_erro(sistema=sistema, rotina='[paulinia] - Error execucao query que lista todas as tabelas',
                            mensagem=f'Erro ao executar a query {e}', cliente=cliente)
            return []

    def get_table_columns(self, table_name):
        """

        Retrieve column names and data types from a table

        :param table_name: Nomes das tabelas listadas no metodo get_tables_from_db
        :return: Colunas listadas de cada tabela| df vazio do pandas
        """
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """
        conn = self.db.conn
        try:
            columns = pd.read_sql(query, conn)
            return columns
        except pd.errors.DatabaseError as e:
            logger.log_erro(sistema=sistema, rotina='[paulinia] - Error execucao query que lista as colunas de '
                                                    'cada tabela', mensagem=f'Erro ao executar a query {e}',
                            cliente=cliente)
            return pd.DataFrame()

    def detect_table_config(self, table_name):
        """
        Detecta dinamicamente as colunas de controle da tabela.
        """
        columns = self.get_table_columns(table_name)
        if columns.empty:
            return None

        key_id = 'id' if 'id' in columns['column_name'].values else None
        key_dt_update = 'data_atualizacao' if 'data_atualizacao' in columns['column_name'].values else None
        key_dt_criacao = 'data_criacao' if 'data_criacao' in columns['column_name'].values else None

        if key_dt_update and key_dt_criacao and key_id:
            order_by = f"isnull({key_dt_update}, {key_dt_criacao}), {key_id}"
        elif key_dt_criacao and key_id:
            order_by = f"{key_dt_criacao}, {key_id}"
        elif key_dt_update and key_id:
            order_by = f"{key_dt_update}, {key_id}"
        elif key_dt_update or key_dt_criacao:
            order_by = key_dt_update or key_dt_criacao
        elif key_id:
            order_by = key_id
        else:
            order_by = "1"

        return {
            "key_id": key_id,
            "key_dt_update": key_dt_update,
            "key_dt_criacao": key_dt_criacao,
            "order_by": order_by
        }

    def get_data(self, table: str, key_id: str, key_dt_update: str, key_dt_criacao: str, last_dt_update: str):
        """
        Extrai dados da tabela com base em colunas de controle, incluindo registros com datas nulas.
        """
        where_condition = "1=1"
        config = self.detect_table_config(table)
        order_by = config.get("order_by") if config else "1"

        if key_dt_update and key_dt_criacao:
            dt_expr = f"isnull({key_dt_update}, {key_dt_criacao})"
        elif key_dt_update:
            dt_expr = key_dt_update
        elif key_dt_criacao:
            dt_expr = key_dt_criacao
        else:
            dt_expr = None

        if last_dt_update and dt_expr:
            if key_id:
                where_condition = (
                    f"((convert(varchar(25), {dt_expr}, 126) + '-' + convert(varchar(50), {key_id})) > "
                    f"'{last_dt_update}')")
            else:
                where_condition = f"(convert(varchar(25), {dt_expr}, 126) > '{last_dt_update}')"

        if dt_expr and key_id:
            new_id_expr = f"convert(varchar(25), {dt_expr}, 126) + '-' + convert(varchar(50), {key_id})"
        elif dt_expr:
            new_id_expr = f"convert(varchar(25), {dt_expr}, 126)"
        elif key_id:
            new_id_expr = f"convert(varchar(50), {key_id})"
        else:
            new_id_expr = "'no_key'"

        query = f"""
            SELECT TOP {os.getenv('TOP', 2000)} 
            'PAULINIA' as unidade_origem,
            DB_NAME() as dataset_origem,
            {new_id_expr} as new_id,
            *
            FROM {table} WITH (NOLOCK)
            WHERE {where_condition}
            ORDER BY {order_by};
        """
        return self.get_data_query(query)

    def get_type(self, table: str):
        query = f"""
             SELECT DISTINCT
                 t.Name AS Data_type,
                 c.name AS Column_Name
             FROM 
                 sys.columns c
             INNER JOIN  
                 sys.types t ON c.user_type_id = t.user_type_id
             WHERE 
                 c.object_id = OBJECT_ID('{table}') 
         """
        df = self.get_data_query(query)

        extra_cols = pd.DataFrame([
            {'Data_type': 'nvarchar', 'Column_Name': 'unidade_origem'},
            {'Data_type': 'nvarchar', 'Column_Name': 'dataset_origem'},
            {'Data_type': 'nvarchar', 'Column_Name': 'new_id'},
        ])

        return pd.concat([extra_cols, df], ignore_index=True)

    def send_data(self, table: str):
        """
        Generic method to send data to Kafka and Schema Registry

        :param table: Nome da tabela que estamos extraindo as informações
        :return: None
        """
        config = self.detect_table_config(table)
        if not config:
            logger.log_aviso(sistema=sistema, rotina='[paulinia] - Envio de dados',
                             mensagem='Dataframe vazio', cliente=cliente)
            return None

        topic = f'paulinia_{table}'
        key_id = topic + '_id'
        key_dt_update = topic + '_dt_atualizacao'

        last_dt_update = get_last_date(key_dt_update, topic) if config["key_dt_update"] else None

        df_data = self.get_data(
            table=table,
            key_id=config["key_id"],
            key_dt_update=config["key_dt_update"],
            key_dt_criacao=config.get("key_dt_criacao"),
            last_dt_update=last_dt_update,
        )

        if df_data is None or df_data.empty:
            logger.log_aviso(sistema=sistema, rotina='[paulinia] - Envio',
                             mensagem='Dataframe vazio', cliente=cliente,
                             tabela=topic.replace('paulinia_', ""), topico=topic)
            return None

        # try:
        #     if not self.kafka_confluent.criar_topico(topic):
        #         logger.log_aviso(sistema=sistema, rotina='[paulinia] - Cria tópico',
        #                          mensagem='Falha ao criar o tópico %s. Verifique o Kafka',
        #                          cliente=cliente, tabela=topic.replace('paulinia_', ""), topico=topic)
        # except Exception as e:
        #     logger.log_erro(sistema=sistema, rotina='[paulinia] - Erro tópico',
        #                     mensagem=f'Erro ao verificar ou criar o tópico: {e}',
        #                     cliente=cliente, tabela=topic.replace('paulinia_', ""), topico=topic)
        #     return None

        # TODO KAFKA CONFLUENTE TIRAR
        try:
            if not self.kafka_confluent_ali.criar_topico(topic):
                logger.log_aviso(sistema=sistema, rotina='[paulinia] - Cria topico',
                                 mensagem='Falha ao criar o tópico %s. Verifique o Kafka',
                                 cliente=cliente, tabela=topic.replace('paulinia_', ""), topico=topic)
        except Exception as e:
            logger.log_erro(sistema=sistema, rotina='[paulinia] - Error topico',
                            mensagem=f'Erro ao verificar ou criar o tópic {e}', cliente=cliente,
                            tabela=topic.replace('paulinia_', ""), topico=topic)
            return None

        data_type = self.get_type(table)
        # avro_schema = self.avro.generate_avro_schema(data_type, topic)
        # todo ADICIONAR O STATUS CODE
        # schema_id = self.avro.register_schema(avro_schema, topic)
        # if schema_id is None:
        #     logger.log_erro(sistema=sistema, rotina='[paulinia] - Erro schema id',
        #                     mensagem='Falha ao registrar schema', cliente=cliente,
        #                    tabela=topic.replace('paulinia_', ""), topico=topic)
        #     return None

        avro_schema_ali = self.avro_ali.generate_avro_schema(data_type, topic)

        schema_id_ali = self.avro_ali.register_schema(avro_schema_ali, topic)
        if schema_id_ali is None:
            logger.log_erro(sistema=sistema, rotina='[paulinia] - Error schema id',
                            mensagem=f'Failed to register schema', cliente=cliente,
                            tabela=topic.replace('paulinia_', ""), topico=topic)
            print('Failed to register schema')
            return None

        for _, row in df_data.iterrows():
            row_dict = row.to_dict()
            # serialized_message = self.avro.serialize_with_schema_registry(schema_id, avro_schema, row_dict, topic)

            serialized_message_ali = self.avro_ali.serialize_with_schema_registry(schema_id_ali, avro_schema_ali,
                                                                                  row_dict, topic)
            # if not self.kafka.send_message(topic, serialized_message):
            #     return None

            # TODO TIRAR AQUI
            if not self.kafka_ali.send_message(topic, serialized_message_ali):
                print('erro ao enviar')
                return None

            if 'new_id' in row:
                self.cache.set(key_dt_update, row['new_id'])
            if config["key_id"]:
                self.cache.set(key_id, row[config["key_id"]])

    def run(self):
        """Run send_data for all tables"""
        try:
            self.reprocessa()
        except Exception as e:
            logger.log_erro(sistema=sistema, rotina='[paulinia] - Error ao reprocessar',
                            mensagem=f'Error reprocessa: {e}', cliente=cliente)
        tables = self.get_tables_from_db()
        for table in tables:
            self.send_data(table)


if __name__ == '__main__':
    client = Paulinia()
    client.run()
