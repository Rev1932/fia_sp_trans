from fastavro.schema import parse_schema
from fastavro import schemaless_writer
from dotenv import load_dotenv
from struct import pack
from io import BytesIO
import datetime
import pandas as pd
import json
import requests
import uuid
from .log import Logger
import os
from requests.auth import HTTPBasicAuth


url_api = 'https://api-logs-2.datawake.cloud:9401'
load_dotenv()
user = 'datawakeroot' # os.getenv("USERNAME_API")
password = 'tFnCc2*bEH885K@BxV86' # os.getenv('PASSWORD_API')
logger = Logger(url_api, user, password)

sistema = 'Data-Bee'
cliente = 'Paulinia'

# TODO EXECUTAVEL FOI GERADO SEM COLOCAR OS METODOS COMO STATICOS


class Avro:
    def __init__(self, url, user=None, password=None):
        self.schema_registry_url = url
        self.auth = HTTPBasicAuth(user, password) if user and password else None

    @staticmethod
    def generate_avro_schema(df, topic):
        """
        Gera um schema Avro válido a partir dos tipos de dados SQL Server em um DataFrame.

        :param df: DataFrame com colunas ['Data_type', 'Column_Name'].
        :param topic: Nome do tópico Kafka.
        :return: Esquema Avro parseado.
        """
        sql_to_avro_type = {
            "bigint": "long",
            "binary": "bytes",
            "bit": "boolean",
            "char": "string",
            "date": "string",
            "datetime": "string",
            "decimal": "double",
            "float": "double",
            "int": "int",
            "nchar": "string",
            "numeric": "double",
            "nvarchar": "string",
            "smallint": "int",
            "sysname": "string",
            "time": "string",
            "tinyint": "int",
            "uniqueidentifier": "string",
            "varbinary": "bytes",
            "varchar": "string"
        }

        fields = []
        for _, row in df.iterrows():
            sql_type = row['Data_type'].lower()
            column = row['Column_Name']
            avro_type = sql_to_avro_type.get(sql_type, "string")

            fields.append({
                "name": column,
                "type": ["null", avro_type],
                "default": None
            })

        schema = {
            "type": "record",
            "name": f"{topic}_record",
            "fields": fields
        }

        return parse_schema(schema)

    @staticmethod
    def serialize_with_schema_registry(schema_id, schema, row, topic):
        """
        Serialize a row for Kafka with Schema Registry header.

        :param schema_id: ID do Schema a ser registrado.
        :param schema: Schema a ser registrado.
        :param row: Linha do banco a ser serializada.
        :param topic: Topico do Kafka a qual esse schema esta se referindo.
        :return: buffer.getvalue()
        """
        buffer = BytesIO()

        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, pd.Timestamp):
                row[key] = value.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif isinstance(value, datetime.date):
                row[key] = value.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif isinstance(value, datetime.time):
                row[key] = value.strftime('%H:%M:%S')
            elif isinstance(value, uuid.UUID):
                row[key] = str(value)
            elif isinstance(value, bool):
                row[key] = value
            elif isinstance(value, bytes):
                pass

        for field in schema.get("fields", []):
            name = field.get("name")
            types = field.get("type")
            value = row.get(name)

            if isinstance(types, list):
                if "bytes" in types and isinstance(value, str):
                    row[name] = value.encode("utf-8")
                elif "long" in types and isinstance(value, float) and value.is_integer():
                    row[name] = int(value)
                elif "int" in types and isinstance(value, float) and value.is_integer():
                    row[name] = int(value)

        buffer.write(pack(">bI", 0, schema_id))
        schemaless_writer(buffer, schema, row)

        return buffer.getvalue()

    def register_schema(self, schema, topic):
        """

        Register the Avro schema to the Schema Registry.

        If conflict (409) occurs, delete and re-register the schema.

        :param schema: Schema a ser registrado.
        :param topic: Topico do Kafka a qual esse schema esta se referindo.
        :return: schema_id or None
        """
        schema_id = self.check_existing_schema(topic, schema)
        if schema_id:
            logger.log_info(sistema=sistema, rotina='[avro_registry] - register schema',
                            mensagem=f'Schema já existente, id {schema_id}', cliente=cliente, topico=topic,
                            tabela=topic.replace('paulinia', ""))
            return schema_id

        url = f"{self.schema_registry_url}/subjects/{topic}-value/versions"
        payload = {"schema": json.dumps(schema)}
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        try:
            response = requests.post(url, json=payload, headers=headers, auth=self.auth, verify=False)
            if response.status_code == 409:  # Se der conflito, exclui o schema e tenta de novo
                logger.log_info(sistema=sistema, rotina='[avro_registry] - register schema',
                                mensagem=f'Conflito detectado (409), excluindo schema...', cliente=cliente,
                                topico=topic, tabela=topic.replace('paulinia_', ""))

                self.delete_schema(topic)

                response = requests.post(url, json=payload, headers=headers, auth=self.auth, verify=False)
                response.raise_for_status()

            response.raise_for_status()
            schema_id = response.json()["id"]

            return schema_id

        except requests.exceptions.RequestException as e:
            logger.log_erro(sistema=sistema, rotina='[avro_registry] - Error: register schema',
                            mensagem=f'Erro ao registrar schema: {e}')
            return None

    def delete_schema(self, topic):
        """
        Delete the schema from Schema Registry.

        :param topic: Topico relacionado ao schema que quer apagar.
        :return:
        """
        url = f"{self.schema_registry_url}/subjects/{topic}-value"

        try:
            response = requests.delete(url, auth=self.auth, verify=False)
            if response.status_code == 200:
                logger.log_info(sistema=sistema, rotina='[avro_registry] - delete schema',
                                mensagem=f'Schema {topic}-value excluído com sucesso.', cliente=cliente, topico=topic,
                                tabela=topic.replace('paulinia_', ""))
                print('apagado')
            else:
                print('erro', response.status_code, response.text)
                logger.log_erro(sistema=sistema, rotina='[avro_registry] - Error: delete schema',
                                mensagem=f'Erro ao excluir schema: {response.status_code} - {response.text}')
        except requests.exceptions.RequestException as e:
            print(e)
            logger.log_erro(sistema=sistema, rotina='[avro_registry] - Error: delete schema',
                            mensagem=f'Erro ao excluir schema: {e}')

    def check_existing_schema(self, topic, schema):
        """
        Check if schema already exists in the Schema Registry.

        :param topic: Topico do Kafka a qual esse schema esta se referindo.
        :param schema: Schema para verifcar se existe.
        :return: existing_schema_id, None
        """
        url = f"{self.schema_registry_url}/subjects/{topic}-value/versions/latest"
        try:
            response = requests.get(url, auth=self.auth, verify=False)
            if response.status_code == 404:
                # Schema not found
                return None

            response.raise_for_status()
            existing_schema = response.json()

            existing_schema_str = json.dumps(json.loads(existing_schema['schema']), sort_keys=True)
            new_schema_str = json.dumps(schema, sort_keys=True)

            if existing_schema_str == new_schema_str:
                return existing_schema['id']
        except requests.exceptions.RequestException as e:
            logger.log_erro(sistema=sistema, rotina='[avro_registry] - Error: check existing schema',
                            mensagem=f'Erro ao gerar schema: {e}')
        return None


if __name__ == '__main__':
    test = Avro('https://registry-1.datawake.cloud:49086')
    ts = ['limeira_dw_material_movimentacao_tipo']

    for topico in ts:
        test.delete_schema(topico)
