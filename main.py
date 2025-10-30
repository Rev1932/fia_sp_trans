from collector import ApiConection
import os
import sys
import argparse
from dotenv import load_dotenv

#extrair as variaveis de ambiente + libs relevantes

load_dotenv()
    #token de auth e url da API
API_TOKEN = os.getenv('SPTRANS_API_TOKEN')
API_BASE_URL = os.getenv('OLHO_VIVO_URL')

def get_parse (): 
    parser = argparse.ArgumentParser(description='Extrai o GET a ser executado')
    parser.add_argument('--get', type=str, required=True, help='Metodo que será utilizada no GET da API')
    return parser.parse_args()

def main():
    args = get_parse()
    connect = ApiConection(API_TOKEN, API_BASE_URL, args)
    session = connect.auth()
    data = connect.get_data(session)
    connect.save_data(data, 'dados')

if __name__ == "__main__":
    main()