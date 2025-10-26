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

def get_api_get (): 
    parser = argparse.ArgumentParser(description='Extrai o GET a ser executado')
    parser.add_argument('--get_url', type=str, required=True, help='Variavel que será utilizada no GET da API')
    return parser.parse_args()

def main():
    args = get_api_get()
    connect = ApiConection(API_TOKEN, API_BASE_URL, args.get_url)
    session = connect.auth()
    data = connect.get_data(session)
    connect.save_data(data, 'dados')

if __name__ == "__main__":
    main()