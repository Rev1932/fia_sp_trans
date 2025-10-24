from collector import ApiConection
import os
import sys
from dotenv import load_dotenv

#extrair as variaveis de ambiente + libs relevantes

load_dotenv()
    #token de auth e url da API
API_TOKEN = os.getenv('SPTRANS_API_TOKEN')
API_BASE_URL = os.getenv('OLHO_VIVO_URL')
API_GET_URL =  os.getenv('TO_ADD') #TODO vamos loopar constantemente todos os gets a principio???


def main():
    auth = ApiConection(API_TOKEN, API_BASE_URL, API_GET_URL)
    auth.auth()
    auth.get_data()
    auth.save_data()

if __name__ == "__main__":
    main()