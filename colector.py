import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

#TODO: salvar os dados da API diretamente dentro de um dataframe, ao inves de dicionario

load_dotenv()
#token de auth e url da API
API_TOKEN = os.getenv('SPTRANS_API_TOKEN')
API_URL = os.getenv('OLHO_VIVO_URL')

#onde os dados serão salvos para vins exploratorios
output_path = 'dados'

def auth() -> requests.Session:
    """
    Autentica na API da SPTrans 
    """
    auth_url = f'{API_URL}/Login/Autenticar?token={API_TOKEN}'
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
    
def get_data(session: requests.Session) -> list:
    if not session:
        print("Sessão de autenticação inválida")
        return None
    
    posicao_url = f'{API_URL}/Posicao'

    try:
        response = session.get(posicao_url)
        response.raise_for_status()
        dados = response.json()
        print(f"Encontrados {len(dados.get('l', []))} linhas em operação.")
        return dados
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar posição dos veículos: {e}")
    except json.JSONDecodeError:
        print("Erro ao decodificar a resposta JSON da API.")
        
    return None

def save_data(dados:dict, path: str):
    """
    Salva os dados retornados pela API em um arquivo JSON.
    O nome do arquivo inclui a data e hora da coleta.
    """
    if not dados:
        print("Nenhum dado para salvar.")
        return

    # Garante que a pasta de destino exista
    if not os.path.exists(path):
        os.makedirs(path)
        
    # Gera um nome de arquivo único com timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    nome_arquivo = f'posicao_veiculos_{timestamp}.json'
    caminho_arquivo = os.path.join(path, nome_arquivo)
    
    try:
        with open(caminho_arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)
        print(f"Dados salvos com sucesso em: {caminho_arquivo}")
    except IOError as e:
        print(f"Erro ao salvar o arquivo: {e}")

# --- Execução Principal ---
if __name__ == "__main__":
    print("Iniciando o coletor de dados da SPTrans...")
    
    # 1. Autenticar na API
    sessao_autenticada = auth()
    
    # 2. Se a autenticação funcionou, busca e salva os dados
    if sessao_autenticada:
        posicao_atual = get_data(sessao_autenticada)
        save_data(posicao_atual, output_path)
        
    print("Dados coletados.")