# 🚌 Coletor de Dados - API Olho Vivo (SPTrans)

Este projeto é um microserviço de coleta de dados de alta disponibilidade, projetado para consumir a API **Olho Vivo da SPTrans** em tempo real, transformar os dados e enviá-los como um *Producer* para tópicos do **Apache Kafka**.

O serviço foi arquitetado para ser um *daemon* (serviço de longa duração), gerenciado por um *launcher* em shell que garante sua execução contínua. A lógica de transformação de JSONs aninhados da API é gerenciada de forma genérica usando a biblioteca **Pandas**, especificamente a função `json_normalize`.

## 🏛️ Arquitetura

O projeto é composto por 5 componentes principais que trabalham juntos:

1.  **`main.py` (O Serviço)**: O coração do aplicativo. Este script é um serviço de longa duração que roda em um loop infinito. Ele é responsável por:
    * Manter a autenticação com a API.
    * Ler o arquivo `endpoints.txt` a cada ciclo.
    * Chamar a API para cada *alias* configurado.
    * Usar o Pandas para transformar as respostas JSON em DataFrames "achatados".
    * Converter os DataFrames de volta para `list[dict]`.
    * Enviar as mensagens para o Kafka usando a classe `KafkaProducerUtil`.

2.  **`kafka_utils.py` (O Conector Kafka)**: Um módulo utilitário que define a classe `KafkaProducerUtil`. Esta classe gerencia uma **conexão persistente** com o broker Kafka, lidando com o envio de mensagens, *batching* e fechamento limpo da conexão.

3.  **`executar_coleta.sh` (O Launcher Resiliente)**: Um script de *shell* que atua como "guardião" do `main.py`. Ele usa um loop `until` para iniciar o serviço Python e, o mais importante, **reiniciá-lo automaticamente** se o script falhar por qualquer motivo (erro de rede, falha na API, etc.), garantindo que o coletor esteja sempre rodando.

4.  **`endpoints.txt` (O Arquivo de Configuração)**: O "cérebro" da coleta. Em vez de usar `argparse`, este arquivo de texto simples define quais *aliases* de coleta devem ser executados a cada ciclo. O `main.py` lê este arquivo para saber o que fazer.

5.  **`.env` (As Credenciais)**: Um arquivo de ambiente (padrão *dotenv*) para armazenar credenciais sensíveis (token da API, endereço do Kafka) de forma segura, fora do código-fonte.

## 📋 Pré-requisitos

* Python 3.9+
* Uma instância do Apache Kafka acessível.
* Credenciais válidas para a API Olho Vivo (SPTrans).

## 🚀 Instalação e Configuração

1.  Clone este repositório:
    ```bash
    git clone <url-do-seu-repositorio>
    cd <nome-do-repositorio>
    ```

2.  Crie um ambiente virtual (recomendado):
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  Instale as dependências. Crie um arquivo `requirements.txt` com o seguinte conteúdo:
    **`requirements.txt`**
    ```text
    requests
    kafka-python
    pandas
    python-dotenv
    ```
    Em seguida, instale-o:
    ```bash
    pip install -r requirements.txt
    ```

4.  Crie e configure seu arquivo `.env`:
    **`.env`**
    ```ini
    # Token da API Olho Vivo
    SPTRANS_API_TOKEN="SEU_TOKEN_AQUI"

    # Endereço dos seus brokers Kafka
    KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

    # (Opcional) Tempo em segundos entre cada chamada de API (para evitar Rate Limit)
    API_RATE_LIMIT_SECONDS=5

    # (Opcional) Tempo em segundos de espera entre ciclos de coleta completos
    CYCLE_SLEEP_SECONDS=300
    ```

5.  Configure seus *aliases* de coleta no `endpoints.txt`:
    **`endpoints.txt`**
    ```ini
    # Este arquivo define quais coletas serão executadas.
    # O main.py irá ler cada linha e processá-la.
    # Comentários (com #) e linhas em branco são ignorados.

    # Coletas de Posição
    posicao_frota
    posicao_linha_5111
    posicao_linha_31398 # (Letreiro 2766-10)

    # Coletas de Dados Cadastrais (rodar com menos frequência)
    empresas_todas
    corredores_todos

    # Coletas de Previsão
    previsao_parada_4200052
    ```

## 👟 Como Executar

O serviço é iniciado usando o *script* de *shell* `executar_coleta.sh`.

1.  Torne o script executável (apenas na primeira vez):
    ```bash
    chmod +x executar_coleta.sh
    ```

2.  Inicie o serviço:
    ```bash
    ./executar_coleta.sh
    ```
    O script começará a rodar e permanecerá no seu terminal. Se ele falhar, o *script* o reiniciará automaticamente após 10 segundos. Para parar, pressione `Ctrl+C`.

3.  (Opcional) Executando em Background (Modo de Produção)
    Para rodar o serviço permanentemente em um servidor, use `nohup` e `&`:
    ```bash
    nohup ./executar_coleta.sh > coletor.log 2>&1 &
    ```
    * `nohup`: Garante que o script continue rodando mesmo se você fechar o terminal.
    * `> coletor.log 2>&1`: Redireciona toda a saída (logs e erros) para o arquivo `coletor.log`.
    * `&`: Coloca o processo em segundo plano.

---

## 🔧 Configuração Avançada (Adicionando Novos Endpoints)

A inteligência do coletor está na capacidade de adicionar novos *endpoints* sem reescrever a lógica de transformação. Isso é feito editando duas funções no `main.py`:

1.  **`rotear_chamada(alias)`**: Mapeia um *alias* do `endpoints.txt` para uma URL, um tópico Kafka e uma configuração de transformação (o `pd_config`).
2.  **`transformar_para_dataframe(json_data, pd_config)`**: Usa a configuração do `pd_config` para achatar o JSON usando `pd.json_normalize`.

### O `pd_config`

O `pd_config` é um dicionário que passa argumentos diretamente para `pd.json_normalize`:

* `record_path`: O caminho (uma lista) até a **lista de dados** que você quer transformar em **linhas** no DataFrame.
* `meta`: Uma lista de chaves do JSON "pai" que você quer **copiar** para cada nova linha (ex: copiar o `codigo_linha` para cada veículo).
* `rename_map`: (Opcional) Um dicionário para renomear as colunas da API (ex: `py` -> `latitude`).

### Exemplo: Adicionando o Endpoint `/Posicao/Garagem`

**1. JSON de Exemplo (API):**
```json
{
  "hr": "10:30",
  "e": [
    { "c": 1, "a": 11, "v": [ {"p": "1 1001", "py": -23.1}, ... ] },
    { "c": 2, "a": 8,  "v": [ {"p": "2 1501", "py": -23.5}, ... ] }
  ]
}