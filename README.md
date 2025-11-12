Pipeline de Dados — API Olho Vivo (SPTrans)
Visão Geral

Este projeto tem como objetivo a construção de um pipeline de dados em near real-time (atualização a cada 5 minutos), consumindo informações da API Olho Vivo (SPTrans), processando-as e disponibilizando-as para análise no Power BI.

A arquitetura foi desenvolvida com foco em modularidade, escalabilidade e governança de dados, utilizando tecnologias de streaming, processamento distribuído e armazenamento em camadas.

Arquitetura do Pipeline
1. Coleta de Dados

A coleta é feita através de um microserviço Python (DataBee) que realiza requisições GET na API Olho Vivo.

Os dados brutos obtidos são enviados para o Apache Kafka, que atua como mensageiro entre as camadas do pipeline.

Esse processo é executado em intervalos de 5 minutos, garantindo atualização contínua e leve.

2. Ingestão e Streaming (Apache Kafka)

O Kafka recebe os dados do microserviço e os distribui em tópicos (um para cada tipo de informação, como veículos, linhas, posições, etc.).

Cada tópico serve como fonte de dados para o Apache Spark, que realiza o consumo em tempo quase real.

3. Processamento e Enriquecimento (Apache Spark)

O Spark é responsável por ler, limpar e transformar os dados vindos do Kafka.

Ele escreve os resultados no MinIO, um serviço compatível com S3, utilizando o formato Parquet para eficiência e compressão.

O pipeline segue o modelo de camadas Medallion:

Bronze: dados brutos recebidos diretamente do Kafka.

Silver: dados limpos e deduplicados, com chaves únicas e estrutura normalizada.

Gold: dados prontos para consumo analítico e integração com ferramentas BI.

4. Armazenamento (MinIO)

O MinIO funciona como o Data Lake do projeto, armazenando os dados das camadas Bronze, Silver e Gold.

Cada camada é salva em formato Parquet, otimizando leitura e compressão.

5. Consulta e Integração Analítica

O DuckDB atua como query engine, permitindo consultas SQL diretamente sobre os arquivos Parquet armazenados no MinIO.

O Power BI se conecta ao DuckDB, possibilitando dashboards dinâmicos e análises em tempo quase real.

Tecnologias Utilizadas
Tecnologia	Função
API Olho Vivo (SPTrans)	Fonte de dados em tempo real sobre transporte público
DataBee (Microserviço Python)	Coleta e envio dos dados para o Kafka
Apache Kafka	Ingestão e mensageria em tempo real
Apache Spark	Processamento distribuído e transformação dos dados
MinIO (S3)	Data Lake para armazenamento em camadas
DuckDB	Motor de consultas SQL sobre arquivos Parquet
Power BI	Visualização e análise dos dados processados
Fluxo Resumido

API Olho Vivo → DataBee → Kafka
Coleta e envio dos dados em tópicos.

Kafka → Spark → MinIO
Processamento, tratamento e persistência nas camadas Bronze, Silver e Gold.

MinIO → DuckDB → Power BI
Consulta SQL e visualização analítica em dashboards.

Frequência

Near Real-Time — atualização a cada 5 minutos

Resultado Esperado

Um pipeline automatizado que:

Garante ingestão contínua dos dados de transporte.

Mantém o histórico e consistência por meio das camadas Medallion.

Fornece dados limpos e prontos para análise em Power BI.

Facilita reprocessamentos e governança através da separação por camadas e tópicos.
