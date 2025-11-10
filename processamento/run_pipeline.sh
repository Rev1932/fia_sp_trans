#!/bin/bash

# ===============================================================
# Loop infinito para rodar pipeline SPTrans a cada 5 minutos
# ===============================================================

# ativa o venv (ajuste o caminho se necessário)
source .venv/bin/activate

while true
do
    echo "🚀 Iniciando pipeline SPTrans (Bronze → Silver → Gold)..."
    echo "🕒 Execução iniciada em: $(date)"
    echo "==================="

    echo "1️⃣  Rodando Bronze"
    python bronze.py

    echo "==================="
    echo "2️⃣  Rodando Silver"
    python silver.py

    echo "==================="
    echo "3️⃣  Rodando Gold"
    python gold.py

    echo "✅ Pipeline concluída às: $(date)"
    echo "-----------------------------------------"

    # tempo de espera: 300 segundos = 5 minutos
    echo "⏳ Aguardando 5 minutos para a próxima execução..."
    sleep 300
done
