#!/bin/bash

# --- Configurações ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PYTHON_EXE="python3"
MAIN_SCRIPT="$SCRIPT_DIR/main.py"

# Arquivos de parâmetros
ARQUIVO_ID_LINHA="$SCRIPT_DIR/id_linha_endpoints.txt"
ARQUIVO_NOME_LINHA="$SCRIPT_DIR/linha_endpoints.txt"

# Intervalo entre CICLOS COMPLETOS (5 minutos)
INTERVALO_CICLO_SEGUNDOS=300

# Intervalo curto entre CADA chamada de API (para evitar ser bloqueado)
ESPERA_ENTRE_CHAMADAS_SEGUNDOS=3


# --- Loop Infinito (Ciclo Externo) ---
echo "Iniciando o serviço de coleta... Pressione [Ctrl+C] para parar."
while true
do
    echo "----------------------------------------------------"
    echo "INICIANDO NOVO CICLO COMPLETO DE COLETA"
    echo "Data/Hora: $(date)"
    echo "----------------------------------------------------"

    # --- TAREFA 1: python3 main.py --get posicao_linha ---
    echo "[$(date +%T)] Iniciando Coleta: posicao_linha (Arquivo: $ARQUIVO_ID_LINHA)"
    
    tr -d '\r' < "$ARQUIVO_ID_LINHA" | while read -r id_linha
    do
        # Ignora linhas em branco ou comentários
        if [[ -n "$id_linha" && ! "$id_linha" =~ ^# ]]; then
            echo "[RUN] $PYTHON_EXE $MAIN_SCRIPT --get posicao_linha --id_linha $id_linha"
            $PYTHON_EXE "$MAIN_SCRIPT" --get posicao_linha --id_linha "$id_linha"
            
            echo "Aguardando $ESPERA_ENTRE_CHAMADAS_SEGUNDOS seg..."
            sleep $ESPERA_ENTRE_CHAMADAS_SEGUNDOS
        fi
    done
    echo "[$(date +%T)] Coleta 'posicao_linha' concluída."


    # --- TAREFA 2: python3 main.py --get previsao_linha ---
    echo "[$(date +%T)] Iniciando Coleta: previsao_linha (Arquivo: $ARQUIVO_ID_LINHA)"

    tr -d '\r' < "$ARQUIVO_ID_LINHA" | while read -r id_linha
    do
        if [[ -n "$id_linha" && ! "$id_linha" =~ ^# ]]; then
            echo "[RUN] $PYTHON_EXE $MAIN_SCRIPT --get previsao_linha --id_linha $id_linha"
            $PYTHON_EXE "$MAIN_SCRIPT" --get previsao_linha --id_linha "$id_linha"
            
            echo "Aguardando $ESPERA_ENTRE_CHAMADAS_SEGUNDOS seg..."
            sleep $ESPERA_ENTRE_CHAMADAS_SEGUNDOS
        fi
    done
    echo "[$(date +%T)] Coleta 'previsao_linha' concluída."


    # --- TAREFA 3: python3 main.py --get linha_buscar ---
    echo "[$(date +%T)] Iniciando Coleta: linha_buscar (Arquivo: $ARQUIVO_NOME_LINHA)"

    tr -d '\r' < "$ARQUIVO_NOME_LINHA" | while read -r nome_linha
    do
        if [[ -n "$nome_linha" && ! "$nome_linha" =~ ^# ]]; then
            # As aspas em "$nome_linha" são cruciais para nomes com espaços
            echo "[RUN] $PYTHON_EXE $MAIN_SCRIPT --get linha_buscar --linha \"$nome_linha\""
            $PYTHON_EXE "$MAIN_SCRIPT" --get linha_buscar --linha "$nome_linha"
            
            echo "Aguardando $ESPERA_ENTRE_CHAMADAS_SEGUNDOS seg..."
            sleep $ESPERA_ENTRE_CHAMADAS_SEGUNDOS
        fi
    done
    echo "[$(date +%T)] Coleta 'linha_buscar' concluída."


    # --- Fim do Ciclo ---
    echo "----------------------------------------------------"
    echo "CICLO COMPLETO CONCLUÍDO."
    echo "Aguardando $INTERVALO_CICLO_SEGUNDOS segundos (5 minutos) para recomeçar."
    echo "----------------------------------------------------"
    sleep $INTERVALO_CICLO_SEGUNDOS

done