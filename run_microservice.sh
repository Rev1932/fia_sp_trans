#!/bin/bash

# --- Configurações ---
ARQUIVO_PARAMETROS="get_list.txt"
SCRIPT_PYTHON="main.py"

echo "Pressione [CTRL+C] para parar o script."
while true; do
    echo "================================================="
    echo "Iniciando novo ciclo de coleta: $(date)"
    echo "================================================="

    # 3. Lê o arquivo de parâmetros linha por linha
    while IFS='|' read -r termo_linha termo_parada || [[ -n "$termo_linha" ]]; do
        
        # Ignora linhas em branco ou que começam com #
        if [[ -z "$termo_linha" ]] || [[ "$termo_linha" == \#* ]]; then
            continue
        fi
        
        # Monta o array de comando base
        # Usar um array é mais seguro que 'eval' para argumentos com espaços
        CMD_ARRAY=("python" "$SCRIPT_PYTHON" "--linha" "$termo_linha")

        # 4. Verifica se o parâmetro de parada existe
        if [ -n "$termo_parada" ]; then
            # Adiciona o argumento de parada ao comando
            CMD_ARRAY+=("--parada" "$termo_parada")
            echo "--- Coletando Linha: [$termo_linha] | Parada: [$termo_parada] ---"
        else
            echo "--- Coletando Linha: [$termo_linha] (somente posição) ---"
        fi

        # 5. Executa o comando Python
        "${CMD_ARRAY[@]}"
        
        # 6. Pausa entre as requisições para não sobrecarregar a API
        echo "Pausa de 5 segundos..."
        sleep 5

    done < "$ARQUIVO_PARAMETROS"

    echo "================================================="
    echo "Ciclo completo. Aguardando 60 segundos para recomeçar."
    echo "================================================="
    sleep 60

done