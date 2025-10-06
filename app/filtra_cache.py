import pickle

topicos_new = [
    "new_paranoa_dw_api_gateway",
    "new_paranoa_dw_api_gateway_up_url",
    "new_paranoa_dw_erp_queue_controle",
    "new_paranoa_dw_log",
    "new_paranoa_dw_material_roteiro",
    "new_paranoa_dw_oee",
    "new_paranoa_dw_oee_historico",
    "new_paranoa_dw_erp_queue",
    "new_paranoa_dw_etiqueta",
    "new_paranoa_dw_lote",
    "new_paranoa_dw_lote_unidade_producao_consumo",
    "new_paranoa_dw_microtempo",
    "new_paranoa_dw_op_integracao",
    "new_paranoa_dw_wms_solicitacao"
]

# Carrega o arquivo .pkl
with open('cache.pkl', 'rb') as f:
    data = pickle.load(f)

# Remove as chaves que você quer excluir
for topico in topicos_new:
    if f'{topico}_id' in data:
        del data[f'{topico}_id']
    if f'{topico}_dt_atualizacao' in data:
        del data[f'{topico}_dt_atualizacao']

# Salva o arquivo novamente
with open('cache.pkl', 'wb') as f:
    pickle.dump(data, f)
