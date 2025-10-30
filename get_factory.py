from collector import ApiConection

class GetDistribution():
    def __init__(self):
        self.gets = {
            "posicao": GetPosicao,
            "posicao_linha" : GetPosicaoLinha,
            "posicao_garagem": GetPosicaoGaragem,
            "linha_buscar": GetLinhaBuscar,
            "linha_buscar_sentido": GetLinhaBuscarSentido,
            "paradas_buscar": GetParadasBuscar,
            "paradas_buscar_linha": GetParadasBuscarLinha,
            "paradas_buscar_corredor": GetParadasBuscarCorredor,
            "corredor": GetCorredor,
            "empresas": GetEmpresas,
            "previsao": GetPrevisao,
            "previsao_linha": GetPrevisoLinha,
            "previsao_parada": GetPrevisaoParada,
        }

    def new_instance(self, api_token, base_url, args):
        if args.get in self.gets:
            classe = self.gets[args.get]
            return classe(api_token, base_url, args)
        

class GetPosicao() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = args.get

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data, 'dados')

class GetPosicaoLinha() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/Linha?codigoLinha={args.linha}"

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data, 'dados')

class GetPosicaoGaragem() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/Garagem?codigoEmpresa={args.empresa}"

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data, 'dados')

class GetLinhaBuscar() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/Buscar?termosBusca={args.linha}" 

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data, 'dados')