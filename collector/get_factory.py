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
            "empresa": GetEmpresas,
            "previsao": GetPrevisao,
            "previsao_linha": GetPrevisaoLinha,
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
        self.topic_name = args.get
        self.chave_conteudo = 'l'
        self.get = args.get

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        print(data)
        transformed_data = connect.transform_data(data, self.chave_conteudo)
        connect.save_data(transformed_data, self.topic_name)

class GetPosicaoLinha() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.topic_name = args.get
        self.chave_conteudo = 'vs'
        self.params = {
            'codigoLinha': args.id_linha
        }
        self.get = "posicao/linha"

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session, self.params)
        transformed_data = connect.transform_data(data, self.chave_conteudo)
        connect.save_data(transformed_data, self.topic_name)

class GetPosicaoGaragem() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = "garagem"

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data)

class GetLinhaBuscar() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.topic_name = args.get
        self.params = {
            'termosBusca': args.linha
        }
        self.get = "linha/buscar"

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session, self.params)
        transformed_data = connect.transform_data(data)
        connect.save_data(transformed_data, self.topic_name)

class GetLinhaBuscarSentido() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/buscarLinhaSentido?termosBusca={args.linha}&sentido={args.sentido}" 

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data)

class GetParadasBuscar() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.topic_name = args.get
        self.params = {
            'termosBusca': 'Afonso'
        }
        self.get = "parada/buscar"

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session, self.params)
        transformed_data = connect.transform_data(data)
        connect.save_data(transformed_data, self.topic_name)

class GetParadasBuscarLinha() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/buscarparadasoorlinha?codigoLinha={args.linha}" 

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data)

class GetParadasBuscarCorredor() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/BuscarParadasPorCorredor?codigoCorredor={args.corredor}" 

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data)

class GetCorredor() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.topic_name = args.get
        self.get = args.get

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        print(data)
        transformed_data = connect.transform_data(data)
        connect.save_data(transformed_data, self.topic_name)

class GetEmpresas() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}" 

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data)

class GetPrevisao() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/?codigoParada={args.parada}&codigoLinha={args.linha} " 

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data)

class GetPrevisaoLinha() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.topic_name = args.get
        self.chave_conteudo = 'ps'
        self.params = {
            'codigoLinha': args.id_linha
        }
        self.get = "previsao/linha"

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session, self.params)
        transformed_data = connect.transform_data(data, self.chave_conteudo)
        connect.save_data(transformed_data, self.topic_name)

class GetPrevisaoParada() :
    def __init__(self, api_token, base_url, args):
        self.api_token = api_token
        self.base_url = base_url
        self.get = f"{args.get}/Parada?codigoParada={args.parada}" 

    def run(self):
        connect = ApiConection(self.api_token, self.base_url, self.get)
        session = connect.auth()
        data = connect.get_data(session)
        connect.save_data(data)