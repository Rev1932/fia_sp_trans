from cryptography.fernet import Fernet
from dotenv import dotenv_values
from io import StringIO

chave_ = r"Qf5StLVX6wFAwlwdhXTVaF7359YOe-MdmGFZIfqyj6k="


def gerar_chave():
    chave = Fernet.generate_key()
    with open('chave.key', 'wb') as chave_arquivo:
        chave_arquivo.write(chave)


def carregar_chave():
    return open('chave.key', 'rb').read()


def criptografar_env():
    chave = carregar_chave()
    fernet = Fernet(chave)

    with open('.env', 'rb') as arquivo:
        conteudo = arquivo.read()

    conteudo_criptografado = fernet.encrypt(conteudo)

    with open('.env', 'wb') as arquivo_criptografado:
        arquivo_criptografado.write(conteudo_criptografado)


def descriptografar_env():
    fernet = Fernet(chave_)

    with open('.env', 'rb') as arquivo_criptografado:
        conteudo_criptografado = arquivo_criptografado.read()

    conteudo_descriptografado = fernet.decrypt(conteudo_criptografado)

    return conteudo_descriptografado.decode()


def editar_env(campo, valor):
    fernet = Fernet(chave_)

    with open('.env', 'rb') as arquivo_criptografado:
        conteudo_criptografado = arquivo_criptografado.read()

    conteudo_descriptografado = fernet.decrypt(conteudo_criptografado).decode()

    env_vars = dotenv_values(stream=StringIO(conteudo_descriptografado))

    env_vars[campo] = valor

    novo_conteudo = "\n".join(f"{key}={value}" for key, value in env_vars.items())

    conteudo_recriptografado = fernet.encrypt(novo_conteudo.encode())

    with open('.env', 'wb') as arquivo:
        arquivo.write(conteudo_recriptografado)


if __name__ == '__main__':
    # Exemplo de uso
    # gerar_chave()  # Gere a chave uma única vez
    # criptografar_env()  # Criptografe o arquivo inicialmente

    # Editar o arquivo sem descriptografar para o disco
    editar_env("LOG_LEVEL", "ERROR")
    # te = descriptografar_env()
    # editar_env("DB_SERVER", "186.193.228.29")
    # editar_env("DB_PORT", "4022")
    # editar_env("DB_NAME", "dw_bruning")
    # editar_env("DB_USER", "datawake")
    # editar_env("DB_PWD", "8QPH407v")

    # editar_env("DB_SERVER", r"SRVSQLSER01P\SRVSQLSER01P")
    # editar_env("DB_PORT", "1433")
    # editar_env("DB_NAME", "datawake_prd")
    # editar_env("DB_USER", "datawake.integracao")
    # editar_env("DB_PWD", "D@t@W4k3!")
