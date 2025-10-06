import os
import sys
import pystray
from PIL import Image

def quit_action(icon, item):
    icon.stop()
    os._exit(0)  # Encerra o script

def setup(icon):
    icon.visible = True

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

icon_path = resource_path('bee.ico')

# Carregue o ícone a partir do arquivo .ico
icon = pystray.Icon('Data Bee - Datawake', Image.open(icon_path), menu=pystray.Menu(
    pystray.MenuItem('Quit', quit_action)
))

# Função para rodar a aplicação com a bandeja
def start_icon():
    icon.run(setup)  # Mantém o ícone visível até que o usuário clique em "Quit"