import json
from glob import glob
import xmltodict
from pprint import pp
from collections import deque


class NotaEntrada:
    def __init__(self):
        self.chave = deque()
        self.nome = deque()
        self.preco = deque()
        self.icms = deque()

    def receber_nota(self):
        todas_notas = glob('*.xml')
        with open(todas_notas[0]) as f:
            x = xmltodict.parse(f.read())
            pp(x['nfeProc']['NFe']['infNFe'])
