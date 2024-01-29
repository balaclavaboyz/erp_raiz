from math import ceil
import json
from glob import glob
import xmltodict
from pprint import pp
from collections import deque


class NotaEntrada:
    def __init__(self):
        def menu():
            while True:
                pp('1. show stock')
                pp('2. show difal')
                pp('3. show stock em reais')
                res = int(input())
                if 4 > res > 0:
                    match res:
                        case 1:
                            self.get_prod_stock()
                        case 2:
                            self.get_icms_owned()
                        case 3:
                            self.get_total_prods()

        self.chave = deque()
        self.nome = deque()
        self.prods = deque()
        self.icms = deque()
        self.receber_nota()

        self.stock = {}
        self.stock_rules = {}
        self.stock_rules.update({'SOLDA TUBO 25G BEST 183 MSX10 AZ (CX/36)': '36',
                                 'CABO DADOS SATA 50CM PLUSCABLE (PCT/10)': '10'})
        menu()
        # pp(self.chave)
        # pp(self.nome)
        # pp(self.prods)
        # pp(self.icms)

    def receber_nota(self):
        todas_notas = glob('./entrada/*.xml')
        for i in todas_notas:
            with open(i) as f:
                x = xmltodict.parse(f.read())
                # pp(x['nfeProc']['NFe']['infNFe'])
                self.chave.append(x['nfeProc']['NFe']['infNFe']['@Id'])
                self.prods.append(x['nfeProc']['NFe']['infNFe']['det'])
                self.nome.append(x['nfeProc']['NFe']['infNFe']['emit']['CNPJ'])
                self.icms.append(x['nfeProc']['NFe']['infNFe']['total']['ICMSTot'])

    def get_total_prods(self):
        for i in self.prods:
            total = 0.0
            for prod in i:
                total += float(prod['prod']['vProd'])
            pp(total)

    def get_prod_stock(self):
        for nota in self.prods:
            for prod in nota:
                if prod['prod']['cProd'] in self.stock_rules:
                    un = int(self.stock_rules.get(prod['prod']['xProd']))
                    prod['prod']['qCom'] = self.stock_rules.get(prod['prod']['xProd'])
                    prod['prod']['vUnCom'] = str(float(prod['prod']['vUnCom']) / un)
                    prod['prod']['vProd'] = str(float(prod['prod']['vProd']) / un)
                name = prod['prod']['xProd']
                qnt = int(prod['prod']['qCom'].split('.')[0])
                if name in self.stock:
                    old = self.stock.get(name)
                    new = int(old) + qnt
                    self.stock.update({name: new})
                else:
                    self.stock.update({name: qnt})
        pp(self.stock)

    def get_icms_owned(self):
        for i in self.icms:
            difal_moeda = 0.0
            vbc = float(i['vBC'])
            vicms = float(i['vICMS'])
            icms_fonercedor = vicms / vbc
            difal = 0.18 - icms_fonercedor
            difal_moeda = difal * vbc
            pp(round(difal_moeda, 3))  # TODO verificar qual round vou usar baseado valor de cobranca da contabilidade
