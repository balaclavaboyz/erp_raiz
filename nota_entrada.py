from glob import glob
import xmltodict
from pprint import pp
from collections import deque
import sqlite3


class NotaEntrada:
    def __init__(self):
        def create_db():
            # nota de entrada
            self.cur.execute('''create table if not exists entrada 
            (id integer primary key autoincrement,
             cnpj text,
             chave int,
             emissao text,
             valor_total real,
             valor_bc real,
             valor_icms real,
             valor_icms_devido real,
             valor_frete real
             )''')
            # dados dos produtos
            self.cur.execute('''create table if not exists prods
            (id integer primary key,
            fk_nota_entrada integer,
            nome text,
            quant int,
            valor_uni real,
            valor_bc_icms real,
            valor_icms real,
            valor_icms_devido real,
            valor_aliquota_icms real,
            ecan int,
            foreign key(fk_nota_entrada) references entrada(id))''')

            self.cur.execute('''create table if not exists saida
            (id integer primary key autoincrement,
            emissao text,
            cean int,
            quant int,
            valor_total real,
            frete real,
            chave int,
            cpf_cnpj int,
            cep int,
            rua text,
            municipio text,
            uf text,
            pais text,
            nome text,
            complemento text)''')

            self.cur.execute('''create table if not exists anuncio
            (id integer primary key,
            cean int,
            quant int,
            taxa_fixa real,
            taxa_comiss real,
            is_full int)''')
            self.con.commit()
            # self.cur.execute('''select * from sqlite_master where type=\'table\'''')
            # for i in self.cur.fetchall():
            #     pp(i)

        def menu():
            while True:
                pp('1. show stock')
                pp('2. show difal')
                pp('3. show stock em reais')
                pp('4. show PL')
                res = int(input())
                if 5 > res > 0:
                    match res:
                        case 1:
                            self.get_prod_stock()
                        case 2:
                            pp(self.get_icms_owned())
                        case 3:
                            pp(self.get_total_prods())
                        case 4:
                            self.get_pl()

        self.chave = deque()
        self.nome = deque()
        self.prods = deque()
        self.icms = deque()

        self.con = sqlite3.connect('db.db')
        self.cur = self.con.cursor()
        create_db()

        self.stock = {}
        self.stock_saida = {}
        self.stock_rules = {}
        self.stock_rules.update({'SOLDA TUBO 25G BEST 183 MSX10 AZ (CX/36)': '36',
                                 'CABO DADOS SATA 50CM PLUSCABLE (PCT/10)': '10'})

        self.receber_nota()
        self.receber_saida()

        menu()
        # pp(self.chave)
        # pp(self.nome)
        # pp(self.prods)
        # pp(self.icms)

    def receber_nota(self):
        todas_notas = glob('./entrada/*.xml')
        for i in todas_notas:
            with open(i, encoding='utf-8') as f:
                x = xmltodict.parse(f.read())
                # pp(x['nfeProc']['NFe']['infNFe'])
                # self.chave.append(x['nfeProc']['NFe']['infNFe']['@Id'])
                # self.prods.append(x['nfeProc']['NFe']['infNFe']['det'])
                # self.nome.append(x['nfeProc']['NFe']['infNFe']['emit']['CNPJ'])
                # self.icms.append(x['nfeProc']['NFe']['infNFe']['total']['ICMSTot'])
                cnpj = x['nfeProc']['NFe']['infNFe']['emit']['CNPJ']
                chave = x['nfeProc']['NFe']['infNFe']['@Id']
                emissao = x['nfeProc']['NFe']['infNFe']['ide']['dhEmi']
                valor_total = float(x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vNF'])
                valor_bc = float(x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vBC'])
                valor_icms = float(x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vICMS'])
                valor_icms_devido = float(valor_bc * (0.18 - (valor_icms / valor_bc)))
                valor_frete = float(x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vFrete'])
                self.cur.execute('''insert into entrada
                (cnpj,chave,emissao,valor_total,valor_bc,valor_icms,valor_icms_devido,valor_frete) values
                (?,?,?,?,?,?,?,?)''', (
                    cnpj, chave, emissao, valor_total, valor_bc, valor_icms, valor_icms_devido, valor_frete))
                self.con.commit()

    def receber_saida(self):
        all_saidas = glob('./saida/*.xml')
        for i in all_saidas:
            with open(i, encoding='utf-8') as f:
                x = xmltodict.parse(f.read())
                if type(x['nfeProc']['NFe']['infNFe']['det']) is list:
                    for one_prod in x['nfeProc']['NFe']['infNFe']['det']:
                        emissao = x['nfeProc']['NFe']['infNFe']['ide']['dhEmi']
                        cean = one_prod['prod']['cEAN']
                        quant = one_prod['prod']['qCom']
                        valor_total = one_prod['prod']['vProd']
                        frete = x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vFrete']
                        chave = x['nfeProc']['NFe']['infNFe']['@Id']

                        try:
                            cpf_cnpj = x['nfeProc']['NFe']['infNFe']['dest']['CPF']
                        except KeyError as e:
                            cpf_cnpj = x['nfeProc']['NFe']['infNFe']['dest']['CNPJ']

                        cep = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['CEP']
                        rua = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xLgr']
                        municipio = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xMun']
                        uf = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['UF']
                        pais = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xPais']
                        nome = x['nfeProc']['NFe']['infNFe']['dest']['xNome']
                        complemento = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xCpl']
                        self.cur.execute('''insert into saida
                        (emissao,cean,quant,valor_total,frete,chave,cpf_cnpj,cep,rua,municipio, uf,pais,nome,complemento) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                                         (
                                         emissao, cean, quant, valor_total, frete, chave, cpf_cnpj, cep, rua, municipio,
                                         uf,
                                         pais, nome, complemento))
                        self.con.commit()
                else:
                    one_prod = x['nfeProc']['NFe']['infNFe']['det']

                    cean = one_prod['prod']['cEAN']
                    quant = one_prod['prod']['qCom']
                    valor_total = one_prod['prod']['vProd']
                    frete = x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vFrete']
                    chave = x['nfeProc']['NFe']['infNFe']['@Id']
                    try:
                        cpf_cnpj = x['nfeProc']['NFe']['infNFe']['dest']['CPF']
                    except KeyError as e:
                        cpf_cnpj = x['nfeProc']['NFe']['infNFe']['dest']['CNPJ']
                    cep = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['CEP']
                    rua = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xLgr']
                    municipio = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xMun']
                    uf = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['UF']
                    pais = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xPais']
                    nome = x['nfeProc']['NFe']['infNFe']['dest']['xNome']
                    complemento = x['nfeProc']['NFe']['infNFe']['dest']['enderDest']['xCpl']
                    self.cur.execute('''insert into saida
                    (cean,quant,valor_total,frete,chave,cpf_cnpj,cep,rua,municipio, uf,pais,nome,complemento) values(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                                     (cean, quant, valor_total, frete, chave, cpf_cnpj, cep, rua, municipio, uf, pais,
                                      nome, complemento))
                    self.con.commit()

    def get_pl(self) -> float:
        icms_devido = 0.0
        valor_venda = 0.0
        valor_produto = 0.0
        self.cur.execute('select valor_total from entrada')
        for i in self.cur.fetchall():
            valor_produto += float(i[0])

        self.cur.execute('select valor_icms_devido from entrada')
        for i in self.cur.fetchall():
            icms_devido += float(i[0])

        # get all simples nacional tax
        self.cur.execute('select valor_total from saida')
        for i in self.cur.fetchall():
            valor_venda += float(i[0])

        pp(icms_devido)
        pp(valor_venda)
        pp(valor_produto)
        pp('total')
        return -icms_devido + valor_venda - (0.04 * valor_venda) - valor_produto

    def get_total_prods(self) -> list:
        total = []
        for i in self.prods:
            tmp = 0
            for prod in i:
                tmp += float(prod['prod']['vProd'])
            total.append(tmp)
        return total

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
        res = []
        for i in self.icms:
            difal_moeda = 0.0
            vbc = float(i['vBC'])
            vicms = float(i['vICMS'])
            icms_fonercedor = vicms / vbc
            difal = 0.18 - icms_fonercedor
            difal_moeda = difal * vbc
            # pp(round(difal_moeda, 3))  # TODO verificar qual round vou usar baseado valor de cobranca da contabilidade
            res.append(difal_moeda)
        return res
