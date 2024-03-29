from glob import glob

import pandas
import xmltodict
from pprint import pp
from collections import deque
import sqlite3

SIMPLES_NACIONAL = 0.04
ML_COMISS = 0.12


class State:
    def __init__(self):
        self.con = sqlite3.connect('db.db')
        self.cur = self.con.cursor()
        # pandas df here
        self.simples_nacional = 0.04
        self.ml_comiss = 0.12


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
            fk_chave_entrada integer,
            fk_chave_saida integer,
            nome text,
            quant int,
            valor_tot real,
            valor_icms_devido real,
            cean int,
            ml_comiss real,
            ml_taxa real,
            emissao text,
            foreign key(fk_chave_entrada) references entrada(chave),
            foreign key(fk_chave_saida) references saida(chave))''')

            self.cur.execute('''create table if not exists saida
            (id integer primary key autoincrement,
            emissao text,
            valor_total real,
            frete real,
            chave int,
            cpf_cnpj int
            )''')

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
                            pp('nada')
                        case 2:
                            pp('nada')
                        case 3:
                            pp('nada')
                        case 4:
                            self.get_pl()

        self.con = sqlite3.connect('db.db')
        self.cur = self.con.cursor()
        create_db()

        self.stock = {}
        self.stock_saida = {}
        self.stock_rules = {}
        self.stock_rules.update({'SOLDA TUBO 25G BEST 183 MSX10 AZ (CX/36)': '36',
                                 'CABO DADOS SATA 50CM PLUSCABLE (PCT/10)': '10'})

        self.receber_entrada()
        self.receber_saida()

        menu()

    def receber_entrada(self):
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

                prods = x['nfeProc']['NFe']['infNFe']['det']

                def parse_prods(one_prod: dict):
                    q_cean = one_prod['prod']['cEAN']
                    q_xProd = one_prod['prod']['xProd']
                    q_qCom = one_prod['prod']['qCom']
                    try:
                        q_frete = float(one_prod['prod']['vFrete'])
                    except KeyError:
                        q_frete = 0.0
                    q_valor_tot = float(one_prod['prod']['vProd']) * -1

                    # TODO see if decimal notation is used
                    q_icms_bc = float(one_prod['imposto']['ICMS']['ICMS00']['vBC'])
                    q_icms_porcent = float(one_prod['imposto']['ICMS']['ICMS00']['pICMS']) / 100
                    q_icms_devido = q_icms_bc * (0.18 - q_icms_porcent)
                    # taxa_fixa, can be only be calc after sell not before
                    q_taxa_fixa = 6 if (q_valor_tot + q_frete + 6) / (
                            1 - SIMPLES_NACIONAL - ML_COMISS) >= 79 else 19
                    # another convoluted math to calculate if the taxa fixa is 6 or 19
                    return q_cean, q_xProd, q_qCom, q_frete, q_valor_tot, q_icms_devido, q_taxa_fixa

                def db_insert_prod(i_chave, i_xProd, i_qCom, i_valor_tot, i_icms_devido, i_cean, i_taxa_fixa):
                    self.cur.execute('''insert into prods
                    (fk_chave_entrada,
                    nome,
                    quant,
                    valor_tot,
                    valor_icms_devido,
                    cean,
                    emissao
                    )values(?,?,?,?,?,?,?)''', (
                        i_chave,  # fk_chave_saida,
                        i_xProd,  # nome,
                        i_qCom,  # quant,
                        i_valor_tot,  # valor_uni,
                        i_icms_devido,  # valor_icms_devivo,
                        i_cean,
                        emissao  # cean,
                    ))
                    self.con.commit()

                if type(prods) is list:
                    for prod in prods:
                        cean, xProd, qCom, frete, valor_tot, icms_devido, taxa_fixa = parse_prods(prod)

                        db_insert_prod(i_chave=chave,
                                       i_xProd=xProd,
                                       i_qCom=qCom,
                                       i_valor_tot=valor_tot,
                                       i_icms_devido=icms_devido,
                                       i_cean=cean,
                                       i_taxa_fixa=taxa_fixa)
                else:
                    cean, xProd, qCom, frete, valor_tot, icms_devido, taxa_fixa = parse_prods(prods)
                    db_insert_prod(i_chave=chave,
                                   i_xProd=xProd,
                                   i_qCom=qCom,
                                   i_valor_tot=valor_tot,
                                   i_icms_devido=icms_devido,
                                   i_cean=cean,
                                   i_taxa_fixa=taxa_fixa)

    def receber_saida(self):
        all_saidas = glob('./saida/*.xml')
        for i in all_saidas:
            with open(i, encoding='utf-8') as f:
                x = xmltodict.parse(f.read())

                # static parms for inserting into saida table
                emissao = x['nfeProc']['NFe']['infNFe']['ide']['dhEmi']
                frete = x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vFrete']
                chave = x['nfeProc']['NFe']['infNFe']['@Id']
                try:
                    cpf_cnpj = x['nfeProc']['NFe']['infNFe']['dest']['CPF']
                except KeyError:
                    cpf_cnpj = x['nfeProc']['NFe']['infNFe']['dest']['CNPJ']
                valor_total = x['nfeProc']['NFe']['infNFe']['total']['ICMSTot']['vProd']

                self.cur.execute('''insert into saida
                (emissao,
                valor_total,
                frete,
                chave,
                cpf_cnpj)values
                (?,?,?,?,?)''', (
                    emissao,
                    valor_total,
                    frete,
                    chave,
                    cpf_cnpj
                ))

                if type(x['nfeProc']['NFe']['infNFe']['det']) is list:
                    for one_prod in x['nfeProc']['NFe']['infNFe']['det']:
                        emissao = x['nfeProc']['NFe']['infNFe']['ide']['dhEmi']
                        cean = one_prod['prod']['cEAN']
                        quant = one_prod['prod']['qCom']
                        valor_tot = one_prod['prod']['vProd']
                        chave = x['nfeProc']['NFe']['infNFe']['@Id']
                        nome = one_prod['prod']['xProd']
                        ml_taxa = 6 if float(valor_tot) <= 79 else 20

                        self.cur.execute('''insert into prods
                        (fk_chave_saida,
                         nome,
                         quant,
                         valor_tot,
                         cean,
                         ml_comiss,
                         ml_taxa,
                         emissao) values(?,?,?,?,?,?,?,?)''', (
                            chave,
                            nome,
                            quant,
                            valor_tot,
                            cean,
                            ML_COMISS,
                            ml_taxa,
                            emissao
                        ))
                        self.con.commit()
                else:
                    # fixo
                    one_prod = x['nfeProc']['NFe']['infNFe']['det']

                    emissao = x['nfeProc']['NFe']['infNFe']['ide']['dhEmi']
                    cean = one_prod['prod']['cEAN']
                    quant = one_prod['prod']['qCom']
                    valor_tot = one_prod['prod']['vProd']
                    chave = x['nfeProc']['NFe']['infNFe']['@Id']
                    nome = one_prod['prod']['xProd']
                    ml_taxa = 6 if float(valor_tot) <= 79 else 20

                    self.cur.execute('''insert into prods
                        (fk_chave_saida,
                         nome,
                         quant,
                         valor_tot,
                         cean,
                         ml_comiss,
                         ml_taxa,
                         emissao) values(?,?,?,?,?,?,?,?)''', (
                        chave,
                        nome,
                        quant,
                        valor_tot,
                        cean,
                        ML_COMISS,
                        ml_taxa,
                        emissao
                    ))
                    self.con.commit()

    def get_pl(self) -> float:

        # icms_devido = 0.0
        # valor_venda = 0.0
        # valor_produto = []
        # emissao_entrada = []
        # self.cur.execute('select valor_total,emissao from entrada')
        # for i in self.cur.fetchall():
        #     valor_produto.append(i[0])
        #     emissao_entrada.append(i[1])
        # self.cur.execute('select valor_icms_devido from entrada')
        # for i in self.cur.fetchall():
        #     icms_devido += float(i[0])
        #
        # # get all simples nacional tax
        # self.cur.execute('select valor_total from saida')
        # for i in self.cur.fetchall():
        #     valor_venda += float(i[0])

        total = 0.0

        dfe = pandas.read_sql_query('select valor_total, emissao, valor_icms_devido, valor_frete from entrada',
                                    self.con)
        dfe['emissao'] = pandas.to_datetime(dfe['emissao'])
        dfe.set_index('emissao', inplace=True)
        pp(dfe.groupby(pandas.Grouper(freq='ME')).sum())

        df = pandas.read_sql_query('select valor_total, emissao from saida', self.con)
        df['emissao'] = pandas.to_datetime(df['emissao'])
        df.set_index('emissao', inplace=True)
        pp(df.groupby(pandas.Grouper(freq='ME')).sum())

        self.cur.execute('''
        select valor_tot ,strftime('%Y-%m-%d', emissao) as emissao_m
        from prods
        group by strftime('%Y-%m',emissao_m)
        ''')
        for i in self.cur.fetchall():
            pp(i)
            total += i[0]
        pp(total)
        # return -icms_devido + valor_venda - (0.04 * valor_venda) - valor_produto
        return 0.0
