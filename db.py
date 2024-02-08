import sqlite3
class Db:
    def __init__(self,cur:sqlite3.Cursor, con:sqlite3.Connection):
        self.cur=cur
    def create_db(self):
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

