import sqlite3

class State:
    def __init__(self):
        self.con = sqlite3.connect('db.db')
        self.cur = self.con.cursor()
        # pandas df here
        self.simples_nacional = 0.04
        self.ml_comiss = 0.12
