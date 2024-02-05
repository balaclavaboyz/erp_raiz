from nota_entrada import NotaEntrada
from os.path import exists
from os import remove
from pprint import pp

if __name__ == '__main__':
    if exists('./db.db'):
        remove('./db.db')
        pp('removido db')
    else:
        pp('db n exists')
    n = NotaEntrada()
