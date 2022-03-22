from os.path import exists
import time
import pyodbc
import pandas as pd

connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-I882R5G;DATABASE=inconsistencia;Trusted_Connection=yes;')
cursor = connection.cursor()
arquivoCliente = open(f"./csv/clients-001.csv", "r", encoding="utf-8")


def lerArquivoClientes(id=1, contador=1):
   # arquivoCliente = open(f"./csv/clients-00{id}.csv", "r", encoding="utf-8")

    data = pd.read_csv(f"./csv/clients-00{id}.csv", sep=";", encoding="utf-8")
    df = pd.DataFrame(data)
    print(df)
    while True:
        contador += 1
        for row in df.itertuples():
            print(row[1])
            cursor.execute('''
           insert into clientes(id, nome, email, data_cadastro, telefone)
          values(?,?,?,?,?)
        ''',
                           row[1],
                           row[2],
                           row[3],
                           row[4],
                           row[5]
                           )
        connection.commit()
        if not row:
            break

    arquivoCliente.close()

    if exists(f"./csv/clients-00{(id+1)}.csv"):
        lerArquivoClientes(id+1, contador)


'''
def lerArquivoTransactionIn(id=1, contador=1):
    arquivoTransaction = open(
        f"./csv/transaction-in-00{id}.csv", "r", encoding="utf-8")
    print(f"===== lendo arquivo transaction-in-00{id}.csv ===== ")
    time.sleep(2)

    while True:
        contador += 1
        linha = arquivoTransaction.readline()
        if not linha:
            break
        print("Linha {}: {}".format(contador, linha.strip()))
    arquivoTransaction.close()

    if exists(f"./csv/transaction-in-00{(id+1)}.csv"):
        lerArquivoTransactionIn(id+1, contador)


def lerArquivoTransactionOut(id=1, contador=1):
    # id = str.zfill(3)
    # .rjust(3, '0')
    id_str = str(id)
    # id = f'{id_str.zfill(3)}'
    arquivoTransaction = open(
        f"./csv/transaction-out-{id_str.zfill(3)}.csv", "r", encoding="utf-8")
    print(f"===== lendo arquivo transaction-out-{id_str.zfill(3)}.csv ===== ")
    time.sleep(2)

    while True:
        contador += 1
        linha = arquivoTransaction.readline()
        if not linha:
            break
        print("Linha {}: {}".format(contador, linha.strip()))
    arquivoTransaction.close()

    if exists(f"./csv/transaction-out-{str(id+1).zfill(3)}.csv"):
        lerArquivoTransactionOut(id+1, contador)

'''
lerArquivoClientes()

'''
time.sleep(2)
lerArquivoTransactionIn()
time.sleep(2)
lerArquivoTransactionOut()
'''
