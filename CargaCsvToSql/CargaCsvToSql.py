
import xlrd
import pymssql
import pyodbc
import pandas as pd

#print(pyodbc.drivers())  

print('===============  INI Programa =============== ')

def Ouput(array):
    for r in range(len(array)):
        for v in range(len(array[0])):
            print(array[r][v],end='')
            print('  ',end='' )
        print(' ')

print('===============  INI Conexion =============== ')
#try:
#conexion = pymssql.connect(server='TOSHIBA', user='sa',password='Admin123', database='VIVAR',port ='1433')
#conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=TOSHIBA;DATABASE=VIVAR;UID=sa;PWD=Admin123')
conexion = pyodbc.connect('DRIVER={SQL Server};SERVER=TOSHIBA;DATABASE=VIVAR;UID=sa;PWD=Admin123')
cursor = conexion.cursor()
print('===============  VALIDATE Conexion =============== ')

df = pd.read_csv("Resultados_.csv")
df.head()

#sql = "Delete from submit " 
#cursor.execute(sql)
#print('=============== Conexion OK =============== ')
#except:
    #print('=============== Conexion error =============== ')

print('=============== LOAD =============== ')
csv = xlrd.open_workbook("Resultados.csv")
print(csv.head())
csv.head()
hoja = csv.sheet_by_name("Resultados")
for r in range(1,sheet.nrows):
    C = sheet.row_values(r)
    anum = C[0].rstrip()
    bnum = C[1].rstrip()
    sql = "insert into submit values('%s','%s')"%(anum,bnum)
    cursor.execute(sql)
conexion.commit()

sqlQuery = "Select - from subtmit" # ()
cursor.execute(sqlQuery)
results = cursor.fetchall()
Ouput(results)
conexion.close()