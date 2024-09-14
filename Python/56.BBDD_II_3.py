print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		56.BBDD_II_3 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# 	Insercion de Varios Registros (Lote De Registros)
#	Recuperacion de Varios Registros
# -------------------------------------------------------------
import sqlite3
# -------------------------------------------------------------
#	01.ABRIR O CREAR LA CONEXION
miConexion=sqlite3.connect("55.PrimeraBase")
# 	02.CREAR PUNTERO O CURSOR
miCursor=miConexion.cursor()
# 	03.EJECUTAR QUERYS
# -------------------------------------------------------------
miCursor.execute("SELECT * FROM PRODUCTOS")
#	fetchall() 
#	Devuelve una lista con todos los registros que no devuelve la instruccion SQL
VariosProductos=miCursor.fetchall()

for producto in VariosProductos:
	print(producto)
#print(VariosProductos)
# -------------------------------------------------------------
miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------