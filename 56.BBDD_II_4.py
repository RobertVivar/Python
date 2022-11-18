print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		56.BBDD_II_4 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# 	Insercion de Varios Registros (Lote De Registros)
#	Recuperacion de Varios Registros
# -------------------------------------------------------------
import sqlite3
# -------------------------------------------------------------
#	01.ABRIR O CREAR LA CONEXION
miConexion=sqlite3.connect("56.GestionProductos")
# 	02.CREAR PUNTERO O CURSOR
miCursor=miConexion.cursor()
# 	03.EJECUTAR QUERYS
# -------------------------------------------------------------
miCursor.execute("SELECT * FROM PRODUCTOS")
#	fetchall() 
#	Devuelve una lista con todos los registros que no devuelve la instruccion SQL
VariosProductos=miCursor.fetchall()

for producto in VariosProductos:
	print("  [Nombre Articulo] : ", producto[0], " [Seccion] : ", producto[2])
#print(VariosProductos)
# -------------------------------------------------------------
miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------