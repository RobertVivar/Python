print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		56.BBDD_II ")
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
#miCursor.execute("INSERT INTO PRODUCTOS VALUES('BALON',15,'DEPORTES')")
# Listas y Tuplas
variosProductos=[
	("Camiseta", 10 , "Deportes"),
	("Jarron", 20 , "Ceramica"),
	("Camion", 30 , "Jugueteria"),

]
miCursor.executemany("INSERT INTO PRODUCTOS VALUES(?,?,?)",variosProductos)

miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------