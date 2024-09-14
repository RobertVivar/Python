print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		58.BBDD_IV ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
# 	Clausula (UNIQUE)
#	Operadores CRUD 
#				C = Crear
#				R : Leer Informacion)
#				U : Update
#				D = Delete
# -------------------------------------------------------------
import sqlite3
# -------------------------------------------------------------
miConexion=sqlite3.connect("58.CRUD")

miCursor=miConexion.cursor()

miCursor.execute('''
	CREATE TABLE PRODUCTOS(
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		NOMBRE_ARTICULO VARCHAR(50) UNIQUE,
		PRECIO INTEGER,
		SECCION VARCHAR(20)	)
	''')

productos=[
	("Pelota",20,"Jugueteria"),
	("Pantalon",15,"confeccion"),
	("destornillador",25,"Ferreteria"),
	("Jarron",45,"Ceramica"),
	("Pantalones",35,"confeccion")
]
# -------------------------------------------------------------
miCursor.executemany("INSERT INTO PRODUCTOS VALUES (NULL,?,?,?)",productos)
# -------------------------------------------------------------
miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------