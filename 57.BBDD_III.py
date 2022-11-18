print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		57.BBDD_III ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
import sqlite3
# -------------------------------------------------------------
miConexion=sqlite3.connect("57.GestionProductos")

miCursor=miConexion.cursor()

miCursor.execute('''
	CREATE TABLE PRODUCTOS(
		CODIGO_ARTICULO VARCHAR(4) PRIMARY KEY,
		NOMBRE_ARTICULO VARCHAR(50),
		PRECIO INTEGER,
		SECCION VARCHAR(20)	)
	''')

productos=[
	("AR01","Pelota",20,"Jugueteria"),
	("AR02","Pantalon",15,"confeccion"),
	("AR03","destornillador",25,"Ferreteria"),
	("AR04","Jarron",45,"Ceramica")
]
# -------------------------------------------------------------
miCursor.executemany("INSERT INTO PRODUCTOS VALUES (?,?,?,?)",productos)
# -------------------------------------------------------------
miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------
