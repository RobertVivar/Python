print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		57.BBDD_III_3 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
import sqlite3
# -------------------------------------------------------------
miConexion=sqlite3.connect("57.GestionProductos2")

miCursor=miConexion.cursor()

miCursor.execute('''
	CREATE TABLE PRODUCTOS(
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		NOMBRE_ARTICULO VARCHAR(50),
		PRECIO INTEGER,
		SECCION VARCHAR(20)	)
	''')

productos=[
	("Pelota",20,"Jugueteria"),
	("Pantalon",15,"confeccion"),
	("destornillador",25,"Ferreteria"),
	("Jarron",45,"Ceramica")
]
# -------------------------------------------------------------
miCursor.executemany("INSERT INTO PRODUCTOS VALUES (NULL,?,?,?)",productos)
# -------------------------------------------------------------
miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------