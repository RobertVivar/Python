print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		58.BBDD_IV_3_Update ")
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
# -------------------------------------------------------------
# UPDATE
#miCursor.execute("SELECT * FROM PRODUCTOS WHERE SECCION='confeccion'")
miCursor.execute("UPDATE PRODUCTOS SET PRECIO=35 WHERE NOMBRE_ARTICULO='Pelota'")

productos=miCursor.fetchall()

print(productos)
# -------------------------------------------------------------
# -------------------------------------------------------------
#miCursor.executemany("INSERT INTO PRODUCTOS VALUES (NULL,?,?,?)",productos)
# -------------------------------------------------------------
miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------