print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		58.BBDD_IV_2_Read ")
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
# Read
miCursor.execute("SELECT * FROM PRODUCTOS WHERE SECCION='confeccion'")

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