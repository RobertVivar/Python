print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		55.BBDD_I ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# Creacion de BD
# Conexion de BBD
# Insertar Registros
# SQL SERVER , ORACLE,, MYSQL,, SQLlTE, PostGreSQL
# QUE ES SQLite
#	sistema de gestion de BBDD Relacional
# Escrito en C, es de codigo abeirto
# se guarda como un unico fichero en host

import sqlite3

#	01.ABRIR O CREAR LA CONEXION
miConexion=sqlite3.connect("PrimeraBase")
# 	02.CREAR PUNTERO O CURSOR
miCursor=miConexion.cursor()
# 	03.EJECUTAR QUERYS
#miCursor.execute("CREATE TABLE PRODUCTOS (NOMBRE_ARTICULO VARCHAR(50),PRECIO INTEGER,SECCION VARCHAR(20))")
miCursor.execute("INSERT INTO PRODUCTOS VALUES()")

miConexion.close()


