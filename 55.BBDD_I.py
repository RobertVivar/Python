print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		55.BBDD_I ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
# Creacion de BD
# Conexion de BBD
# Insertar Registros
# SQL SERVER , ORACLE,, MYSQL,, SQLlTE, PostGreSQL
# QUE ES SQLite
#	sistema de gestion de BBDD Relacional
# Escrito en C, es de codigo abeirto
# se guarda como un unico fichero en host
# -------------------------------------------------------------
import sqlite3
# -------------------------------------------------------------
#	01.ABRIR O CREAR LA CONEXION
miConexion=sqlite3.connect("55.PrimeraBase")
# 	02.CREAR PUNTERO O CURSOR
miCursor=miConexion.cursor()
# 	03.EJECUTAR QUERYS
# -------------------------------------------------------------
#	PASO 01 :  CREAR TABLA
# -------------------------------------------------------------
#miCursor.execute("CREATE TABLE PRODUCTOS (NOMBRE_ARTICULO VARCHAR(50),PRECIO INTEGER,SECCION VARCHAR(20))")
# -------------------------------------------------------------
#	PASO 02 :  INSERTAR REGISTRO
# -------------------------------------------------------------
miCursor.execute("INSERT INTO PRODUCTOS VALUES('BALON',15,'DEPORTES')")
miConexion.commit()
# -------------------------------------------------------------
miConexion.close()
# -------------------------------------------------------------