print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		37.Archivos.Externos_2 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# Objetivo : Persistencia de datos
# 2 ALTERNATIVAS
#	Manejo de arcihvos externos
# 	Trabajo con BBDD

#	Modo Lectura
#	Modo Escritura
# 	Modo append
from io import open
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#para leer la informacion
archivo_texto=open("archivo.txt","r")
texto=archivo_texto.read()
archivo_texto.close()
print(texto)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
