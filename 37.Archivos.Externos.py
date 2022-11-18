print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		37.Archivos.Externos ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# Objetivo : Persistencia de datos
# 2 ALTERNATIVAS
#	Manejo de arcihvos externos
# 	Trabajo con BBDD

#	Modo Lectura
#	Modo Escritura
# 	Modo append
from io import open

archivo_texto=open("archivo.txt","w")#modo escritura

frase ="Estupendo dia para estudiar Python "

archivo_texto.write(frase)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
