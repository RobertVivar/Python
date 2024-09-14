print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		37.Archivos.Externos_4 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

from io import open

#para leer la informacion
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
archivo_texto=open("archivo.txt","r")
# Leer archivo y almacenar en la variable
texto=archivo_texto.read()
archivo_texto.close()
print(texto)
print("================")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
archivo_texto=open("archivo.txt","r")
lineas_texto=archivo_texto.readlines()
archivo_texto.close()
print("lineas_texto[0] = " , lineas_texto[0])
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=