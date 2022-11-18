print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		38.Archivos.Externos_2 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

from io import open
# Lectura y Escritura = r+
archivo_texto=open("archivo.txt","r+")

archivo_texto.write("Comienzo Del Texto")

#print(archivo_texto.readlines())

lista_texto=archivo_texto.readlines()

lista_texto[5]=" Esta Linea ha sido incluida desde el exterior \n "
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#archivo_texto.writelines(lista_texto)

#archivo_texto.close()

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=