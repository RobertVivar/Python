print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		38.Archivos.Externos ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
from io import open

arhcivo_texto=open("archivo.txt","r")

#print(arhcivo_texto.read())

# metodo seek()
# se posiciona en la posicion que le indiquemos 

#arhcivo_texto.seek(11)
arhcivo_texto.seek(11)

print(arhcivo_texto.read())