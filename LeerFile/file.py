##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#    1
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
archivo_texto=open("d:/Rep/Python/LeerFile/config.ini","r", encoding="utf-8")
texto=archivo_texto.read()
archivo_texto.close()
print(texto)

#ith open("d:/Rep/Python/LeerFile/config.ini", "r", encoding="utf-8") as archivo:
#   for linea in archivo:
#       print(linea)
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#   2
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#from io import open
#file = open("d:/Rep/Python/LeerFile/config.ini", "r")
#print(file.read())
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#   3
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#archivo = open("d:/Rep/Python/LeerFile/config.ini",'r')
#print(archivo.readable())
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#   4
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#with open("d:/Rep/Python/LeerFile/config.ini") as archivo:
 #   print(archivo.readlines())
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#   5
##--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#with open("d:/Rep/Python/LeerFile/config.ini") as archivo:
#    for linea in archivo:
#        print(linea)
