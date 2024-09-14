print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		39.Archivos.Serializacion ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Serializacion de colecciones , objetos
# guardar en un fichero externo un dicicionario , objeto , en codigo binario
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Biclioteca
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Pickle 
#  Metodo dump() : Volcado de datos al datos al fichero binario externo
#  Metodo load() : carga de datos del fichero fichero binario externo
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

import pickle # importando libreria

lista_nombres=["Robert","Emilio","Vivar","Mori"]

# Escritura binario = wb
fichero_binario=open("listas_nombres","wb")

#volcado externo 
pickle.dump(lista_nombres,fichero_binario)

#Cerrando el fichero
fichero_binario.close()

#Borrarlo de la memoria
del(fichero_binario)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=