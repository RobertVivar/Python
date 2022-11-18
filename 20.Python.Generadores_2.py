print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		20.Python.Generadores_2")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#	Nueva Instruccion	=	yield from
#	Utilidad : Simplicar el codigo de los generaodres en el caso de utilizar 
# 	bucles anidados
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def devuelve_ciudades(*ciudades):
# 	Cuando se coloca un asterisco significa q va recibir un num indeterminado de elementos
#	en forma de tuplca
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

	for elemento in ciudades:
		yield elemento
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

ciudades_devueltas = devuelve_ciudades("Lima,","Madrid,","Bilbao","Paris")

print(next(ciudades_devueltas))

print(next(ciudades_devueltas))

print(next(ciudades_devueltas))
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
def devuelve_ciudades2(*ciudades):
	for elemento in ciudades:
		for subElmento in elemento:
			yield subElmento
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
ciudades_devuelve2=devuelve_ciudades2("Lima,","Madrid,","Bilbao","Paris")

print(next(ciudades_devuelve2))

print(next(ciudades_devuelve2))
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=