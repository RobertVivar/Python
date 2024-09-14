print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		19.Python.Generadores_1")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#	Estructuras que extraen valores de una funcion y e almacenan en objetos iterables
#	Estos valores se alamcenan d euno en uno
#	Cada Vez que un generador almacena un valorm esta permance en un estado pausado hasta 
# 	que se solicita el siguiente.
# 	Esta Caracteristica es conocida como "suspension de estado"
# 	Son mas efecientes que las funciones Tradicionales
# 	muy utiles con listas de valores infinitos
#	Bajo determinados escenarios , ser amuy util que un generador devuelva los valores de uno en uno	
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def generaPares(limite):
	num = 1
	miLista=[]
	while num<limite:
		miLista.append(num*2)
		num=num+1
	return miLista
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
print(generaPares(10))
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def generaPares2(limite):
	num = 1	
	while num<limite:
		yield num*2
		num=num+1
devuelvePares = generaPares2(10)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
for i in devuelvePares:
	print(i)
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def generarPares3(limite):
	num=1
	while num<limite:
		yield num*2
		num=num+1
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
devuelvePares3=generarPares3(10)
print(next(devuelvePares3))
print("Aqui podri air mas codigo")
print(next(devuelvePares3))
print("Aqui podri air mas codigo")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=