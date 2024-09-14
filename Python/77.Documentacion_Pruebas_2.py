print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		77.Documentacion_Pruebas_2")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
# 	Utilizar la documentacion para realizar pruebas
#	=>	Modulo doctest
# -------------------------------------------------------------
import math
# -------------------------------------------------------------
def raizCuadrada(listaNumeros):
	"""
	La funcion devuelve una lista con la raiz cuadrada
	de los elementos numericos pasados por parametros 
	en otra lista

	>>> lista=[]
	>>> for i in [4, 9, 16]:
	...		lista.append(i)
	>>> raizCuadrada(lista)
	[2.0, 3.0, 4.0]


	"""
	return [math.sqrt(n) for n in listaNumeros]
# -------------------------------------------------------------
import doctest
doctest.testmod()
# -------------------------------------------------------------
#print (raizCuadrada([9,16,25,39]))
#print (raizCuadrada([9-,16,25,39]))
# -------------------------------------------------------------