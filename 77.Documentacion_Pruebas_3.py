print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		77.Documentacion_Pruebas_3")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
# 	Utilizar la documentacion para realizar pruebas
#	=>	Modulo doctest
# -------------------------------------------------------------
import math
# -------------------------------------------------------------
#	Los 3 puntos (...) :  Nos permite leer el error que sale
#	al ejecutar o compilar el programa python , ya sea numero negativo
#	La 1era y la ultima deben ser iguales
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

	>>> lista=[]
	>>> for i in [4,-9,16,50,78,-90,125]:
	...	   lista.append(i)
	>>> raizCuadrada(lista)
	Traceback (most recent call last):
		...
	ValueError: math domain error


	"""
	return [math.sqrt(n) for n in listaNumeros]
# -------------------------------------------------------------
import doctest
doctest.testmod()
# -------------------------------------------------------------
#print (raizCuadrada([9,16,25,39]))
#print (raizCuadrada([9,-16,25,39]))
# -------------------------------------------------------------