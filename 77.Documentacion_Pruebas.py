print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		77.Documentacion_Pruebas")
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
	"""
	return [math.sqrt(n) for n in listaNumeros]
# -------------------------------------------------------------
print (raizCuadrada([9,16,25,39]))
# -------------------------------------------------------------