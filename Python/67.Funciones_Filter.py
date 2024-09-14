print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		66.Funciones_Filter ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#	Funciones Filter y utilidad
#	Comprobar o verificar que los elementos de una secuencia cumplen
#	una condicion, devolviendo un iterador con los elementos
#	que cumplen dicha condicion
# -------------------------------------------------------------
def numero_par(num):
	if num%2==0:
		return True
# -------------------------------------------------------------
numeros=[2,88,4,29,9,23,22,1]

#print(filter(numero_par,numeros))
print("list(filter()) : ", list(filter(numero_par,numeros)))
print("lambda : " , list(filter(lambda numero_par: numero_par%2==0, numeros)))
# -------------------------------------------------------------

# -------------------------------------------------------------

# -------------------------------------------------------------