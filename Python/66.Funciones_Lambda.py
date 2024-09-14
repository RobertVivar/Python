print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		66.Funciones_Lambda ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#	Funciones_Lambda : Lista JavaScript
#	Es Una funcion anonima , para programar , abreviar
#	para que la sintaxis sea mas ligera y ahorranos tiempo
#	Consiste en resumir un funcion normal en python
# -------------------------------------------------------------
def CalcularAreaTriangulo(base, altura):
	return (base * altura)/2
# -------------------------------------------------------------
print(CalcularAreaTriangulo(15,2))
triangulo1=CalcularAreaTriangulo(5,7)
triangulo2=CalcularAreaTriangulo(9,6)
print(triangulo1)
print(triangulo2)
# -------------------------------------------------------------
#	APLICANDO LA FUNCION [lambda]
# -------------------------------------------------------------
#	
CalcularAreaTriangulo2=lambda base, altura:(base*altura)/2
#
# -------------------------------------------------------------
print("")
print("CalcularAreaTriangulo2: " ,CalcularAreaTriangulo2(15,3))
print("CalcularAreaTriangulo2: " ,CalcularAreaTriangulo2(20,3))
print("")
# -------------------------------------------------------------
#al_Cubo:lambda numero:pow(numero,3)

al_Cubo=lambda numero:numero**3

print("al_Cubo: " ,al_Cubo(13))
# -------------------------------------------------------------
destacar_valor=lambda comision:"¡{}! $".format(comision)
comision_Robert=15585

print("comision : ", destacar_valor(comision_Robert))
# -------------------------------------------------------------