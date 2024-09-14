print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		75.Documentacion")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#	QUE ES ?
#	=========
#	=> Incuir comentarios en clases , metodos, modulos 
#	PARA QUE ?
#	=========
#	=> Para ayudar en el trabajo en equipo .
# 	Especialemnte util en aplicaciones complejas
# -------------------------------------------------------------
def areaCuadrado(lado):
	"""	Calcula El Area De Un Cuadrado 
	Elevando al Cuadrdado el lado , pasado por parametro """
	return "El area del cuadrado Es : " + str(lado*lado)
# -------------------------------------------------------------
def areaTriangulo(base, altura):
	""" Calcula el area de un triangulo utilizando los parametros
		Base y altura"""
	return "El area del Triangulo Es : " + str((base*altura)/2)
# -------------------------------------------------------------
print(areaCuadrado(5))
print(areaCuadrado.__doc__)
help(areaCuadrado)
#print(areaTriangulo(2,7))
help(areaTriangulo)
# -------------------------------------------------------------