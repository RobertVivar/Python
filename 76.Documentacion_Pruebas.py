print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		76.Documentacion_Pruebas")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
# 	Utilizar la documentacion para realizar pruebas
#	=>	Modulo doctest
# -------------------------------------------------------------
def areaTriangulo(base, altura):
	"""
	Calcula el area de un triangulo dado

	>>> areaTriangulo(3,6)
	9.0

	"""
	return (base*altura)/2	
# -------------------------------------------------------------
def areaTriangulo2(base, altura):
	"""
	Calcula el area de un triangulo dado

	>>> areaTriangulo2(3,6)
	'El area del triangulo es : 9.0'

	>>> areaTriangulo2(4,5)
	'El area del triangulo es : 10.0'

	>>> areaTriangulo2(9,3)
	'El area del triangulo es : 13.5'

	>>> areaTriangulo2(9,3)
	'El area del triangulo es : 11.5'
	
	"""
	return "El area del triangulo es : " + str((base*altura)/2)
# -------------------------------------------------------------
import doctest
doctest.testmod()
# -------------------------------------------------------------	