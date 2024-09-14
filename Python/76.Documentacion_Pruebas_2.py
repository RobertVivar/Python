print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		76.Documentacion_Pruebas_2")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
def compruebaMail(mailUsuario):
	"""
	La funcion comprueba mail , evalua un mail recibido
	en busca del arroba @ ,	
	Si tiene un arroba @ , es correcto
	Si tiene mas de un arroba @ , es incorrecto

	>>> compruebaMail('robert@correo.com')
	True

	>>> compruebaMail('robertcorreo.com@')
	False

	>>> compruebaMail('robertcorreo.com')
	False

	>>> compruebaMail('robert@correo.@.com')
	False

	"""
	arroba=mailUsuario.count('@')
	if(arroba!=1 or 
		mailUsuario.rfind('@')==(len(mailUsuario)-1) or 		
		mailUsuario.find('@')==0):
		return False
	else:
		return True

# -------------------------------------------------------------
import doctest
doctest.testmod()
# -------------------------------------------------------------	