print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		23.Excepciones_3")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
def evaluaEdad(edad):
	if edad<0:
		raise MipropioError("no se permiten edades negativas")

	if edad<20:
		return "Eres muy joven"
	elif edad < 40:
		return "Eres Joven"
	elif edad <65 :
		return "Eres Maduro"
	elif edad <100 :
		return "cuidate.."

print(evaluaEdad(30))

print(evaluaEdad(-15))