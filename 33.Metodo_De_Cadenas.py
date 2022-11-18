print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		33.Metodo_De_Cadenas ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#nombre_usuario=input("Introduce tu nombre de Usuario : ")
#print("El nombre es : ", nombre_usuario.upper())
#print("El nombre es : ", nombre_usuario.lower())
#print("El nombre es : ", nombre_usuario.capitalize())

edad=input("Introduce la edad:")

while (edad.isdigit()==False):
	print("Por favor , introduce un valor numerico")
	edad=input("Introduce la edad : ")

if(int(edad)<18):
	print("no puede pasar")
else:
	print("puedes pasar")


