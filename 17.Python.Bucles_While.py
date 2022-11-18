print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		17.Python.Bucles_While")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
i=1
while i<=4:
	print("ejecucion "+ str(i))
	i=i+1

print("Termino la ejecucion")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#edad=int(input("Introduce tu edad : "))
edad=29

while edad<0:
	print("Edad incorrecta , vuelve intentarlo")
	edad=int(input("Introduce tu edad ; "))

print("Gracias por colaborar ")
print("Edad del aspirante : "+ str(edad))
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

import math # IMPORTAR
#num = int(input("Introduce un numero por favor : "))
numero = 7
intentos=0
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
while numero<0:
	print("No se puede hallar la raiz de un numero negativo")
	if intentos==2:
		print("Has consumido demasiados intentos, El programa finalizo")
		break;
	#numero = int(input("Introduce un numero por favor : "))
	numero = 7
	if numero==0:
		intentos=intentos+1
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
if intentos<2:
	solucion=math.sqrt(numero)
	print("La raiz cuadrada de "+ str(numero) + " es " +  str(solucion))
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=