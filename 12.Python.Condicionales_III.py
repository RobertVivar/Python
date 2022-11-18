print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		11.Python.Condicionales_II")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
edad=7

if edad<100:
	print("edad es correcta")
else:
	print("edad es incorrecta")

print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

salario = int(input("Introduce un salario del presidente : "))
#print("Salario " + salario)
print("Salario " + str(salario))

salarioD = int(input("Introduce un salario del Director : "))
print("Salario " + str(salarioD))

salarioA = int(input("Introduce un salario del Administrativo : "))
print("Salario " + str(salarioD))
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
if salario < salarioD < salarioA:
	print("Todo funciona correcta")
else:
	print("Algo falla ")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=