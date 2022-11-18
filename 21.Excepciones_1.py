print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		21.Excepciones_1")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def suma(num1,num2):
	return num1+num2
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def resta(num1,num2):	
	return num1-num2
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def multiplica(num1,num2):	
	return num1*num2
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def divide(num1,num2):	
	try:
		return num1/num2
	except ZeroDivisionError:
		print("No se puede dividiri entre 0")
		return "Operacion erronea"	
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
op1=(int(input("Introduce el primer numero : ")))
op2=(int(input("Introduce el primer numero : ")))
operacion=input("Introduce la operacion a realizar (suma,resta,mulitiplica,divide): ")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
if operacion =="suma":
	print(suma(op1,op2))
if operacion =="resta":
	print(resta(op1,op2))
if operacion =="multiplica":
	print(multiplica(op1,op2))
if operacion =="divide":
	print(divide(op1,op2))
else:
	print("Operacion no Contemplada")
print("Operacion ejecutada , Continuacion de ejecucion del programa")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=