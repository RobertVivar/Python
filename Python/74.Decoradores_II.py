print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		74.Decoradores_II")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#	CREANDO LA FUNCION DECORADORA
# -------------------------------------------------------------
def funcion_decoradora(funcion_parametro):
	#	*args = Recibe un numero indeterminado
    def funcion_interior(*args):
    	# Acciones adicionales que decoran
    	print("Vamos a realizar un calculo: ")
    	print("=============================")
    	funcion_parametro(*args)
    	#Acciones adicionales que decoran
    	print("Hemos terminado el calculo")
    return funcion_interior
# -------------------------------------------------------------
def funcion_decoradora2(funcion_parametro):
	#	*args = Recibe un numero indeterminado
    def funcion_interior(*args,**kwargs):
    	# Acciones adicionales que decoran
    	print("Vamos a realizar un calculo: ")    	
    	funcion_parametro(*args)#,**kwargs):
    	#Acciones adicionales que decoran
    	print("Hemos terminado el calculo")
    return funcion_interior
# -------------------------------------------------------------
def funcion_decoradora3(funcion_parametro):
	#	*args = Recibe un numero indeterminado
    def funcion_interior(*args,**kwargs):
    	# Acciones adicionales que decoran
    	print("Vamos a realizar un calculo: ")    	
    	funcion_parametro(*args,**kwargs)
    	#Acciones adicionales que decoran
    	print("Hemos terminado el calculo")
    return funcion_interior
# -------------------------------------------------------------
@funcion_decoradora
def suma(num1,num2,num3):
    print(num1+num2+num3)
# -------------------------------------------------------------
@funcion_decoradora
def resta(num1,num2):
    print(num1-num2)
# -------------------------------------------------------------
@funcion_decoradora2
def potencia(base,exponente):
	print(pow(base,exponente))
# -------------------------------------------------------------
@funcion_decoradora3
def potencia2(base,exponente):
	print(pow(base,exponente))
# -------------------------------------------------------------
suma(7,5,8)
resta(12,10)
potencia(5,3)
potencia2(base=5,exponente=3)
# -------------------------------------------------------------