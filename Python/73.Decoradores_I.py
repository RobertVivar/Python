print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		73.Decoradores_I")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#   Funciones que a su vez añaden funcionalidades a otras funciones
#   Por eso se las denonima "decoradores" , por que "Decoran"
#   a otras funciones
# -------------------------------------------------------------
#   ESTRUCTURA DE UN DECORADOR
# -------------------------------------------------------------
#   3 FUNCIONES (A , B y C)
#   Donde A Recibe como parametro a B , para devoler C.
#   Un decorador devuelve una funcion
# -------------------------------------------------------------
#	CREANDO LA FUNCION DECORADORA
# -------------------------------------------------------------
def funcion_decoradora(funcion_parametro):
    def funcion_interior():
    	# Acciones adicionales que decoran
    	print("Vamos a realizar un calculo: ")
    	funcion_parametro()
    	#Acciones adicionales que decoran
    	print("Hemos terminado el calculo")
    return funcion_interior
# -------------------------------------------------------------
@funcion_decoradora
def suma():
    print(15+20)
# -------------------------------------------------------------
@funcion_decoradora
def resta():
    print(30-10)
# -------------------------------------------------------------
suma()
resta()
# -------------------------------------------------------------