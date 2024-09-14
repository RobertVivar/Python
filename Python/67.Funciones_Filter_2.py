print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		66.Funciones_Filter_2 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#	Funciones Filter y utilidad
#	Comprobar o verificar que los elementos de una secuencia cumplen
#	una condicion, devolviendo un iterador con los elementos
#	que cumplen dicha condicion
# -------------------------------------------------------------
def numero_par(num):
	if num%2==0:
		return True
# -------------------------------------------------------------
numeros=[2,88,4,29,9,23,22,1]
#print(filter(numero_par,numeros))
#print("list(filter()) : ", list(filter(numero_par,numeros)))
#print("lambda : " , list(filter(lambda numero_par: numero_par%2==0, numeros)))
# -------------------------------------------------------------
class Empleado:
	def __init__(self, nombre, cargo, salario):
		self.nombre=nombre
		self.cargo=cargo
		self.salario=salario

	def __str__(self):
		return "[{}] , que trabaja como [{}] , tiene un salario de {} $".format(self.nombre,self.cargo,self.salario)
#-------------------------------------------------------------
listaEmpleados=[
Empleado("Robert","Analista",6000),
Empleado("Emilio","Programador",5000),
Empleado("Vivar","Senior",7000),
Empleado("Mori","Developer",6500),
Empleado("BigData","Data",8000),
]
#-------------------------------------------------------------
salarios_altos=filter(lambda empleado:empleado.salario>5000,listaEmpleados)
# -------------------------------------------------------------
for empleado_salario in salarios_altos:
	print(empleado_salario)