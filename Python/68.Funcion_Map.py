print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		68.Funcion_Map ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#	Aplica un funcion a cada elemento de una lista iterable
#	(listas, tuplas, etc) devolviendo una lista con los resultados
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
# -------------------------------------------------------------
def calculo_comision(empleado):
	if(empleado.salario<=7000):
		empleado.salario=empleado.salario*1.03
	return empleado
print("--------------------------------------------------------")
print("			filter map ")
print("--------------------------------------------------------")
salarios_altos=filter(lambda empleado:empleado.salario>5000,listaEmpleados)
# -------------------------------------------------------------
for empleado_salario in salarios_altos:
	print(empleado_salario)
print("--------------------------------------------------------")
print("			funcion map ")
print("--------------------------------------------------------")
#-------------------------------------------------------------
listaEmpleadosComision=map(calculo_comision,listaEmpleados)
# -------------------------------------------------------------
for empleado in listaEmpleadosComision:
	print(empleado)
# -------------------------------------------------------------