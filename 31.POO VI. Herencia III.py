print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		31.POO VI. Herencia III")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#funcion super : llama al metodo de la clase padre
# ininstance :  si una clase pertence a una clase o no
class Persona(): #===================================
	def __init__(self, nombre, edad, Lugar_residencia):
		self.nombre=nombre
		self.edad=edad
		self.Lugar_residencia=Lugar_residencia
	
	def descripcion(self):
		print(" Nombre : ", self.nombre , " \n Edad : ", self.edad , "\n Residencia : ", self.Lugar_residencia)

class Empleado(Persona): #===================================
	#def __init__(self,salario,antiguedad):
	def __init__(self,salario,antiguedad,nombre_empleado,edad_empleado,residencia_empleado):
		#super().__init__("Antonio", 55, "España")
		#Constructoer clase padre
		super().__init__(nombre_empleado, edad_empleado, residencia_empleado)
		self.salario=salario
		self.antiguedad=antiguedad

	def descripcion(self):
		super().descripcion()
		print(" Salario : " , self.salario, "\n Antiguedad ", self.antiguedad)

#Antonio=Empleado(1500, 15)
Antonio=Empleado(1500, 15,"Manuel",55,"Peru")
#Antonio=Empleado(Manuel",55,"Peru")
Antonio.descripcion()
#Antonio2 = Persona("Antonio",55,"España")
#Antonio=Empleado(1500, 15,"Manuel",55,"Peru")
print(isinstance(Antonio, Empleado))