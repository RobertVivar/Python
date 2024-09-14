print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		29.POO VI. Herencia")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
class Vehiculos():

	def __init__(self,marca,modelo):
		self.marca=marca
		self.modelo=modelo
		self.enmarcha=False
		self.acelera=False
		self.frena=False

	def arrancar(self):
		self.enmarcha=True

	def acelerar(selt):
		self.acelera=True

	def frenar(self):
		self.frena=True

	def estado(self):
		print(" Marca : " , self.marca, "\n Modelo : ", self.modelo,	"\n En Marcha : ", self.enmarcha,
		  "\n Acelerando : ", self.acelera, "\n Frenando : ", self.frena)

# Nuestra Clase Moto hereda la clase Vehiculos
class Moto(Vehiculos):	
	pass				# Para No Construir de momento

miMoto=Moto("Honda", "CBR")

miMoto.estado()