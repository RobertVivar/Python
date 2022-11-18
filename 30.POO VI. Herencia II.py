print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		30.POO VI. Herencia II")
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

class Furgoneta(Vehiculos):

	def carga(self, cargar):
		self.cargado=cargar
		if(self.cargado):
			return "La Fugoneta esta cargada"
		else:
			return "La fugoneta no esta cargada"

# Nuestra Clase Moto hereda la clase Vehiculos
class Moto(Vehiculos):	
	hcaballito=""

	def caballito(self):
		self.hcaballito=" ==> Voy haciendo el caballito"

	def estado(self):
		print(" Marca : " , self.marca, "\n Modelo : ", self.modelo,	"\n En Marcha : ", self.enmarcha,
		  "\n Acelerando : ", self.acelera, "\n Frenando : ", self.frena, "\n" , self.hcaballito )

class VElectricos():
	def __init__(self):
		self.autonomia=100

	def cargarEnergia(self):
		self.cargando=True

miMoto=Moto("Honda", "CBR")
miMoto.caballito()
miMoto.estado()

miFurgorneta=Furgoneta("Renault","Kangoo")
miFurgorneta.arrancar()
miFurgorneta.estado()
print(miFurgorneta.carga(True))

# Herencia Multiple
class BicicletaElectrica(VElectricos,Vehiculos):
	pass

#miBici=BicicletaElectrica("Orbea","HI1030")
