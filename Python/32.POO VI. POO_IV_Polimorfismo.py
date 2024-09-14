print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		32.POO VI. POO_IV_Polimorfismo")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

class Coche():
	def desplazamiento(self):
		print("Me desplazo utilizando 4 ruedas")

class Moto():
	def desplazamiento(self):
		print("Me desplazo utilizando 2 ruedas")

class Camion():
	def desplazamiento(self):
		print("Me desplazamiento utilizando 6 ruedas")

def desplazamientoVehiculo(vehiculo):
	vehiculo.desplazamiento()

miVehiculo=Moto()
desplazamientoVehiculo(miVehiculo)
miVehiculo2=Coche();
desplazamientoVehiculo(miVehiculo2)
miVehiculo3=Camion()
desplazamientoVehiculo(miVehiculo3)