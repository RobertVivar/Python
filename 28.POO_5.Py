print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		28.POO_5")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Coche():
	# Encasulapcion de variables
	
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def __init__(self):
	# Metodo Constructor de mi clase
	# para que no modifiquen la propiedad se debe encapsular con 2 guiones debajo
		self.__largoCoche=250 # Propiedad 1
		self.__anchoCoche=120 # Propiedad 2				
		self.__ruedas =4		# Propiedad 3		
		self.__enmarcha=False # Propiedad 4
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def arrancar3(self,arrancamos):	 
		self.__enmarcha=arrancamos
		if(self.__enmarcha):
			chequeo=self.__chequeo_interno()

		if(self.__enmarcha and chequeo):
			return "El Coche Esta En Marcha"
		elif(self.__enmarcha and chequeo==False):
			return "Algo ha ido mal en el chequeo, no podemos arrancar"
		else:
			return "El Coche Esta Parado"		
	
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def estado3(self):
		print("El Coche Tiene ",self.__ruedas,"Ruedas . Un ancho De ", self.__anchoCoche, " y un Largo de ",self.__largoCoche)
	
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def __chequeo_interno(self): #encapsulando con 2 guiones abajo
		print("Realizando chequeo interno")
		self.gasolina="ok"
		self.aceite="ok"
		self.puertas="cerradas"

		if(self.gasolina=="ok" and self.aceite=="ok" and self.puertas=="cerradas"):
			return True
		else:
			return False
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miCoche=Coche()	#instanciar la clase
print(miCoche.arrancar3(True))
print(miCoche.estado3())
#print(miCoche.chequeo_interno())
print("------------- Creamos el Segundo objeto----------------------")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miCoche2=Coche()	#instanciar la clase
print(miCoche2.arrancar3(False))
print(miCoche2.estado3())
#print(miCoche2.chequeo_interno())
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
