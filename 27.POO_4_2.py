print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		27.POO_4_2")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Coche():
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def __init__(self):
	# Metodo Constructor de mi clase
		self.largoCoche=250 # Propiedad 1
		self.anchoCoche=120 # Propiedad 2
		#self.ruedas=4	   	# Propiedad 3
		# para que no modifiquen la propiedad se debe encapsular con 2 guiones debajo
		self.__ruedas =4
		self.enmarcha=False # Propiedad 4	
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def arrancar3(self,arrancamos):	 
		self.enmarcha=arrancamos
		if(self.enmarcha):
			return "El Coche Esta En Marcha"
		else:
			return "El Coche Esta Parado"		
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def estado3(self):
		print("El Coche Tiene ",self.__ruedas,"Ruedas . Un ancho De ", self.anchoCoche, 
				" y un Largo de ",self.largoCoche)
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=	
miCoche=Coche()	#instanciar la clase
print(miCoche.arrancar3(True))
print(miCoche.estado3())
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

print("------------- Creamos el Segundo objeto----------------------")
miCoche2=Coche()	#instanciar la clase
print(miCoche2.arrancar3(False))
miCoche2.ruedas=2 # modiiccando el vcalor d euna propiedad
print(miCoche2.estado3())
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=