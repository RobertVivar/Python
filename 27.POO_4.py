print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		27.POO_4")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Coche():
	
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	#Creando un constructor	
	largoCoche=250 # Propiedad 1
	anchoCoche=120 # Propiedad 2
	ruedas=4	   # Propiedad 3
	enmarcha=False # Propiedad 4

	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def arrancar(self):	 # 1er comportamiento
		self.enmarcha=True		
	def estado(self): 	# 2do comportamiento
		if(self.enmarcha):
			return "El coche esta en marcha"	 	
		else:
			return "el coche esta parado"
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=		
	def arrancar2(self,arrancamos):	 
		self.enmarcha=arrancamos
		if(self.enmarcha):
			return "El Coche Esta En Marca"
		else:
			return "El Coche Esta Parado"		
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=			
	def estado2(self):
		print("El Coche Tiene ",self.ruedas,"Ruedas . Un ancho De ", self.anchoCoche, 
				" y un Largo de ",self.largoCoche)
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

miCoche=Coche()	#instanciar la clase
miCoche2=Coche()
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

print(" ==> El Largo Del Coche Es : " , miCoche2.largoCoche)
print(" ==> El Coche Tiene : " , miCoche2.ruedas , "Ruedas")
print("def arrancar2 = ", miCoche2.arrancar2(True))
print("def estado2 = ",miCoche2.estado2())

print("------------- Creamos el Segundo objeto----------------------")
print("El Largo Del Coche Es : " , miCoche2.largoCoche)
print("El Coche tiene : " , miCoche2.ruedas , "Ruedas")
print(miCoche2.arrancar2(False))
print(miCoche2.estado2())
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=