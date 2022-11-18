print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		26.POO_3")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

class Coche():
	largoCoche=250 # Propiedad 1
	anchoCoche=120 # Propiedad 2
	ruedas=4	   # Propiedad 3
	enmarcha=False # Propiedad 4

	def arrancar(self):	 # 1er comportamiento
		self.enmarcha=True		
	def estado(self): 	# 2do comportamiento
		if(self.enmarcha):
			return "El coche esta en marca"	 	
		else:
			return "el coche esta parado"
	

miCoche=Coche()	#instanciar la clase

print(" ==> El Largo Del Coche Es : " , miCoche.largoCoche)
print(" ==> El Coche Tiene : " , miCoche.ruedas , "Ruedas")
#print("def arrancar2 = ", miCoche.arrancar())
print("def estado2 = ",miCoche.estado())

