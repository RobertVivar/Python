print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		41.Archivos.Guardado_Permanente ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Guardar datos de forma permanente en ficheros
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
import pickle # Libreria
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Persona:
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def __init__(self, nombre, genero , edad):
		self.nombre=nombre
		self.genero=genero
		self.edad=edad
		print("Se Ha Creado Una Persona Nueva Con El Nombre De : ", self.nombre )
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	#__str__  = Convertir en cadena de texto a informacion de un objeto
	def __str__(self):
		return "{} {} {}".format(self.nombre, self.genero, self.edad)
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class ListaPersonas:
	personas=[]
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def agregarPersonas(self,p):
		self.personas.append(p)
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def  mostrarPersonas(self):
		for p in self.personas:
			print(p)

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista=ListaPersonas()
p=Persona("Robert", "Masculino",29)
miLista.agregarPersonas(p)

p=Persona("Emilio", "Masculino",30)
miLista.agregarPersonas(p)

p=Persona("Vivar", "Masculino",31)
miLista.agregarPersonas(p)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista.mostrarPersonas()
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=