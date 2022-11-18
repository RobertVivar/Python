print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		41.Archivos.Guardado_Permanente_2 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Guardar datos de forma permanente en ficheros
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
import pickle # Libreria
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Persona:
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
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
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	personas=[]
	#Creando el Constructor
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def __init__(self):
		# agregar informacion binario = ab+
		listaDePersonas=open("ficheroExterno","ab+") 
		listaDePersonas.seek(0)
		try:
			# Volcado de informacion
			self.personas=pickle.load(listaDePersonas)
			print("Se Cargaron {} Personas Del Fichero Externo".format(len(self.personas)))
		except:
			print("El fichero Esta Vacio")
		finally:
			listaDePersonas.close()
			del(listaDePersonas)	#Borrar
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def agregarPersonas(self,p):
		self.personas.append(p)
		self.guardarPersonasEnFicheroExterno()	# add linea
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def mostrarPersonas(self):
		for p in self.personas:
			print(p)
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def guardarPersonasEnFicheroExterno(self):
		listaDePersonas=open("ficheroExterno","wb")	# Escribir Informacion
		pickle.dump(self.personas, listaDePersonas)
		listaDePersonas.close()
		del(listaDePersonas)
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	# mostrar datos del fichero
	#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	def mostrarInfoFicheroExterno(self):
		print("La Informacion Del Fichero Externo Es La Siguiente : ")
		for p in self.personas:
			print(" ==> ", p)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista=ListaPersonas()
# Declaramos Una Variable
persona=Persona("Robert","Masculino",30)
miLista.agregarPersonas(persona)
miLista.mostrarInfoFicheroExterno()
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=