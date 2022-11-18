print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		53.Interfaces.Graficas_XII ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Que son Las Ventanas Emergentes
#   Ventanas modales para informar , avisar 
#   o permitir realizar tareas al usuario
# ----------------------------------------------------
from tkinter import *
from tkinter import messagebox
root=Tk()
# -------------------------------------------------------------
def infoAdicional():
	messagebox.showinfo("Procesador de Robert" , "Procesador de textos version 2019")

def avisoLincencia():
	messagebox.showwarning("Licencia" , "Producto bajo licencia GNU")	

def salirAplicacion_question():
	valor=messagebox.askquestion("Salir" , "Deseas Salir De La Aplicacion?")
	if valor=="yes":
		root.destroy()

def salirAplicacion():
	valor=messagebox.askokcancel("Salir" , "Deseas Salir De La Aplicacion?")
	if valor==True:
		root.destroy()

def cerrarDocumento():
	valor=messagebox.askretrycancel("Reintentar" , "No Es Posible Cerrar . Documento Bloqueado")
	if valor==False:
		root.destroy()
# -------------------------------------------------------------
barraMenu=Menu(root)
#root.config(menu=barraMenu)
root.config(menu=barraMenu, width=300 ,  height=300)
# -------------------------------------------------------------
#archivoMenu=Menu(barraMenu)
archivoMenu=Menu(barraMenu, tearoff=0)
#	Agregar Sub Menu
archivoMenu.add_command(label="Nuevo")
archivoMenu.add_command(label="Guardar")
archivoMenu.add_command(label="Guardar Como")
archivoMenu.add_separator()
archivoMenu.add_command(label="Cerrar", command=cerrarDocumento)
archivoMenu.add_command(label="Salir", command=salirAplicacion)
# -------------------------------------------------------------
archivoEdicion=Menu(barraMenu, tearoff=0)
archivoEdicion.add_command(label="Copiar")
archivoEdicion.add_command(label="Cortar")
archivoEdicion.add_command(label="Pegar")
# -------------------------------------------------------------
archivoHerramientas=Menu(barraMenu)
# -------------------------------------------------------------
archivoAyuda=Menu(barraMenu, tearoff=0)
archivoAyuda.add_command(label="Licencia", command=avisoLincencia)
#archivoAyuda.add_command(label="Acerca De ...")
archivoAyuda.add_command(label="Acerca De ...", command=infoAdicional)
# -------------------------------------------------------------
barraMenu.add_cascade(label="Archivo", menu=archivoMenu)
barraMenu.add_cascade(label="Edicion", menu=archivoEdicion)
barraMenu.add_cascade(label="Herramientas", menu=archivoHerramientas)
barraMenu.add_cascade(label="Ayuda", menu=archivoAyuda)
# -------------------------------------------------------------
root.mainloop()
# -------------------------------------------------------------