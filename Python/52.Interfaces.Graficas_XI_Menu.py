print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		52.Interfaces.Graficas_XI ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Que son los Menus
#   Barra en la parte superior con : 
#   opciones de sub-menu
# ----------------------------------------------------------------------------
from tkinter import *
root=Tk()
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
archivoMenu.add_command(label="Cerrar")
archivoMenu.add_command(label="Salir")
# -------------------------------------------------------------
archivoEdicion=Menu(barraMenu, tearoff=0)
archivoEdicion.add_command(label="Copiar")
archivoEdicion.add_command(label="Cortar")
archivoEdicion.add_command(label="Pegar")
# -------------------------------------------------------------
archivoHerramientas=Menu(barraMenu)
# -------------------------------------------------------------
archivoAyuda=Menu(barraMenu, tearoff=0)
archivoAyuda.add_command(label="Licencia")
archivoAyuda.add_command(label="Acerca De ...")
# -------------------------------------------------------------
barraMenu.add_cascade(label="Archivo", menu=archivoMenu)
barraMenu.add_cascade(label="Edicion", menu=archivoEdicion)
barraMenu.add_cascade(label="Herramientas", menu=archivoHerramientas)
barraMenu.add_cascade(label="Ayuda", menu=archivoAyuda)
# -------------------------------------------------------------
root.mainloop()
# -------------------------------------------------------------