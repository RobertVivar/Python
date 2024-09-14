print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		42.Interfaces.Graficas_I ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Son Ventanas , con las que interactuamos con los programas o aplicaciones
# Tambien denomiandas GUI (Interfaz Grafica de Usuario) , son intermediario 
# enter el programa y el usuario Formadas por un conjunto de graficos como 
# Ventanas , botones , menus , casillas de verificacion, etc
# Librerias con las que trabajana para crear GUI : 
#	-	Tkinter
#	-	WxPython
#	-	PyQT
#	-	PyGTK
# ----------------------------------------------------------------------------
#	Tkinter : Es un puente entre Python y la ñlibreria "TCL/TK"
# 	Se intala con el siguiente comando : " $sudo apt-get install python3-tk "
# ----------------------------------------------------------------------------
# Estructura de ventana  con Tkinter
# ----------------------------------------------------------------------------
# Raiz (tk)
# Frame (Aglutinador de elementos) : Dentro de este Frame se conoce como Widgets.
# 	Es decir botones , menus desplegables, casillas de verificaciones , 
#	botones de radio , cuadro de texto , todo se conoce en python como Widgets
# ----------------------------------------------------------------------------
from tkinter import *
raiz=Tk()
#Titulo a la ventana
raiz.title("Ventana de pruebas")
# Utilizamos el objeto resizable
#raiz.resizable(0,0)	#	width , height
#raiz.resizable(True,False)	
raiz.resizable(500,20)	

# Cambiar Icono ( .ico ) # google => Conversor  .ico
# Metodo mainloop() : es un bucle infinito , debe estar siempre al final
raiz.mainloop() 
