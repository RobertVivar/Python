print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		43.Interfaces.Graficas_II ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Estructura de ventana  con Tkinter
# ----------------------------------------------------------------------------
# Raiz (tk)
# -----------
#	Frame (Aglutinador de elementos) : D
#	Dentro de este Frame se conoce como Widgets.
# 	Es decir botones , menus desplegables, casillas de verificaciones , 
#	botones de radio , cuadro de texto , todo se conoce en python como Widgets
# ----------------------------------------------------------------------------
from tkinter import *

raiz=Tk()
raiz.title("Ventana de Pruebas")
raiz.resizable(True,False)
raiz.iconbitmap("icono.ico")
raiz.geometry("560x350")
# ---------------------------------------------
# color azul
raiz.config(bg="blue")

miFrame=Frame()
miFrame.pack()
#miFrame.pack(side="left")
#miFrame.pack(side="bottom")
#miFrame.pack(side="left",anchor="n")
#miFrame.pack(side="right",anchor="n")

#miFrame.pack(fill="x")
#miFrame.pack(fill="y")
#miFrame.pack(fill="y",expand="True")
#miFrame.pack(fill="both", expand="True")
# ---------------------------------------------
# cambiar el color de fondo
miFrame.config(bg="red")
#Tamaño al frame
miFrame.config(width="650",height="350")
# Borde
miFrame.config(bd=35)
# Tipo de Borde
#miFrame.config(relief="groove")
miFrame.config(relief="sunken")
miFrame.config(cursor="pirate")

raiz.mainloop()
# ---------------------------------------------