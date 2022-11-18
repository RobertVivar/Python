print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		44.Interfaces.Graficas_III ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Creacion de interfaces graficas
# ----------------------------------------------------------------------------
# Libreria Tkinter : Widget Label
# ----------------------------------------------------------------------------
# Los label nos permite mostrar texto o imagenes
# Tienen como unica finalidad mostrar texto , 
# no se puede interactuar con el (Borrar , arrastrar )
#	Sintaxis
#	variableLAbel=Label(contenedor,opciones)
# --------------------------------------------------------------------------|
#	Opcion 				|		Descripcion									|
# --------------------------------------------------------------------------|
#	Text          		|	Texto que se muestra en el label                |
#	Anchor				|	Controla la posicion del texto si hay espacio   |
#						|	suficiente para el (center por defecto)         |
#	Bg                  |	Color de fondo                                  |
#	Bitmap              |	Mapa de Bits que se mostrara como grafico       |
#	Bd                  |	Grosor del borde ( 2 px por defecto)            |
#	Font                |	Tipo de fuente a mostrar                        |
#	Fg                  |	Color de la fuente     (Frontal)                |
#	Width               |	Ancho  de Label en caracteres (no en pixeles)   |
#	Height              |	Altura de label en carcateres (no en pixeles)   |
#	image	            | 	Muestra imagen en el Label en lugar de texto    |
#	Justify             |	Justificacion del texto del label               |
# --------------------------------------------------------------------------|
from tkinter import *

root=Tk()
miFrame=Frame(root, width=500,height=400)
miFrame.pack()

#miLabel=Label(miFrame,text="Hola Pyhton")
#miLabel.place(x=100,y=200)

#Label(miFrame,text="Hola Pyhton").place(x=100,y=200)
#Label(miFrame,text="Hola Pyhton",fg="red", font=(18)).place(x=100,y=200)
#Label(miFrame,text="Hola Pyhton",fg="red", font=("Arial", 30)).place(x=100,y=200)
miImagen=PhotoImage(file="gif.gif")
Label(miFrame, image=miImagen ).place(x=100,y=200)

#miLabel.pack()
root.mainloop()