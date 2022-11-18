print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		45.Interfaces.Graficas_IV_2 ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Creacion de interfaces graficas
# ----------------------------------------------------------------------------
# Libreria Tkinter : Widget Entry
# ----------------------------------------------------------------------------
# Es el Cuadro de Texto que sirve para introducir Texto
# ----------------------------------------------------------------------------
from tkinter import *
raiz=Tk()
# ----------------------------------------------------------------------------
miFrame=Frame(raiz,width=1000,height=800)
miFrame.pack()
# ----------------------------------------------------------------------------
#CuadroTexto=Entry(raiz)
#CuadroTexto.pack()

# Creando una Grilla
CuadroNombre=Entry(miFrame)
CuadroNombre.grid(row= 0, column=1, padx=10,pady=10)

CuadroApellido=Entry(miFrame)
CuadroApellido.grid(row= 1, column=1, padx=10,pady=10)

CuadroDireccion=Entry(miFrame)
CuadroDireccion.grid(row= 2, column=1, padx=10,pady=10)

LabelNombre=Label(miFrame, text="Nombre : ")
LabelNombre.grid(row= 0, column=0, sticky="e", padx=10,pady=10)

LabelApellido=Label(miFrame, text="Apellido :")
LabelApellido.grid(row= 1 , column=0 , sticky="e", padx=10,pady=10)

LabelDireccion=Label(miFrame, text="Direccion de casa : ")
LabelDireccion.grid(row= 2, column=0, sticky="e", padx=10,pady=10)
# ----------------------------------------------------------------------------
raiz.mainloop()
# ----------------------------------------------------------------------------