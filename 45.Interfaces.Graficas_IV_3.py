print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		45.Interfaces.Graficas_IV_3 ")
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
miFrame=Frame(raiz,width=1200,height=800)
miFrame.pack()
# ----------------------------------------------------------------------------
#CuadroTexto=Entry(raiz)
#CuadroTexto.pack()

# Creando una Grilla
CuadroNombre=Entry(miFrame)
CuadroNombre.grid(row= 0, column=1, padx=10,pady=10)
CuadroNombre.config(fg="red", justify="center")

CuadroPass=Entry(miFrame)
CuadroPass.grid(row= 1, column=1, padx=10,pady=10)
CuadroPass.config(show="*" ,fg="red", justify="left")

CuadroApellido=Entry(miFrame)
CuadroApellido.grid(row= 2, column=1, padx=10,pady=10)
CuadroApellido.config(fg="red", justify="left")

CuadroDireccion=Entry(miFrame)
CuadroDireccion.grid(row= 3, column=1, padx=10,pady=10)
# ----------------------------------------------------------------------------
LabelNombre=Label(miFrame, text="Nombre : ")
LabelNombre.grid(row= 0, column=0, sticky="e", padx=10,pady=10)

LabelApellido=Label(miFrame, text="Apellido :")
LabelApellido.grid(row= 2 , column=0 , sticky="e", padx=10,pady=10)

LabelDireccion=Label(miFrame, text="Direccion de casa : ")
LabelDireccion.grid(row= 3, column=0, sticky="e", padx=10,pady=10)

LabelPass=Label(miFrame, text="Passsword : ")
LabelPass.grid(row= 1, column=0, sticky="e", padx=10,pady=10)
# ----------------------------------------------------------------------------
raiz.mainloop()
# ----------------------------------------------------------------------------