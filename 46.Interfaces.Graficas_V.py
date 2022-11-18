print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		46.Interfaces.Graficas_V ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Creacion de interfaces graficas
# ----------------------------------------------------------------------------
# Libreria Tkinter : Widget Text  (Introducir Texto Largo)
#                    y Button
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

minombre=StringVar()

# Creando una Grilla
#CuadroNombre=Entry(miFrame)
CuadroNombre=Entry(miFrame,textvariable=minombre)
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

textoComentario=Text(miFrame , width=16, height=5)
textoComentario.grid(row=4,column=1, padx=10,pady=10)

scrollVert=Scrollbar(miFrame, command=textoComentario.yview)
scrollVert.grid(row=4,column=2, sticky="nsew")

textoComentario.config(yscrollcommand=scrollVert.set)
# ----------------------------------------------------------------------------
LabelNombre=Label(miFrame, text="Nombre : ")
LabelNombre.grid(row= 0, column=0, sticky="e", padx=10,pady=10)

LabelApellido=Label(miFrame, text="Apellido :")
LabelApellido.grid(row= 2 , column=0 , sticky="e", padx=10,pady=10)

LabelDireccion=Label(miFrame, text="Direccion : ")
LabelDireccion.grid(row= 3, column=0, sticky="e", padx=10,pady=10)

LabelPass=Label(miFrame, text="Passsword : ")
LabelPass.grid(row= 1, column=0, sticky="e", padx=10,pady=10)

LabelComentarios=Label(miFrame, text="Comentarios : ")
LabelComentarios.grid(row= 4, column=0, sticky="e", padx=10,pady=10)

# ----------------------------------------------------------------------------
def Codigboton():
    minombre.set("Robert")
    #set = Establecer Valor
    #get = Para obtener informaicon de un cuadro de texto
# ----------------------------------------------------------------------------
botonEnvio=Button(raiz, text="Enviar", command=Codigboton)
botonEnvio.pack()
# ----------------------------------------------------------------------------
raiz.mainloop()
# ----------------------------------------------------------------------------