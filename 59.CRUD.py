print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		59.CRUD ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#	APLICACION GRAFICA [CRUD]
#	[	PRACTICA CALIFICADA # (1)	]
# -------------------------------------------------------------
from tkinter import *
raiz=Tk()
# -------------------------------------------------------------
miFrame=Frame(raiz,width=1200,height=800)
miFrame.pack()
# -------------------------------------------------------------
minombre=StringVar()
# -------------------------------------------------------------
#       PANTALLA
# -------------------------------------------------------------
pantalla=Entry(miFrame)
#pantalla.grid(row=1,column=1,padx=10,pady=10,columnspan=4)
#pantalla.config(background="RED", fg="#03f943", justify="right")
# -------------------------------------------------------------
#		[COLUMNA 1]
# -------------------------------------------------------------
LabelNombre=Label(miFrame, text="ID : ")
LabelNombre.grid(row= 0, column=0, sticky="e", padx=10,pady=10)

LabelNombre=Label(miFrame, text="Nombre : ")
LabelNombre.grid(row= 1, column=0, sticky="e", padx=10,pady=10)

LabelPass=Label(miFrame, text="Passsword : ")
LabelPass.grid(row= 2, column=0, sticky="e", padx=10,pady=10)

LabelApellido=Label(miFrame, text="Apellido :")
LabelApellido.grid(row= 3 , column=0 , sticky="e", padx=10,pady=10)

LabelDireccion=Label(miFrame, text="Direccion : ")
LabelDireccion.grid(row= 4, column=0, sticky="e", padx=10,pady=10)

LabelComentarios=Label(miFrame, text="Comentarios : ")
LabelComentarios.grid(row= 5, column=0, sticky="e", padx=10,pady=10)
# -------------------------------------------------------------
#		[COLUMNA 2]
# -------------------------------------------------------------
CuadroID=Entry(miFrame,textvariable=id)
CuadroID.grid(row= 0, column=1, padx=10,pady=10)
#CuadroID.config(fg="red", justify="center")

CuadroNombre=Entry(miFrame,textvariable=minombre)
CuadroNombre.grid(row= 1, column=1, padx=10,pady=10)
CuadroNombre.config(fg="red", justify="left")

CuadroPass=Entry(miFrame)
CuadroPass.grid(row= 2, column=1, padx=10,pady=10)
CuadroPass.config(show="*" ,fg="red", justify="left")

CuadroApellido=Entry(miFrame)
CuadroApellido.grid(row= 3, column=1, padx=10,pady=10)
#CuadroApellido.config(fg="red", justify="left")

CuadroDireccion=Entry(miFrame)
CuadroDireccion.grid(row= 4, column=1, padx=10,pady=10)

textoComentario=Text(miFrame , width=16, height=5)
textoComentario.grid(row=5,column=1, padx=10,pady=10)

scrollVert=Scrollbar(miFrame, command=textoComentario.yview)
scrollVert.grid(row=5,column=2, sticky="nsew")

textoComentario.config(yscrollcommand=scrollVert.set)
# ----------------------------------------------------------------------------
def Codigboton():
    minombre.set("Robert")
    #set = Establecer Valor
    #get = Para obtener informaicon de un cuadro de texto
# ----------------------------------------------------------------------------
botonCreate=Button(miFrame, text="Create", command=Codigboton)
botonCreate.grid(row= 6, column=0, padx=10,pady=10)
#botonCreate.pack()

botonRead=Button(miFrame, text="Read", command=Codigboton)
botonRead.grid(row= 6, column=1, padx=10,pady=10)
#botonRead.pack()

botonUpdate=Button(miFrame, text="Update", command=Codigboton)
botonUpdate.grid(row= 6, column=2, padx=10,pady=10)
#botonUpdate.pack()

botonDelete=Button(miFrame, text="Delete", command=Codigboton)
botonDelete.grid(row= 6, column=3, padx=10,pady=10)
#botonDelete.pack()
# ----------------------------------------------------------------------------
raiz.mainloop()
# ----------------------------------------------------------------------------
