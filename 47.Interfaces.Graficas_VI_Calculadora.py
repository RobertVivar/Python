print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		47.Interfaces.Graficas_VI ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Creacion de interfaces graficas
# ----------------------------------------------------------------------------
# Interfaz Grafica "Calculadora"
# ----------------------------------------------------------------------------
#   1) Importar la libreria tkinter
#   2) * = importar todas las clases
#   3) raiz = Importar la raiz principal
from tkinter import *
raiz=Tk()

miFrame=Frame(raiz)
miFrame.pack() #empaquetando 
# -------------------------------------------------------------------
#       PANTALLA
# -------------------------------------------------------------------
pantalla=Entry(miFrame)
#pantalla.grid(row=1,column=1,padx=10,pady=10)
pantalla.grid(row=1,column=1,padx=10,pady=10,columnspan=4)
pantalla.config(background="black", fg="#03f943", justify="right")
# -------------------------------------------------------------------
#       [FILA 1]
# -------------------------------------------------------------------
boton7=Button(miFrame, text="7", width=3)
boton7.grid(row=2,column=1)
boton8=Button(miFrame, text="8", width=3)
boton8.grid(row=2,column=2)
boton9=Button(miFrame, text="9", width=3)
boton9.grid(row=2,column=3)
botonMult=Button(miFrame, text="*", width=3)
botonMult.grid(row=2,column=4)
# -------------------------------------------------------------------
#       [FILA 2]
# -------------------------------------------------------------------
boton4=Button(miFrame, text="4", width=3)
boton4.grid(row=3,column=1)
boton5=Button(miFrame, text="5", width=3)
boton5.grid(row=3,column=2)
boton6=Button(miFrame, text="6", width=3)
boton6.grid(row=3,column=3)
botonDiv=Button(miFrame, text="/", width=3)
botonDiv.grid(row=3,column=4)
# -------------------------------------------------------------------
#       [FILA 3]
# -------------------------------------------------------------------
boton3=Button(miFrame, text="1", width=3)
boton3.grid(row=4,column=1)
boton2=Button(miFrame, text="2", width=3)
boton2.grid(row=4,column=2)
boton1=Button(miFrame, text="3", width=3)
boton1.grid(row=4,column=3)
botonSuma=Button(miFrame, text="+", width=3)
botonSuma.grid(row=4,column=4)
# -------------------------------------------------------------------
#       [FILA 4]
# -------------------------------------------------------------------
botonRest=Button(miFrame, text="-", width=3)
botonRest.grid(row=5,column=1)
boton0=Button(miFrame, text="0", width=3)
boton0.grid(row=5,column=2)
botonPunto=Button(miFrame, text=".", width=3)
botonPunto.grid(row=5,column=3)
botonIgual=Button(miFrame, text="=", width=3)
botonIgual.grid(row=5,column=4)
# -------------------------------------------------------------------
raiz.mainloop()
