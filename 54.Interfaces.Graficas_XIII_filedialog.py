print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		54.Interfaces.Graficas_XIII ")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# ----------------------------------------------------------------------------
# Que son Las Ventanas Emergentes
#   Ventanas modales .
#			Abrir Archivos
# ----------------------------------------------------
from tkinter import *
from tkinter import filedialog
# -------------------------------------------------------------
root=Tk()
# -------------------------------------------------------------
def abreFichero():
	#fichero=filedialog.askopenfilename(title="Abrir")
	# Direccionar Al Directerio C
	#fichero=filedialog.askopenfilename(title="Abrir",initialdir="C:")
	fichero=filedialog.askopenfilename(title="Abrir",initialdir="C:",
			 filetypes=(("Ficheros de Excel" ,"*.xlsx"),
	 					("Ficheros de Texto" ,"*.txt"),
	 					("Todos Los ficheros","*.*")))
	print(fichero)
# -------------------------------------------------------------
Button(root, text="Abrir Fichero", command=abreFichero).pack()

# -------------------------------------------------------------
root.mainloop()
# -------------------------------------------------------------