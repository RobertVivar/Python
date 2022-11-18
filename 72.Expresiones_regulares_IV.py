print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		72.Expresiones_regulares_IV")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#   Funciones del modulo re (match y search)
# -------------------------------------------------------------
import re
# -------------------------------------------------------------
nombre1="Robert Vivar"
nombre2="Emilio Vivar"
nombre3="Vivar Mori"
nombre4="993202588"
# -------------------------------------------------------------
if re.match("Robert",nombre1):
    print("Encontrado : ",nombre1)
else:
    print("No Encontrado ")  
# -------------------------------------------------------------
#   IGNORECASE = Ignorar Case Sensitive
# -------------------------------------------------------------
if re.match("robert",nombre1,re.IGNORECASE):
    print("Encontrado : ",nombre1)
else:
    print("No Encontrado ")  
# -------------------------------------------------------------
if re.match(".ivar",nombre2,re.IGNORECASE):
    print("Encontrado : ",nombre2)
else:
    print("No Encontrado ")  
# -------------------------------------------------------------
if re.match("\d",nombre4,re.IGNORECASE):
    print("Encontrado : ",nombre4)
else:
    print("No Encontrado ")  
# -------------------------------------------------------------
if re.search("vivar",nombre2,re.IGNORECASE):
    print("Encontrado : ",nombre2)
else:
    print("No Encontrado ")  
# -------------------------------------------------------------