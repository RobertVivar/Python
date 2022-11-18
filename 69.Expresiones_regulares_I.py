print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		69.Expresiones_regulares_I")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#   Que Son :
# -------------------------------------------------------------
#	Son sintaxis y ejemplos sencillos
#   Son Secuencia de caracteres que forman un patron
#   de busqueda
#   Para que Sirven :
# -------------------------------------------------------------
#   Para el trabajo y procesamiento de texto
#   Ejemplos :
# -------------------------------------------------------------
#   - Buscar un texto que se ajusta a un formato determinado
#     (Correo electronico)
#   - Buscar si existe o no una cadena de caracteres dentro 
#     de un texto
#   - contar el numero de coincidencias dentro deun texto
#       ETC
# -------------------------------------------------------------
import re
# -------------------------------------------------------------
cadena="Vamos a aprender expresiones regulares"
# -------------------------------------------------------------
#   Metodo - search() : Hacer una busqueda de una cadena
# -------------------------------------------------------------
print(re.search("aprender",cadena))
print(re.search("aprenderr",cadena))
# -------------------------------------------------------------
#   Buscando con una condicional
# -------------------------------------------------------------
texoBuscar="aprender"
if re.search(texoBuscar, cadena) is not None:
    print("He encontrado el texto")
else:
    print("No He encontrado el texto")
# -------------------------------------------------------------
#   Metodo - start() : 
#           Num de caracteres donde comienza encontrar el texto
# -------------------------------------------------------------
texoBuscar="aprender"
textoEncontrado=re.search(texoBuscar,cadena)
print("Posicion donde Comienza : ", textoEncontrado.start())
print("Posicion donde Termina  : ", textoEncontrado.end())
print("Posicion donde Inicia y Termina  : ", textoEncontrado.span())
# -------------------------------------------------------------
cadena="Vamos a aprender expresiones regulares en Python. Python es una lenguaje Facil"
texoBuscar="Python"
# -------------------------------------------------------------
print(re.findall(texoBuscar, cadena))
# -------------------------------------------------------------
print(len(re.findall(texoBuscar, cadena)))
# -------------------------------------------------------------