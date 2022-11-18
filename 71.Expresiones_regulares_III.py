print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		71.Expresiones_regulares_III")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#   Expresiones Reguladres
#   Rangos
# -------------------------------------------------------------
import re
# -------------------------------------------------------------
lista_nombres=['Robert', 
                'Emilio',
                'IBK',
                'BBVA',
                'Torre IBK',]
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('[o-t]',elemento):
        print("[o-t]] : " , elemento)
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('^[O-T]',elemento):
        print("^[O-T]] : " , elemento)
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('[o-t]$',elemento):
        print("[o-t]$ : " , elemento)
# -------------------------------------------------------------
lista_nombres=['Ma1', 
                'Se1',
                'Ma2',
                'Ba1',
                'Ma3',
                'Va1',
                'Va2',
                'Ma4']
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('Ma[0-3]',elemento):
        print("Ma[0-3] : " , elemento)
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('Ma[^0-3]',elemento):
        print("Ma[^0-3] : " , elemento)
# -------------------------------------------------------------
lista_nombres=['Ma1', 
                'Se1',
                'Ma2',
                'Ba1',
                'Ma3',
                'Va1',
                'Va2',
                'Ma4',
                'MaA',
                'Ma5',
                'MaB',
                'MaC']
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('Ma[0-3A-B]',elemento):
        print("Ma[0-3A-B] : " , elemento)
# -------------------------------------------------------------
lista_nombres=['Ma.1', 
                'Se1',
                'Ma2',
                'Ba1',
                'Ma:3',
                'Va1',
                'Va2',
                'Ma4',
                'MaA',
                'Ma.5',
                'MaB',
                'Ma:C']
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('Ma[.:]',elemento):
        print("Ma[.:] : " , elemento)
# -------------------------------------------------------------