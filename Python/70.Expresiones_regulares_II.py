print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		70.Expresiones_regulares_II")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# -------------------------------------------------------------
#   Metacaracteres (Caracteres Comodin) 
#   Anclas y clases de caracteres
# -------------------------------------------------------------
import re
# -------------------------------------------------------------
lista_nombres=['Vivar Robert', 
                'Vivar Emilio',
                'Vivar Mori',
                'Robert Emilio',
                'Python Data']
# -------------------------------------------------------------
#   ^ = TODOS LOS QUE EMPIEZAN 
for elemento in lista_nombres:
    if re.findall('^Vivar',elemento):
        print("^ : " , elemento)
# -------------------------------------------------------------
#   $ = TODOS LOS QUE TERMINAN
for elemento in lista_nombres:    
    if re.findall('Emilio$',elemento):
        print("$ : " , elemento)
# -------------------------------------------------------------
#   BUSCANDO DOMINIOS
# -------------------------------------------------------------
lista_nombres=['http://robert.com.es', 
                'ftp://robert.com.es', 
                'https://robert.com.es', 
                'ftps://robert.com.es'  ]
# -------------------------------------------------------------
for elemento in lista_nombres:
    if re.findall('^ftp',elemento):
        print("^ : " , elemento)
# -------------------------------------------------------------
lista_nombres=['http://robert.ñ.com.es', 
                'ftp://robert.com.es',                 
                'htps://robert.ñ.com.pe'  ]
# -------------------------------------------------------------
#   [ñ] = BUSCAR SI SE ENCUENTRA ñ
for elemento in lista_nombres:
    if re.findall('[ñ]',elemento):
        print("[ñ] : " , elemento)
# -------------------------------------------------------------
lista_nombres=['hombres', 
                'mujeres',                 
                'mascotas',
                'niños',
                'niñas']
# -------------------------------------------------------------
#   [ñ] = BUSCAR SI SE ENCUENTRA oa
for elemento in lista_nombres:
    if re.findall('niñ[oa]s',elemento):
        print("niñ[oa]s : " , elemento)
# -------------------------------------------------------------
lista_nombres=['hombres', 
                'mujeres',                 
                'mascotas',
                'camion',
                'camión']
# -------------------------------------------------------------
#   [ñ] = BUSCAR SI SE ENCUENTRA oa
for elemento in lista_nombres:
    if re.findall('cami[oó]n',elemento):
        print("cami[oó]n : " , elemento)