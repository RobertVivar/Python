print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		09.Python.Diccionarios")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
midiccionario={"Alemania":"Berlin","Francia":"Paris","EE.UU":"New York","España":"Madrid"}
print( "midiccionario['Francia'] : ", midiccionario["Francia"])
print( "midiccionario['España'] : ", midiccionario["España"])
print( "midiccionario : ", midiccionario)
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#	AGREGAR 
midiccionario["Italia"]="Lisboa"
print( midiccionario)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#	MODIFICAR 
midiccionario["Italia"]="Roma"
print( midiccionario)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#	ELIMINAR
del midiccionario["EE.UU"]
print( midiccionario)
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
mitupla=["España","Francia","EE.UU","Alemania","Peru"]
midiccionario={mitupla[0]:"Barcelona",mitupla[1]:"Paris",mitupla[2]:"City"}
print( midiccionario)
print( midiccionario["España"])
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
midicc={29:"Robert","Nombre":"Emilio","Distrito":"Los  olivos","anios":[2010,2013,2015,2017]}
print(midicc)
print(midicc["anios"])
#Agregar un diccionario dentro de otro diccionario
midicc={29:"Robert","anios":{"temporadas":[2010,2013,2015,2017]}}
print(midicc["anios"])
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
print(midicc.keys())
print(midicc.values())
print(len(midicc))
print(midicc)
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=