print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
print("		07.Python.Listas")
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista=["Robert",88, "Peru", 30.88]
print(" miLista[:] : ", miLista[:])
print(" miLista[2] : ", miLista[2])
print(" miLista[2:4] : ", miLista[2:4])
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista2=["Robert","Lima","Peru","Los Olivos"]
print(" miLista2[:] : ", miLista2[:])
print(" miLista2[-2] : ", miLista2[-2])
print(" miLista2[-3] : ", miLista2[-3])
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista.append("Pyhton")	# Para Agregar elementos ( append )
print(" miLista[1:5:] = ", miLista[1:5:])
print(" miLista[2:5:] = ", miLista[2:5:])
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista.insert(3,"INTERBANK")
print(" miLista[1:5:] = ",miLista[1:5])
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
miLista.extend(["a","b","c"])
print(" miLista[:] : ", miLista[:])
print(" miLista.index('b') : ", miLista.index("b"))
print("a" in miLista)
print("--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista.extend(["a","b","c"])	#Sumar
miLista.remove("b")
print(" miLista[:] = ", miLista[:])
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista.pop()
print(" miLista[:] = ", miLista[:])
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
miLista3=miLista+miLista2
print(" miLista3[:] = ",miLista3[:])
#--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=