# Correcciones importantes del dataset:
##  Remueve las tuplas repetidas del dataset de Distritos de Costa Rica
##

import re

fileName = "Distritos_de_Costa_Rica"
outputFileName = fileName + "_noDuplicateRegs.csv"

input_file = open(fileName + ".csv", "r")
output_file = open(outputFileName, "w")

# Encontrar palabras separadas por espacios o numeros con un punto en medio
regex = r'[\d+]+\n?|[\w(\s?)]+'
# Se crea un arreglo de distritos para ver si estamos repitiendo distritos
listaDistritos = []
# Se guarda words sub 7, 3, 6, 2, 5, 1, 8 (ORDEN DE DATOS QUE HABRA EN EL NUEVO DATASET)
# NOM_DIST, COD_DIST, NOM_CANT, COD_CANT, NOM_PROV, COD_PROV, ID

for line in input_file:
    match_br = re.match(regex, line)
    words = re.findall(regex, line)

    if match_br:
    #br = match_br.group(1)
    #print(br)
        nombreDistrito = words[7]
        if(nombreDistrito not in listaDistritos): # Si el distrito no esta repetido, inserta en el array y escribe la tupla
            listaDistritos.append(nombreDistrito) # Anade el nombre del distrito
            newLine = words[7] + "," + words[3] + "," + words[6] + "," + words[2] + "," + words[5] + "," + words[1] + "," + words[8]
            output_file.write(newLine) # Escribe la linea
            print("Linea nueva: ")
            print(newLine)
            print(words) 
            print(len(words))
        else:
            print("Distrito repetido")

print("Cantidad de distritos: " + str(len(listaDistritos)))
            #if(words[4] != ' '): # Si la tupla tiene atributo NRUTA, escribe la tupla en el nuevo archivo, si no, la ignora
            #    output_file.write(line)
            #else:
            #    output_file.write("--- Empty reg deleted ---\n")