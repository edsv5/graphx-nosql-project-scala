# Correcciones importantes del dataset:
##  * Quitar caracter de bolita de arriba y reemplazarlo con o (Esto se hace con ctrl-f)
##  * Si una linea tira un arreglo de longitud 9, es porque tiene dos comas de mas, borrar las comas
##    en estos espacios
##  * Primero se usa este script
##

import re

fileName = "testDataset"
outputFileName = fileName + "_noExtraComas.csv"

input_file = open(fileName + ".csv", "r")
output_file = open(outputFileName, "w")

# Encontrar palabras separadas por espacios o numeros con un punto en medio
regex = r'[\d+.]+\n?|[\w(\s|\-)]+'
# Cada linea debe dar un arreglo de longitud 7 (7 atributos), si tiene comas de sobra, se borran

for line in input_file:
    
    match_br = re.match(regex, line)
    words = re.findall(regex, line)
    print("Linea: ")
    print(line)
    
    if match_br:
    #br = match_br.group(1)
    #print(br)   
        print(words) 
        print(len(words))
        if(len(words) == 7): # Si tiene la longitud adecuada, no sobran comas, escribir como esta separado por comas
            output_file.write(line)
        if(len(words) == 9):
            print("Sobran 2 comas")
            # Se construye la nueva tupla sin comas de sobra
            # words[0] = OBJECTID_1
            # words[1] = OBJECTID
            # words[2] = RL
            # words[3] = TIPO
            ##### UNIR ESTO #####
            # words[4] = NRUTA(1)
            # words[5] = NRUTA(2)
            # words[6] = NRUTA(3)
            ##### UNIR ESTO #####
            # words[7] = RL --- Por alguna razon esto se repite
            # words[8] = SHAPE_Leng

            newLine = words[0] + "," + words[1] + "," + words[2] + "," + words[3] + "," + words[4] + ";" + words[5] + ";" + words[6] + "," + words[7] + "," + words[8] 
            output_file.write(newLine)



#elif match_ot:
#    ot = match_ot.group(2)    
    # do your replacement

#else:
#    output_file.write(line)

