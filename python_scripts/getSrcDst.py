
## Script para tokenizar el origen y destino de los nodos, cambia el atributo NRUTA por un atributo SrcNode y DstNode

import re

#file1 = "Carreteras_CR_filtrado_comas.csv"
fileName = "testDataset_noExtraComas_noEmptyRegs"
outputFileName = fileName + "_SrcDstAdded.csv"

input_file = open(fileName + ".csv", "r")
output_file = open(outputFileName, "w")

# Encontrar palabras separadas por espacios o ; o numeros con un punto en medio
regex = r'[\d+.]+\n?|[\w(\s|\;)]+'

for line in input_file:
    #match_br = re.match(r'\s*#define .*_BR (0x[a-zA-Z_0-9]{8})', line) # should be your regular expression
    #match_ot = re.match(r'\s*#define (.*)_OT (0x[a-zA-Z_0-9]+)', line) # second regular expression
    match_br = re.match(regex, line)
    words = re.findall(regex, line)
    print("Linea: ")
    print(line)
    
    if match_br:
    #br = match_br.group(1)
    #print(br)   
        print(words) 
        print(len(words))

        # Se construye la nueva tupla con nodo origen y destino 
        # words[0] = OBJECTID_1
        # words[1] = OBJECTID
        # words[2] = RL
        # words[3] = TIPO
        # words[4] = NORIGEN
        # words[5] = NDESTINO
        # Se remueve atributo repetido de RL
        # words[7] = SHAPE_Leng

        newLine = words[0] + "," + words[1] + "," + words[2] + "," + words[3] + "," + words[4] + "," + words[5] + "," + words[7]
        output_file.write(newLine)