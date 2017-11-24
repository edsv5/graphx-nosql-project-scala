# Correcciones importantes del dataset:
##  * TODO: Remueve las tuplas que no tienen atributo NRUTA
##

import re

fileName = "testDataset_noExtraComas"
outputFileName = fileName + "_noEmptyRegs.csv"

input_file = open(fileName + ".csv", "r")
output_file = open(outputFileName, "w")

# Encontrar palabras separadas por espacios o numeros con un punto en medio
regex = r'[\d+.]+\n?|[\w(\s|\-)]+'

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
        if(words[4] != ' '): # Si la tupla tiene atributo NRUTA, escribe la tupla en el nuevo archivo, si no, la ignora
            output_file.write(line)
        #else:
        #    output_file.write("--- Empty reg deleted ---\n")