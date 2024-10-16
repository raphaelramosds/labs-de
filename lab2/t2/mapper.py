# ██████╗  ██████╗ █████╗
# ██╔══██╗██╔════╝██╔══██╗
# ██║  ██║██║     ███████║
# ██║  ██║██║     ██╔══██║
# ██████╔╝╚██████╗██║  ██║
# ╚═════╝  ╚═════╝╚═╝  ╚═╝ UFRN 2024
#
# RAPHAEL RAMOS
# raphael.ramos.102 '@' ufrn.br
#
# EMANOEL BATISTA
# emanoel.batista.104 '@' ufrn.br
# ----------------------------------------------- 
#
# SCRIPT: MAPPER (python3)

#!/usr/bin/env python
import sys
import csv
import re

# Constantes para leitura do arquivo
ENCODING = 'ISO-8859-1'
DELIMITER = ';'
SAMPLES_DIR = 'samples'
TEMP_COLUMN = 'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)'

# Iterar nas linhas do list directory (ls)
for filename in sys.stdin:
    # Recuperar nome do arquivo csv ignorando caractere de nova linha
    filename = re.sub('\n', '', filename)

    # Abrir stream de leitura do arquivo CSV
    with open(f'{SAMPLES_DIR}/{filename}', newline='', encoding=ENCODING) as file:

        # Abrir leitor do CSV
        reader = csv.reader(file, delimiter=DELIMITER)
        city = None
        header = None

        # Recuperar nome da cidade e pular o cabeçalho
        for row in reader:
            header = row
            city = row[1] if row[0] == 'ESTACAO:' else city
            if len(row) != 2:
                break

        # Recuperar numero da coluna da temperatura desejada
        temp_index = header.index(TEMP_COLUMN)

        # Posicionar ponteiro na linha onde comeca os dados
        next(reader)

        # Itera nas linhas com dados
        for row in reader:
            # Recuperar temperatura
            temp = row[temp_index]

            # Substituir temperaturas invalidas por zero
            temp = temp if re.fullmatch(r'\d+(,\d+)*', temp) else '0.0'

            # Substituir virgula por ponto
            temp = temp.replace(',','.')

            # Imprime no stdout a cidade e a temperatura no timestamp
            print(f'{city} {temp}')
        