# Lab 02 - MapReduce com Hadoop - Tarefa 2
# Componentes
#   Raphael Ramos
#   Emanoel Batista

import sys
import csv
import re

ENCODING = 'ISO-8859-1'
DELIMITER = ';'

for filename in sys.stdin:
    # TODO Mapper vai retornar no stdout (Key: Nome da cidade) (Value: Temperatura)
    # TODO Reducer recebe no stdin e retorna no  stdout Key MÉDIA(Value)
    filename = re.sub('\n', '', filename)
    with open(f'samples/{filename}', newline='', encoding=ENCODING) as file:

        # Abrir leitor do CSV
        reader = csv.reader(file, delimiter=DELIMITER)
        state = None
        header = None

        # Recuperar nome da cidade e pular o cabeçalho
        for row in reader:
            header = row
            state = row[1] if row[0] == 'ESTACAO:' else state
            if len(row) != 2:
                break

        # Recuperar numero da coluna da temperatura desejada
        temp_index = header.index('TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)')

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
            print(f'{state} {temp}')
        