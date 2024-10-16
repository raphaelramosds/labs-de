# Lab 02 - MapReduce com Hadoop - Tarefa 2
# Componentes
#   Raphael Ramos
#   Emanoel Batista

import sys
from collections import defaultdict

# Dicionário para armazenar temperaturas por cidade
state_temp = defaultdict(list)

# Lê cada linha da entrada stdin
for line in sys.stdin:
    # TODO Regex para separar em dois grupos: nome da cidade e temperatura
    state = None
    temp = None

    # Calcular média
    state_temp[state].append(float(temp))

# Ordenar pela maior temperatura
# sorted_state_temp = sorted(state_temp.items(), key=lambda x: x[1], reverse=True)

# Imprime os resultados (IP e contagem)
# for state, avg in sorted_state_temp:
    # print(f"{state} {avg}")