# Lab 02 - MapReduce com Hadoop - Tarefa 1
# Componentes
#   Raphael Ramos
#   Emanoel Batista

import sys
from collections import defaultdict

# Dicionário para armazenar contagem de acessos por IP
access_count = defaultdict(int)

# Lê cada linha da entrada stdin
for line in sys.stdin:
    # Divide a linha para separar o endereço IP da contagem (1)
    ip_address, count = line.split()
    # Incrementa a contagem de acessos para o IP
    access_count[ip_address] += int(count)

# Ordena os IPs por número de acessos em ordem decrescente
sorted_access_count = sorted(access_count.items(), key=lambda x: x[1], reverse=True)

# Imprime os resultados (IP e contagem)
for ip_address, count in sorted_access_count:
    print(f"{ip_address} {count}")