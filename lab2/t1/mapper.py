# Lab 02 - MapReduce com Hadoop - Tarefa 1
# Componentes
#   Raphael Ramos
#   Emanoel Batista

import sys

# Lê cada linha do arquivo de log a partir do stdin
for line in sys.stdin:
    # Divide a linha por espaços em branco e pega o primeiro campo (o IP)
    ip_address = line.split()[0]
    # Imprime o endereço IP seguido por 1, para contar um acesso
    print(f"{ip_address} 1")