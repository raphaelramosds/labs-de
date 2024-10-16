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
# SCRIPT: REDUCER (python3)

#!/usr/bin/env python
import re
import sys
from collections import defaultdict

# Dicionario para armazenar temperaturas por cidade
city_temp = defaultdict(list)

# Dicionario para armazenar média de temperatura por cidade
city_avg = defaultdict(float)

# Lê cada linha da entrada stdin
for line in sys.stdin:
    # Dividir linha em dois subgrupos: nome da cidade e temperatura
    match = re.match(r'^(.*)(\s+)([\d.]+)$', line.strip())
    city = match.group(1).strip()
    temp = match.group(3)

    # Adicionar temperatura desta cidade
    city_temp[city].append(float(temp))

# Calcular e imprimir a média para cada cidade
for city, temps in city_temp.items():
    # Calculo da media
    avg = sum(temps) / len(temps)

    # Adicionar media no objeto
    city_avg[city] = avg

# Ordenar pela maior media de  temperatura
sorted_city_avg = sorted(city_avg.items(), key=lambda x: x[1], reverse=True)

# Imprime os resultados (cidade e média de temperatura)
for city, avg in sorted_city_avg:
    print(f"{city} {avg}")
