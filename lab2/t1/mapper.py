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

# Lê cada linha do arquivo de log a partir do stdin
for line in sys.stdin:
    # Divide a linha por espaços em branco e pega o primeiro campo (o IP)
    ip_address = line.split()[0]
    # Imprime o endereço IP seguido por 1, para contar um acesso
    print(f"{ip_address} 1")