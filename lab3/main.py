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
import re
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Frequencia de cargos").getOrCreate()

# Obtém o contexto Spark a partir da sessão Spark criada
sc = spark.sparkContext

# Abre CSV e transforma cada linha em uma lista
lines = sc.textFile('conjunto2.csv') \
    .map(lambda l : l.split(';')) \
    .filter(lambda l : not re.search(r'\bNOME\b|--+', l[0]) and len(l[1]) > 0)
    # Filtra as linhas: 
    # 1. Ignora as linhas do cabeçalho: primeira coluna contém NOME ou mais de um traço
    # 2. Ignora funcionarios que não tem cargo

# Mapeia as linhas filtradas para tuplas, contendo o nome da função e o nome da pessoa, e elimina duplicatas
job_name = lines.map(lambda p: (p[1], p[0])).distinct()

# Agrupa os nomes por função e conta quantas pessoas estão associadas a cada função
role_count = job_name.groupByKey().mapValues(len)

# DEBUG Exibir total
# print(role_count.values().sum())

# Exibe a frequencia de cada cargo no sdout
for role, count in sorted(role_count.collect()):
    print(role, count)