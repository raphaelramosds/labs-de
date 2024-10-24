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
from pyspark.sql import SparkSession, Row

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Frequencia de cargos").getOrCreate()

# Obtém o contexto Spark a partir da sessão Spark criada
sc = spark.sparkContext

# Abre CSV e transforma cada linha em uma lista
lines = sc.textFile('conjunto2.csv') \
    .map(lambda l : l.split(';')) \
    .filter(lambda l : l[0] != 'NOME' and not l[0].startswith('-') and len(l[1]) > 0)
    # Filtra as linhas: 
    # 1. Ignora a linha do cabeçalho.
    # 2. Ignora as linhas onde o nome começa com um hífen.
    # 3. Garante que a segunda coluna (ocupação) tenha conteúdo.

# Mapeia as linhas filtradas para um RDD de Rows, contendo o nome da função e o nome da pessoa, e elimina duplicatas
job_name = lines.map(lambda p: Row(job = p[1], name = p[0])).distinct()

# Agrupa os nomes por função e conta quantas pessoas estão associadas a cada função
role_count = job_name.groupByKey().mapValues(len)

# Exibe a frequencia de cada cargo no sdout
for role, count in sorted(role_count.collect()):
    print(role, count)