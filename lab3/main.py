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

spark = SparkSession \
    .builder \
    .appName("Lab 3") \
    .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile('conjunto2.csv')

columns = lines.map(lambda l : l.split(';'))

people = columns.map(lambda p: Row(name = p[0], job = p[1])).distinct()

# Imprimir cinco primeiros resultados
print("Sem repeticao")
print(f'{people.count()} funcionarios')
