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
from pymongo import MongoClient
from pyspark.sql import SparkSession

# Definir JAVA_HOME para o pyspark funcionar
import os
os.environ['JAVA_HOME'] = '/usr/java/jre1.8.0_431'

# Configuração do MongoDB: na CLI, acesse com mongosh -u mongo_user -p mongo_host
mongo_user = 'root'
mongo_pass = 'root'
mongo_port = 27017
mongo_host = 'localhost'
mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}"
mongo_db = 'sisu_ufrn'

# Criar DB e collection para os estudantes ingressantes
client = MongoClient(mongo_uri)
db = client[mongo_db]
students = db.students

# Iniciar sessão do spark
spark = SparkSession.builder.appName("Spark").getOrCreate()
sc = spark.sparkContext

# Recuperar todas as tags do conjunto de dados
tags = sc.textFile('discentes-2024.csv')

# Ignorar cabeçalho e tranformar linhas do CSV em listas
str_header = tags.first()
rdd_header = sc.parallelize([str_header])
lines = tags.subtract(rdd_header).map(lambda l : l.split(';'))

# Salvar colunas do cabeçalho
columns = str_header.split(';')

# Construa as linhas como objetos cujas chaves são as colunas
rows = [dict(zip(columns, line)) for line in lines.collect()]

# Abrir transaction para inserir cada uma das linhas

# ingressante_id = ingressantes.insert_one({ "name":"Raphael Ramos", "age":19 }).inserted_id

