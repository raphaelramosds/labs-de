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
from pymongo.errors import ConnectionFailure, OperationFailure

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

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

# Colocar restrição de unicidade na coluna matricula
students.create_index('matricula', unique=True)

# Iniciar sessão do spark
spark = SparkSession.builder.appName("Lab 5 - MongoDB com Spark").getOrCreate()
sc = spark.sparkContext

# Recuperar todas as linhas do conjunto de dados
tags = sc.textFile('discentes-2024.csv')

# Ler CSV com delimitador ; (ponto e vírgula) e quote " (aspas duplas)
lines = tags.map(lambda line: line.replace('"', '').split(';'))

# Recuperar colunas
columns = lines.first()

# Remover cabeçalho
lines = lines.filter(lambda line: line != columns)

# Construa as linhas como objetos cujas chaves são as colunas
rows = [dict(zip(columns, line)) for line in lines.collect()]

# Abrir transaction para inserir todas as linhas no MongoDB
with client.start_session() as session:
    try:
        session.start_transaction()
        students.insert_many(rows)
        session.commit_transaction()
        print(f"{len(rows)} registros inseridos com sucesso.")
    # Caso tente inserir linha repetida, aborte a inserção
    except (ConnectionFailure, OperationFailure) as e:
        session.abort_transaction()

# Recuperar registros do banco ignorando a coluna _id pois o spark dataframe não consegue inferir seu tipo diretamente
data = [ { k : v for k, v in doc.items() if k != "_id" } for doc in students.find() ]

# Colocar registros em um Dataframe
df = spark.createDataFrame(data)

# Consultas aos dados
print("Listagem de todos os alunos que ingressaram por meio do SiSU")
df.filter(df.forma_ingresso == "SiSU").show()

print("Frequência de alunos do sexo masculino (M) e do sexo feminino (F)")
df.groupBy("sexo").count().show()

print("Top 5 dos cursos que mais receberam alunos")
df = df.filter(df.nome_curso != '')
df.groupBy("nome_curso").count().orderBy(desc("count")).limit(5).show()

print("Quantidade de alunos do sexo feminino que ingressaram via SISU em PEDAGOGIA")
print(df.filter((df.sexo == 'F') & (df.forma_ingresso == 'SiSU') & (df.nome_curso == 'PEDAGOGIA')).count())

# Encerrar sessão spark
spark.stop()
