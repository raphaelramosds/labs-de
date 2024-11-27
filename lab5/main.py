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
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
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

# Colocar restrição de unicidade na coluna matricula
students.create_index('matricula', unique=True)

# Iniciar sessão do spark
spark = SparkSession.builder.appName("Spark").getOrCreate()
sc = spark.sparkContext

# Recuperar todas as linhas do conjunto de dados
tags = sc.textFile('discentes-2024.csv')

# Ler CSV com delimitador ; (ponto e vírgula) e quote " (aspas duplas)
lines = tags.map(lambda line: line.replace('"', '').split(';'))

# Recuperar colunas
columns = lines.first()

# Ignorar cabeçalho e tranformar linhas do CSV em listas
lines = lines.filter(lambda line: line != columns)

# Construa as linhas como objetos cujas chaves são as colunas
rows = [dict(zip(columns, line)) for line in lines.collect()]

# Abrir transaction para inserir todas as linhas
with client.start_session() as session:
    try:
        session.start_transaction()
        students.insert_many(rows)
        session.commit_transaction()
        print(f"{len(rows)} registros inseridos com sucesso.")
    # Caso tente inserir linha repetida, aborte a inserção
    except (ConnectionFailure, OperationFailure) as e:
        session.abort_transaction()
        print(f"Algo deu errado na inserção do registro: {e}")

# Listar todos os alunos que ingressaram por meio do SiSU

# Computar quantos alunos são do sexo masculino e do sexo feminino

# Computar o top 5 dos cursos que mais receberam alunos

# Realizar consulta múltipla ("relacional"), por exemplo: quantos alunos são do sexo masculino que ingressaram via SiSU em algum curso em específico