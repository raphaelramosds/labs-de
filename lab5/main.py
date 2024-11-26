from pymongo import MongoClient

mongo_user = 'root'
mongo_pass = 'root'
mongo_port = 27017
mongo_host = 'localhost'
mongo_db = 'ingressantes'
mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}"
# NOTE mongosh -u mongo_user -p mongo_host

client = MongoClient(mongo_uri)

db = client[mongo_db]

alunos = db.alunos
aluno_id = alunos.insert_one({ "name":"Raphael Ramos", "age":19 }).inserted_id
print(aluno_id)