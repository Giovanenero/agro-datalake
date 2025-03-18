import json
import os
from pymongo import MongoClient

FILE_PATH = "./consultas-cnd-rural.side"
HOST = os.getenv("HOST")
DATABASE_NAME = 'AGRONEGOCIO'
COLLECTION_NAME = 'CAFIR'

client = MongoClient(HOST)
collection = client[DATABASE_NAME][COLLECTION_NAME]

docs = collection.find({'CPF_CNPJ': None, 'NR_IMOVEL': {'$ne': None}}, limit=100)
cibs = [doc['NR_IMOVEL'] for doc in docs]

with open(FILE_PATH, "r", encoding="utf-8") as file:
    result = json.load(file)

array_cibs = ", ".join(f'"{cib}"' for cib in cibs)
result['tests'][0]['commands'][0]['target'] = f'return [{array_cibs}]'

with open(FILE_PATH, "w", encoding="utf-8") as file:
    json.dump(result, file, indent=4, ensure_ascii=False)

print("Arquivo atualizado com sucesso!")