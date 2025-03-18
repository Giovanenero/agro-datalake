import os
import numpy as np
import pandas as pd
from pymongo import MongoClient

FILE_PATH = os.path.join(os.getcwd(), "extraction/Produtor_Rual_CAFIR_SAS_PB.csv")
COLUMNS = ['NIRF', 'CPF', 'NomePF']

df = pd.read_csv(FILE_PATH, usecols=COLUMNS, dtype={'CPF': str, 'NIRF': str})
HOST = os.getenv("HOST")
DABASE_NAME = 'AGRONEGOCIO'
COLLECTION_NAME = 'CAFIR'

df.replace(np.nan, None, inplace=True) 
df.fillna('None', inplace=True)
df.replace('None', None, inplace=True) 
df.replace('nan', None, inplace=True) 
df.replace('', None, inplace=True)

df = df.dropna(subset=['NIRF'])
cibs = df['NIRF'].tolist()

client = MongoClient(HOST)
collection = client[DABASE_NAME][COLLECTION_NAME]

docs = collection.find({"NR_IMOVEL": {"$in": cibs}})
docs_dict = {doc["NR_IMOVEL"]: doc for doc in docs}

qtd = 0
for _, data in df.iterrows():
    cib = data['NIRF']
    # significa que o cib é referente a uma imóvel de outra UF ou é inválido
    if cib not in docs_dict:
        continue
    

    cpf = data['CPF']
    update_fields = {
        'CPF_CNPJ': '0' + cpf if len(cpf) == 10 else cpf,
        'IN_CPF': True,
        'NM_CONTRIBUINTE': data['NomePF']
    }

    qtd += 1
    collection.update_one(
        {"NR_IMOVEL": cib},
        {"$set": update_fields}
    )

print("{} documentos atualizados".format(qtd))