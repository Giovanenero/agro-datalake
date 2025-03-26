import os
from pymongo import MongoClient, errors
from dotenv import load_dotenv

from models.database_manager import DatabaseManager

load_dotenv()

class Cafir:
    def __init__(self, cpf:str):
        self.cpf = cpf
        self.db_name = 'AGRONEGOCIO'
        self.collection_name = 'CAFIR'
        self.host = os.getenv('HOST')

    def get_cafir(self):
        """
            Retorna todas as informações da certidão do imóvel rural com base no CPF fornecido.

            Retorno:
            - dict: Um dicionário contendo os dados do imóvel, caso encontrado.
        """
        try:
            db_manager = DatabaseManager(self.host, self.db_name, self.collection_name)
            doc = db_manager.find_one({'CPF_CNPJ': self.cpf})
            if doc == {}:
                return {'status': 404, 'message': 'CPF não encontrado.', 'data': {}}
            return {'status': 200, 'message': 'OK', 'data': doc}
        except:
            return {'status': 500, 'message': 'Erro interno no servidor.', 'data': {}}