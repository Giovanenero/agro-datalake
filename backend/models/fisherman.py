import copy
import os
import re
from pymongo import MongoClient, errors
from dotenv import load_dotenv
import requests
from models.database_manager import DatabaseManager
from models.captcha import Captcha
from models.helper import Helper

load_dotenv()

class Fisherman:
    def __init__(self, cpf:str):
        self.db_name = 'AGRONEGOCIO'
        self.collection_pescador = 'PESCADORES'
        self.collection_request = 'SOLICITACOES'
        self.collection_area = 'AREAS_PESCA'
        self.cpf = re.sub(r'\D', '', cpf)
        self.host = os.getenv('HOST')
        self.url = 'https://pesqbrasil-pescadorprofissional.agro.gov.br/consulta'
        self.api = 'https://pesqbrasil-pescadorprofissional.agro.gov.br/api/carteira-profissional/consulta-publica'
        self.sitekey = '9a7cd4e0-f01a-49b4-a162-6fda760845f0'

    def get_fisherman(self, extraction:bool=False, area:bool=True, request:bool=True):
        """
            Retorna todas as informações do pescador com base no CPF fornecido.

            Parâmetros:
            - extraction (bool): Um indicador se deve obter os dados atualizados.
            - area: (bool): Um indicador se deve retornar os dados referente a area de pesca.
            - request: (bool): Um indicador se deve retornar os dados referente as solicitações

            Retorno:
            - dict: Um dicionário contendo os dados do pescador, caso encontrado.
        """
        try:
            if extraction: 
                doc = self.extraction()
                if not area: doc['data'].pop('AREAS', None)
                if not request: doc['data'].pop('SOLICITACAO', None)
                return doc

            result = {
                'status': 200,
                'message': 'OK',
                'data': {}
            }

            database_manager = DatabaseManager(self.host, self.db_name, self.collection_pescador)
            doc_pescador = database_manager.find_one({'CPF': self.cpf})

            if doc_pescador == {}:
                return {'status': 404, 'message': 'CPF não encontrado.', 'data': {}}

            result['data']['PESCADOR'] = doc_pescador

            if request:
                database_manager.handle_collection(self.collection_request)
                doc_solicitacao = database_manager.find_one({'CPF': self.cpf})
                result['data']['SOLICITACAO'] = doc_solicitacao
            
            if area:
                database_manager.handle_collection(self.collection_area)
                docs_areas = database_manager.find({'CPF': self.cpf})
                result['data']['AREAS'] = docs_areas
            
            return result
        except Exception as e:
            raise Exception(e)

    def __processing_insert(self, data:dict):
        """
            Método que processa e insere os dados no banco de dados e retorna todas as 
            informações do pescador com base no CPF fornecido.

            Parâmetros:
            - data: Um dicionário contendo os dados brutos de pescador

            Retorno:
            - dict: Um dicionário contendo os dados do pescador, caso encontrado.
        """
        try:
            database_manager = DatabaseManager(self.host, self.db_name, self.collection_pescador)
            pescador = dict(data.get('pescador', {}))
            renda = data.get('possuiFonteRenda', None)
            helper = Helper()
            date = helper.datetime_now()

            doc_pescador = {
                'CPF': self.cpf,
                'URL_FOTO': data.get('urlFoto', None),
                'NM_PESCADOR': pescador.get('usuario', {}).get('nome', None),
                'DT_NASCIMENTO': helper.str_to_datetime(pescador.get('dataNascimento', None)),
                'CATEGORIA': pescador.get('categoria', {}).get('nomeCategoria', None),
                'SITUACAO': pescador.get('situacao', None),
                'NM_MUNICIPIO': pescador.get('endereco', {}).get('municipio', None),
                'SG_UF': pescador.get('endereco', {}).get('uf', None),
                'NM_RGP': pescador.get('rgp', {}).get('numeroRGP', None),
                'DT_RGP': helper.str_to_datetime(data.get('dataPrimeiroRGP', None)),
                'IN_FONTE_RENDA': renda.lower() == 'sim' if isinstance(renda, str) else None,
                'DT_ATUALIZACAO': date
            }

            database_manager.insert_one(doc_pescador.copy(), True, {"CPF": self.cpf})
            database_manager.handle_collection(self.collection_area)

            docs_area = list(map(lambda x: {
                'CPF': self.cpf,
                'ID_AREA': x['id'],
                'TP_LOCAL': x['tipoLocalPesca'],
                'NM_LOCAL': x['nomePopularLocalPesca'],
                'NM_MUNICIPIO': x['municipio'],
                'SG_UF': x['uf'],
                'DT_ATUALIZACAO': date
            }, data['areaPretendidaPesca']))

            database_manager.insert_many(copy.deepcopy(docs_area), True, {'CPF': self.cpf})
            database_manager.handle_collection(self.collection_request)

            doc_solicitacao = {
                'CPF': self.cpf,
                'DT_SOLICITACAO': helper.str_to_datetime(data.get('dataSolicitacao', None)),
                'TP_SOLICITACAO': data.get('tipoSolicitacao', None),
                'TP_ATUACAO': data.get('formaAtuacao', None),
                'GP_ALVO': data.get('grupoAlvo', None),
                'ST_SOLICITACAO': data.get('situacaoSolicitacao', None),
                'DT_ATUALIZACAO': date
            }

            database_manager.insert_one(doc_solicitacao.copy(), True, {"CPF": self.cpf})
            database_manager.close()

            return {
                'PESCADOR': doc_pescador,
                'AREAS': docs_area,
                'SOLICITACAO': doc_solicitacao
            }
        except Exception as e:
            raise Exception(e)

    def extraction(self):
        """
            Faz a extração de todas as informações do pescador com base no CPF fornecido.

            Retorno:
            - dict: Um dicionário contendo os dados do pescador, area e solicitacao, caso encontrado.
        """
        result = Captcha().solve_hcaptcha(self.sitekey, self.url)
        if result is None: return {'status': 400, 'message': 'Erro ao acessar os dados', 'data': {}}
        token = result['code']
        params = {"cpf": self.cpf, "rgp": ""}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "x-hcaptcha-token": token,
            "Accept": "application/json"
        }

        response = requests.get(self.api, params=params, headers=headers)
        result = response.json()
        status = result['codigo']

        if status != 200: return {'status': 400, 'message': result['mensagem'], 'data': {}}
        
        try:
            data = self.__processing_insert(result['dados'])
            return {'status': 200, 'message': 'OK', 'data': data}
        except:
            return {'status': 500, 'message': 'Erro interno no servidor.', 'data': {}}
