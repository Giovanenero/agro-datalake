import copy
from datetime import datetime
import os
import re
from bson import ObjectId
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

from models.helper import Helper
from models.database_manager import DatabaseManager

load_dotenv()

class Dap:
    def __init__(self, cpf):
        self.db_name = 'AGRONEGOCIO'
        self.collection_incomes = 'RENDIMENTOS_DAP'
        self.collection_producer = 'PRODUTORES_DAP'
        self.collection_issuer = 'EMISSORES_DAP'
        self.collection_dap = 'DAP'
        self.cpf = cpf
        self.host = os.getenv('HOST')
        self.url = 'https://smap14.mda.gov.br/extratodap/PesquisarDAP/CarregarExtratoDAP'
        self.extrato = 'https://smap14.mda.gov.br/extratodap/PesquisarDAP/ExportarExtrato'

    def get_dap(self, extraction:bool=False, incomes:bool=False, producer:bool=False, issuer:bool=False):
        try:  
            if extraction:
                result = self.extraction()
                if result['data'] == {}: return result
                for doc in result['data']:
                    if not issuer:
                        doc.pop('EMISSOR')
                    
                    if not producer:
                        for field in ('TITULAR1', 'TITULAR2'):
                            doc.pop(field)
                    
                    if not incomes:
                        doc.pop('RENDIMENTOS')
                
                return result

            db_manager = DatabaseManager(self.host, self.db_name, self.collection_producer)

            doc_titualar1 = db_manager.find_one(filter={'CPF': self.cpf}, remove={})
            if doc_titualar1 == {}:
                return {'status': 404, 'message': "CPF não encontrado.", 'data': {}}
            titular1_id = str(doc_titualar1['_id'])

            db_manager.handle_collection(self.collection_dap)
            docs_dap = db_manager.find({'ID_TITULAR_1': titular1_id})
            result = []

            for doc_dap in docs_dap:
                data = {}
                if issuer:
                    db_manager.handle_collection(self.collection_issuer)
                    data['EMISSOR'] = db_manager.find_one({'_id': ObjectId(doc_dap['ID_EMISSOR'])})
                
                if producer:
                    db_manager.handle_collection(self.collection_producer)
                    data['TITULAR_1'] = db_manager.find_one({'_id': ObjectId(doc_dap['ID_TITULAR_1'])})
                    data['TITULAR_2'] = db_manager.find_one({'_id': ObjectId(doc_dap['ID_TITULAR_2'])})
                
                if incomes:
                    db_manager.handle_collection(self.collection_incomes)
                    data['RENDIMENTOS'] = db_manager.find({'CD_DAP': doc_dap['CD_DAP']})

                doc_dap.pop('ID_EMISSOR')
                doc_dap.pop('ID_TITULAR_1')
                doc_dap.pop('ID_TITULAR_2')
                data['DAP'] = doc_dap

                result.append(data)

            return {'data': result, 'message': 'OK', 'status': 200}
        except:
            return {'status': 500, 'message': 'Erro interno no servidor.', 'data': {}}

    def __processing_insert_titular(self, helper:Helper, database_manager:DatabaseManager, doc:dict, date:datetime, first:bool=False):
        if doc:
            doc = {
                'NM_TITULAR': doc.get('Nome', None),
                'CPF': doc.get('Cpf', None),
                'RG': doc.get('RG', None),
                'SEXO': doc.get('Sexo', None),
                'NM_MAE': doc.get('NomeDaMae', None),
                'NATURALIDADE': doc.get('Naturalidade', None),
                'ESTADO_CIVIL': doc.get('EstadoCivil', None),
                'REGIME_CASAMENTO': doc.get('RegimeDeCasamento', None),
                'ESCOLARIDADE': doc.get('Escolaridade', None),
                'NIS': doc.get('NIS', None),
                'NM_BAIRRO': doc.get('Bairro', None),
                'NM_MUNICIPIO': doc.get('NomeMunicipio', None),
                'NR_ENDERECO': doc.get('NumeroDoEndereco', None),
                'NM_CIDADE': doc.get('Cidade', None),
                'CEP': doc.get('CEP', None),
                'ENDERECO': doc.get('Endereco', None),
                'TP_ENDERECO': doc.get('TipoEndereco', None),
                'DT_NASCIMENTO': helper.int_to_datetime(helper.str_to_datetime(doc.get('DataNasc', None))),
                'DT_EMISSAO_IDENTIDADE': helper.int_to_datetime(helper.str_to_datetime(doc.get('DataDeEmissaoDaIdentidade', None))),
                'SG_UF_EMISSOR': doc.get('UFdoEmissor', None),
                'DT_ATUALIZACAO': date
            }
            doc =  helper.clean_empty_spaces(doc)
            database_manager.handle_collection(self.collection_producer)
            doc['CPF'] = self.cpf if first else doc['CPF']
            filter = {'CPF': doc['CPF']}
            if not first:
                filter.update({
                    'NM_MAE': doc['NM_MAE'],
                    'NM_TITULAR': doc['NM_TITULAR']
                })
            
            database_manager.insert_one(doc, True, filter)
            doc_id = doc['_id']
            doc.pop('_id')
            return str(doc_id), doc
        return None, None

    def __processing_insert_emissor(self, helper:Helper, database_manager:DatabaseManager, doc:dict, date:datetime):
        emissor_id = None
        if doc:
            doc = {
                'CNPJ': doc.get('CNPJ', None),
                'CPF_REPRESENTANTE': doc.get('CPFRepresentante', None),
                'LOCAL_EMISSAO': doc.get('LocalEmissao', None),
                'NM_REPRESENTANTE': doc.get('NomeRepresentante', None),
                'RAZAO_SOCIAL': doc.get('RazaoSocial', None),
                'DT_ATUALIZACAO': date
            }
            doc = helper.clean_empty_spaces(doc)
            database_manager.handle_collection(self.collection_issuer)
            filter = {
                'CNPJ': doc['CNPJ'], 
                'CPF_REPRESENTANTE': doc['CPF_REPRESENTANTE'],
                'NM_REPRESENTANTE': doc['NM_REPRESENTANTE']
            }
            database_manager.insert_one(doc, True, filter)
            emissor_id = doc['_id']
            doc.pop('_id')
            return str(emissor_id), doc

    def __processing_insert_rendas(self, database_manager:DatabaseManager, docs:list, date:datetime, cd_dap:str):
        if docs:
            docs = list(map(lambda x: {
                'NM_PRODUTO': x.get('NomeProduto', None),
                'DS_PRODUTO': x.get('DescMensagem', None),
                'CD_PRODUTO': x.get('cdProduto', None),
                'RENDA_AUFERIDA': x.get('rendaAuferida', None),
                'RENDA_ESTIMADA': x.get('rendaEstimada', None),
                'CD_DAP': cd_dap,
                'DT_ATUALIZACAO': date,
            }, docs))

            if len(docs):
                database_manager.handle_collection(self.collection_incomes)
                database_manager.delete_many({'CD_DAP': cd_dap})
                database_manager.insert_many(copy.deepcopy(docs))
                return docs
        return None

    def __get_controle_externo(self):
        payload = {
            "cpf": self.cpf,
            "numeroDAP": "null",
            "chave": "",
            "TipoConsulta": "Fisica"
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Referer": "https://smap14.mda.gov.br/",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json"
        }

        response = requests.post(self.extrato, json=payload, headers=headers)
        if response.status_code != 200:
            return None
        
        response = response.json()
        soup = BeautifulSoup(response['HTML'], "html.parser")

        controle_externo = soup.find_all('p')[0]
        controle_externo = str(controle_externo.text).split(':')[1].strip()
        return controle_externo

    def extraction(self):
        try:
            params = {"cpf": self.cpf, "numeroControleExterno": ""}
            response = requests.get(self.url, params=params)
            data = response.json()

            dap = data.get('DAP', None)
            if dap is None:
                mensage = data.get('DescMensagem', None)
                if 'CPF não encontrado na base da DAP' in mensage:
                    return {'status': 404, 'message': 'CPF não encontrado na base da DAP', 'data': {}}
                return {'status': 404, 'message': 'Erro na encontrado na base da DAP', 'data': {}}     

            database_manager = DatabaseManager(self.host, self.db_name)
            helper = Helper()
            date = helper.datetime_now()
            
            result = []
            for doc in dap:
                emissor_id, doc_emissor = self.__processing_insert_emissor(helper, database_manager, doc.get('EmissorDAP', None), date)
                titular1_id, doc_titular1 = self.__processing_insert_titular(helper, database_manager, doc.get('Titular1DAP', None), date, True)
                titular2_id, doc_titular2 = self.__processing_insert_titular(helper, database_manager, doc.get('Titular2DAP', None), date)
                
                controle_externo = self.__get_controle_externo()

                doc_dap = {
                    'AREA_ESTABELECIMENTO': doc.get('AreaEstabelecimento', None),
                    'AREA_IMOVEL_PRINCIPAL': doc.get('AreaImovelPrincipal', None),
                    'CD_CAF': doc.get('CDCAF', None),
                    'CD_DAP': doc.get('CDDA', None),
                    'IN_CANCELADO': doc.get('CDDACancelada', None),
                    'IN_SUSPENSA': doc.get('CDDASuspensa', None),
                    'CARACTERIZACAO': [y.get('CaracterizacaoDoBeneficiario') for y in doc.get('CaracterizacaoDAP', [])],
                    'CATEGORIAS': [y.get('DescricaoDAPCategoria') for y in doc.get('CategoriaDAP', [])],
                    'DT_CANCELAMENTO': helper.int_to_datetime(helper.str_to_datetime(doc.get('DataCancelamento', None))),
                    'DT_DESCANCELAMENTO': helper.int_to_datetime(helper.str_to_datetime(doc.get('DataDescancelamento', None))),
                    'DT_FIM_SUSPENSAO': helper.int_to_datetime(helper.str_to_datetime(doc.get('DataFimSuspensao', None))),
                    'DENOMINACAO_IMOVEL_RURAL': doc.get('DenominacaoImovelPrincipal', None),
                    'LOCALIZACAO_IMOVEL_PRINCIPAL': doc.get('LocalizacaoImovelPrincipal', None),
                    'MOTIVO_CANCELAMENTO': doc.get('MotivoCancelamento', None),
                    'NM_MUNICIPIO_ESTABELECIMENTO': doc.get('MunicipioEstabelecimento', None),
                    'NM_MUNICIPIO': doc.get('MunicipioUF', "/").split('/')[0],
                    'SG_UF': doc.get('MunicipioUF', "/").split('/')[1],
                    'NR_EMPREGADOS_PERMANENTE': doc.get('NEmpregadosPermanente', None),
                    'NR_EMPREGADOS_EdocPLORADOS': doc.get('NImoveisExplorados', None),
                    'NR_MEMBROS_FAMILIA': doc.get('NMembrosFamilia', None),
                    'NM_PROPRIETARIO_IMOVEL_PRINCIPAL': doc.get('NomeDoProprietarioDoImovelPrincipal', None),
                    'CPF_CNPJ_PROPRIETARIO_PRINCIPAL': doc.get('CPFCNPJDoProprietarioPrincipal', None),
                    'OBS_DAP': doc.get('ObsDAP', None),
                    'OBS_CANCELAMENTO': doc.get('ObservacaoCancelamento', None),
                    'OBS_DESCANCELAMENTO': doc.get('ObsDescancelamento', None),
                    'ORGANIZACAO': [y.get('OrganizacaoSocial') for y in doc.get('OrganizacaoDAP', [])],
                    'RENDA_AGROINDUSTRIA_TURISMO': doc.get('RendaAgroIndustriaTurismo', None),
                    'RENDA_ESTABELECIMENTO': doc.get('RendaEstabelecimento', None),
                    'RENDA_FORA_ESTABELECIMENTO': doc.get('RendaForaEstabelecimento', None),
                    'RENDA_PREVIDENCIARIA': doc.get('RendaPrevidenciaria', None),
                    'RESPONSAVEL_CANCELAMENTO': doc.get('ResponsavelCancelamento', None),
                    'STATUS': doc.get('Status', None),
                    'STATUS_JOVEM_MULHER': doc.get('StatusJovemMulher', None),
                    'TIPO': doc.get('TipoDAP', None),
                    'TP_IMOVEL': doc.get('TipoImovel', None),
                    'USO_TERRA': [y.get('UsoDaTerra') for y in doc.get('UsoDaTerraDAP', [])],
                    'DT_EMISSAO': helper.int_to_datetime(helper.str_to_datetime(doc.get('dataEmissao', None))),
                    'ENQUADRAMENTO': doc.get('enquadramento', None),
                    'NR_CONTROLE_EXTERNO': controle_externo,
                    'DT_VALIDADE': helper.int_to_datetime(helper.str_to_datetime(doc.get('validade', None))),
                    'ID_EMISSOR': str(emissor_id),
                    'ID_TITULAR_1': str(titular1_id),
                    'ID_TITULAR_2': str(titular2_id),
                    'DT_ATUALIZACAO': date
                }
                doc_dap = helper.clean_empty_spaces(doc_dap)
                database_manager.handle_collection(self.collection_dap)
                database_manager.insert_one(doc_dap.copy(), True, {'CD_DAP': doc_dap.get('CD_DAP', None)})

                docs_incomes = self.__processing_insert_rendas(database_manager, doc.get('DAPRendas', []), date, doc_dap.get('CD_DAP', None))

                doc_dap.pop('ID_EMISSOR')
                doc_dap.pop('ID_TITULAR_1')
                doc_dap.pop('ID_TITULAR_2')

                result.append({
                    'EMISSOR': doc_emissor,
                    'TITULAR1': doc_titular1,
                    'TITULAR2': doc_titular2,
                    'RENDIMENTOS': docs_incomes,
                    'DAP': doc_dap
                })

            return {'status': 200, 'message': 'OK', 'data': result}
        except:
            return {'status': 500, 'message': 'Erro ao obter os dados da fonte', 'data': {}}
    