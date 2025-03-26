import os
import requests
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

from models.database_manager import DatabaseManager
from models.helper import Helper
from models.captcha import Captcha

class Sintegra:
    def __init__(self, cpf_cnpj):
        self.cpf_cnpj = cpf_cnpj
        self.host = os.getenv('HOST')
        self.db = 'AGRONEGOCIO'
        self.collection = 'CONTRIBUINTES_SINTEGRA'
        self.url = 'https://www4.sefaz.pb.gov.br/sintegra/SINf_ConsultaSintegra.jsp'
        self.api = 'https://www4.sefaz.pb.gov.br/sintegra/SINf_ConsultaSintegra'
        self.sitekey = '6LersSMUAAAAABHeCOewK5bV2AC867NloqE-eBNE'
        self.sitekey = '6LersSMUAAAAABHeCOewK5bV2AC867NloqE-eBNE'

    def get_sintegra(self, extraction:bool=False):
        try:
            if extraction: 
                result = self.extraction()
                if result['status'] != 200: return result
                if result['data'] == {}: return {'status': 404, 'message': 'Documento não encontrado', 'data': {}}
                return result
            
            db_manager = DatabaseManager(self.host, self.db, self.collection)
            doc = db_manager.find_one({'CPF_CNPJ': self.cpf_cnpj})

            if doc == {}:
                return {'status': 404, 'message': 'CPF ou CNPJ não encontrado', 'data': {}} 
            return {'status': 200, 'message': 'OK', 'data': doc}
        except:
            return {'status': 200, 'message': 'Erro interno no servidor.',  'data': {}}

    def extraction(self):
        options = Options()
        options.add_argument(f'--load-extension={os.path.abspath("hekt2")}')
        options.add_argument("--headless")
        driver = uc.Chrome(options=options)
        driver.get(self.url)

        # Primeira, ele tenta manualmente, se não der certo ele usar 2captcha para
        # resolver o recaptcha

        captcha = Captcha()
        result = captcha.get_response(driver, 10)
        if not result['status']:
            result = captcha.solve_recaptcha(self.sitekey, self.url)
            result = {'data': {'code': result['code']}}

        driver.quit()

        payload = {
            "tipoDoc": "CPF" if len(self.cpf_cnpj) == 11 else 'CNPJ',
            "tpDocumento": "3" if len(self.cpf_cnpj) == 11 else "2",
            "nrDocumento": self.cpf_cnpj,
            "g-recaptcha-response": result['data']['code']
        }

        response = requests.post(self.api, data=payload)
        if response.status_code != 200:
            if response.status_code == 404 and not ('was not found on this server' in str(response.text).lower()):
                return {'status': 404, 'message': 'Contribuinte não encontrado.', 'data': {}}
            return {'status': 400, 'message': 'Erro na fonte dos dados.', 'data': {}}
        
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")
        td_tags = table.find_all("td") 
        helper = Helper()

        doc = {
            'CPF_CNPJ': self.cpf_cnpj,
            'INSCRICAO_ESTADUAL': helper.str_to_int(td_tags[3].text),
            'RAZAO_SOCIAL': td_tags[5].text.strip(),
            'LOGRADOURO': td_tags[7].text.strip(),
            'NR_ENDERECO': td_tags[9].text.strip(),
            'COMPLEMENTO_ENDERECO': td_tags[11].text.strip(),
            'NM_BAIRRO': td_tags[13].text.strip(),
            'NM_MUNICIPIO': td_tags[15].text.strip(),
            'SG_UF': td_tags[17].text.strip(),
            'CEP': td_tags[19].text.strip(),
            'NR_TELEFONE': td_tags[21].text.strip(),
            'ATIVIDADE_ECONOMICA': td_tags[23].text.strip(),
            'REGIME_PAGAMENTO': td_tags[25].text.strip(),
            'ST_CADASTRO': td_tags[27].text.strip(),
            'DT_ULTIMA_ATUALIZACAO': helper.str_to_datetime(td_tags[29].text.strip()),
            'DT_ATUALIZACAO': helper.datetime_now()
        }

        db_manager = DatabaseManager(self.host, self.db, self.collection)
        db_manager.insert_one(doc.copy(), True, {'CPF_CNPJ': doc['CPF_CNPJ']})
        return {'status': 200, 'message': 'OK', 'data': doc}