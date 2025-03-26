from datetime import datetime, timezone
import re


class Helper:
    def str_to_datetime(self, date:str):
        """
            input: %d/%m/%Y (str)
            output: %Y-%m-%dT00:00:00.000+00:00 (datetime)
        """
        try:
            return datetime.strptime(date, "%d/%m/%Y")
        except:
            return None
        
    def datetime_now(self):
        return datetime.now(timezone.utc)
    
    def str_to_int(self, value:str):
        try:
            value = value.strip()
            return int(re.sub(r'\D', '', value))
        except: return None

    def int_to_datetime(self, date:int):
        try:
            return datetime.fromtimestamp(date / 1000)
        except:
            return None
        
    def clean_empty_spaces(self, data:any):
        if isinstance(data, dict):
            return {k: self.clean_empty_spaces(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.clean_empty_spaces(i) for i in data]
        elif isinstance(data, str):
            return data.strip()
        else:
            return data
        
    def check_cpf(self, cpf:str):
        if not cpf:
            return {'status': 404, 'message': 'CPF não informado.', 'data': {}}
        
        cpf = re.sub(r'\D', '', cpf)
        if len(cpf) != 11:
            return {'status': 404, 'message': 'CPF invalido.', 'data': {}}

        return {'status': 200, 'message': 'OK', 'data': {'CPF': cpf}}
    
    def check_cnpj(self, cnpj:str):
        if not cnpj:
            return {'status': 404, 'message': 'CNPJ não informado.', 'data': {}}
        
        cnpj = re.sub(r'\D', '', cnpj)
        if len(cnpj) != 14:
            return {'status': 404, 'message': 'CNPJ invalido.', 'data': {}}

        return {'status': 200, 'message': 'OK', 'data': {'CNPJ': cnpj}}
    
    def check_cpf_cnpj(self, cpf_cnpj):
        if not cpf_cnpj:
            return {'status': 404, 'message': 'CPF ou CNPJ não informado.', 'data': {}}
        
        cpf_cnpj = re.sub(r'\D', '', cpf_cnpj)
        if len(cpf_cnpj) == 11 or len(cpf_cnpj) == 14:
            return {'status': 200, 'message': 'OK', 'data': {'CPF_CNPJ': cpf_cnpj}}
        return {'status': 404, 'message': 'CPF ou CNPJ invalido.', 'data': {}}

