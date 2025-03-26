from flask import Flask, jsonify, request

from models.log_manager import LogManager
from models.sintegra import Sintegra
from models.helper import Helper
from models.dap import Dap
from models.cafir import Cafir
from models.fisherman import Fisherman

app = Flask(__name__)

@app.route('/getcafir', methods=['GET'])
def getcafir():
    log_manager = LogManager('getcafir')
    try:
        cpf = request.args.get('cpf')
        result = Helper().check_cpf(cpf)
        if result['status'] != 200: return jsonify(result), result['status']
        
        data = Cafir(result['data']['CPF']).get_cafir()
        log_manager.create_log(data['message'], data['status'], False)
        return jsonify(data), data['status']
    except Exception as e:
        message = 'Erro interno no servidor'
        log_manager.create_log(message + f': {e}', 500)
        return jsonify({'status': 500, 'message': message, 'data': {}}), 500

@app.route('/getpescador', methods=['GET'])
def getpescador():
    log_manager = LogManager('getpescador')
    try:
        result = Helper().check_cpf(request.args.get('cpf'))
        
        if result['status'] != 200: return jsonify(result), result['status']

        extraction = request.args.get('extraction', None)
        if extraction is not None:
            extraction = str(extraction).lower() == 'true'

        area = request.args.get('area', None)
        if area is not None:
            area = str(area).lower() == 'true'
        else: area = True

        solicitacao = request.args.get('solicitacao', None)
        if solicitacao is not None:
            solicitacao = str(solicitacao).lower() == 'true'
        else: solicitacao = True

        fisherman = Fisherman(result['data']['CPF'])

        if extraction is not None:
            data = fisherman.get_fisherman(extraction, area, solicitacao)
            log_manager.create_log(data['message'], data['status'], False)
            return jsonify(data), data['status']
        
        data = fisherman.get_fisherman(False, area, solicitacao)
        if 'CPF não encontrado.' in data['message']:
            data = fisherman.get_fisherman(True, area, solicitacao)

        log_manager.create_log(data['message'], data['status'], False)
        return jsonify(data), data['status']
    except Exception as e:
        message = 'Erro interno no servidor'
        log_manager.create_log(message + f': {e}', 500)
        return jsonify({'status': 500, 'message': message, 'data': {}}), 500
    
@app.route('/getdap', methods=['GET'])
def getdap():
    log_manager = LogManager('getdap')
    try:
        result = Helper().check_cpf(request.args.get('cpf'))
        if result['status'] != 200: return jsonify(result), result['status']

        extraction = request.args.get('extraction', None)
        if extraction is not None:
            extraction = str(extraction).lower() == 'true'

        incomes = request.args.get('rendimentos', None)
        if incomes is not None:
            incomes = str(incomes).lower() == 'true'
        else: incomes = True

        producer = request.args.get('produtores', None)
        if producer is not None:
            producer = str(producer).lower() == 'true'
        else: producer = True

        issuer = request.args.get('emissor', None)
        if issuer is not None:
            issuer = str(issuer).lower() == 'true'
        else: issuer = True
        
        dap = Dap(result['data']['CPF'])

        # Tenta procurar no banco primeiro
        if extraction is None:
            data = dap.get_dap(False, incomes, producer, issuer)
            if data['status'] != 404:
                log_manager.create_log(data['message'], data['status'], False)
                return jsonify(data), data['status']
     
        data = dap.get_dap(True, incomes, producer, issuer)

        log_manager.create_log(data['message'], data['status'], False)
        return jsonify(data), data['status']
    except Exception as e:
        message = 'Erro interno no servidor'
        log_manager.create_log(message + f': {e}', 500)
        return jsonify({'status': 500, 'message': message, 'data': {}}), 500
    
@app.route('/getsintegra', methods=['GET'])
def getsintegra():
    log_manager = LogManager('getsintegra')
    try:
        cpf_cnpj = request.args.get('cpf_cnpj', None)
        helper = Helper()

        # Verifica os caracteres do CPF ou CNPJ
        result = helper.check_cpf_cnpj(cpf_cnpj)
        if result['status'] != 200: return jsonify(result), result['status']

        cpf_cnpj = result['data']['CPF_CNPJ']
        extraction = request.args.get('extraction', None)
        
        sintegra = Sintegra(cpf_cnpj)
        if extraction is not None:
            extraction = str(extraction).lower() == 'true'
            data = sintegra.get_sintegra(extraction)
            log_manager.create_log(data['message'], data['status'], False)
            return jsonify(data), data['status']
        
        # Tenta procurar o contribuinte no banco
        data = sintegra.get_sintegra(False)

        # Tenta puxar o contribuinte direto na fonte
        if data['message'] in 'Documento não encontrado':
            data = sintegra.get_sintegra(True)
        
        log_manager.create_log(data['message'], data['status'], False)
        return jsonify(data), data['status']

    except Exception as e:
        message = 'Erro interno no servidor'
        log_manager.create_log(message + f': {e}', 500)
        return jsonify({'status': 500, 'message': message, 'data': {}}), 500

@app.route('/getproducer', methods=['GET'])
def getproducer():

    log_manager = LogManager('getproducer')

    try:
        cpf = request.args.get('cpf', None)
        helper = Helper()

        # Verifica os caracteres do CPF
        result = helper.check_cpf(cpf)
        if result['status'] != 200: return jsonify(result), result['status']

        cpf = result['data']['CPF']

        cafir = Cafir(cpf)
        dap = Dap(cpf)
        pescador = Fisherman(cpf)
        sintegra = Sintegra(cpf)

        doc_cafir = cafir.get_cafir()

        extraction = request.args.get('extraction', None)
        if extraction is not None:
            extraction = str(extraction).lower() == 'true'
            doc_sintegra = sintegra.get_sintegra(extraction)

            return {
                'status': 200,
                'message': 'OK',
                'data': {
                    'CAFIR': doc_cafir['data'],
                    'DAP': dap.get_dap(extraction, False, True, False)['data'],
                    'PESCADOR': pescador.get_fisherman(extraction, False, False)['data'],
                    'SINTEGRA': sintegra.get_sintegra(extraction)['data']
                }
            }
        
        doc_dap = dap.get_dap(False, False, True, False)
        if doc_dap['status'] == 404:
            doc_dap = dap.get_dap(True, False, True, False)

        doc_pescador = pescador.get_fisherman(False, False, False)
        if doc_pescador['status'] == 404:
            doc_pescador = pescador.get_fisherman(True, False, False)
        
        doc_sintegra = sintegra.get_sintegra(False)
        if doc_sintegra['status'] == 404:
            doc_sintegra = sintegra.get_sintegra(True)

        return {
            'status': 200,
            'message': 'OK',
            'data': {
                'CAFIR': doc_cafir['data'],
                'DAP': doc_dap['data'],
                'PESCADOR': doc_pescador(extraction, False, False)['data'],
                'SINTEGRA': doc_sintegra(extraction)['data']
            }
        }

    except Exception as e:
        message = 'Erro interno no servidor'
        log_manager.create_log(message + f': {e}', 500)
        return jsonify({'status': 500, 'message': message, 'data': {}}), 500
    
app.run(port=5000, host='0.0.0.0', debug=True)