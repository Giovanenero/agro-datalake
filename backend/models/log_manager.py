import logging
from flask import request

logging.getLogger('werkzeug').disabled = True

logging.basicConfig(
    filename='./backend.log',
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

class LogManager:
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name

    def __get_message(self, message: str, status: int) -> str:
        """Gera a mensagem de log formatada, incluindo informações do cliente."""
        client_ip = request.remote_addr
        user_agent = request.user_agent.string
        http_method = request.method
        url = request.url
        
        return (
            f"[{self.endpoint_name}] STATUS: {status} | MESSAGE: {message} | "
            f"IP: {client_ip} | METHOD: {http_method} | URL: {url} | USER-AGENT: {user_agent}"
        )

    def create_log(self, message: str, status: int, insert: bool = True):
        """
        Cria um log e salva em um arquivo.

        Parâmetros:
        - message | str: Mensagem do log;
        - status | int: Código de status HTTP;
        - insert | bool: Se for `False` e o status for 200, o log não será criado.
        """
        if not insert and status == 200:
            return

        log_message = self.__get_message(message, status)

        if status in {200, 404}:
            logging.info(msg=log_message)
        elif status == 400:
            logging.error(msg=log_message)
        elif status == 500:
            logging.critical(msg=log_message)
        else:
            logging.warning(msg=log_message)  # Para outros status, registra como WARNING