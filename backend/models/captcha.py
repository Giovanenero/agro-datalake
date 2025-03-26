import os
import time
from dotenv import load_dotenv
from twocaptcha import TwoCaptcha
import undetected_chromedriver

load_dotenv()

class Captcha:
    def __init__(self):
        self.api_key = os.getenv('CAPTCHA_API_KEY')

    def solve_hcaptcha(self, sitekey:str, url:str, timeout:int=30):
        """ Envia o hCaptcha para o 2Captcha e aguarda a resposta """
        solver = TwoCaptcha(self.api_key)
        return solver.hcaptcha(sitekey=sitekey, url=url)
    
    def solve_recaptcha(self, sitekey:str, url:str, timeout:int=30):
        """ Envia o hCaptcha para o 2Captcha e aguarda a resposta """
        solver = TwoCaptcha(self.api_key)
        return solver.recaptcha(sitekey=sitekey, url=url)
    
    def get_response(self, driver:undetected_chromedriver, timeout:int=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                user_agent = driver.execute_script("return navigator.userAgent")
                res = driver.execute_script("return document.getElementById('g-recaptcha-response').value")
                data = {
                    'code': res,
                    'user-agent': user_agent
                }
                if res != '': return {'status': True, 'data': data}
                else: time.sleep(3)
            except:
                time.sleep(3)
                continue
        return {'status': False, 'data': {}}