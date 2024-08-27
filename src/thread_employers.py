from threading import Thread

import requests


class EmpDataWorker(Thread):
    __slots = ['base_connect', 'emp_id']

    employers_url = 'http://api.hh.ru/employers/'

    def __init__(self, emp_id: int, base_connect):
        super().__init__()
        self.base_connect = base_connect
        self.emp_id = str(emp_id)

    def run(self):
        response = requests.get(self.employers_url + self.emp_id)

        result = response.json()
        emp_name = result.get('name', 'Нет данных')
        open_vacs = result.get('open_vacancies', 0)
        cur = self.base_connect.cursor()
        cur.execute(
            f"""
            INSERT INTO employers (id, employer_name, open_vacancies) VALUES
            ({self.emp_id}, '{emp_name}', {open_vacs});
            """
        )
