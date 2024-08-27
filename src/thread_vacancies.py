from threading import Thread

import requests


class VacDataWorker(Thread):
    __slots = ['base_connect', 'emp_id']

    def __init__(self, emp_id, base_connect):
        super().__init__()
        self.base_connect = base_connect
        self.emp_id = emp_id

    def run(self):
        base_url = 'https://api.hh.ru/vacancies'
        params = {'per_page': 100, 'page': 0, 'only_with_salary': True, 'employer_id': self.emp_id}

        while True:
            response = requests.get(base_url, params=params)
            result = response.json()

            if not result.get('items'):
                break
            else:
                for vac in result['items']:
                    vac_id = vac.get('id')
                    vac_salary = vac['salary'].get('from') or vac['salary'].get('to')
                    vac_name = vac.get('name')
                    vac_descr = vac.get('snippet', {}).get('responsibility')
                    vac_url = vac.get('alternate_url')
                    cur = self.base_connect.cursor()

                    cur.execute(
                        f"""
                                    MERGE INTO vacancies USING
                                        (VALUES({vac_id})) as src(id)
                                    ON vacancies.id = src.id
                                    WHEN NOT MATCHED THEN
                                        INSERT VALUES(
                                        {vac_id}, '{vac_name}', {self.emp_id}, {vac_salary}, '{vac_descr}', '{vac_url}');
                                    """
                    )

            params['page'] += 1
