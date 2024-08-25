import dotenv
import psycopg2
import os
from threading import Thread

import requests

from roots import ENV_DIR


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
        emp_name = result['name']
        open_vacs = result['open_vacancies']
        cur = self.base_connect.cursor()
        cur.execute(
            f"""
            INSERT INTO employers (id, employer_name, open_vacancies) VALUES
            ({self.emp_id}, '{emp_name}', {open_vacs});
            """
        )


class DBManager:

    dotenv.load(ENV_DIR)
    connect_params = {
        'host': os.getenv('host'),
        'user': os.getenv('user'),
        'password': os.getenv('password'),
        'port': os.getenv('port')
    }

    def __init__(self, employers_id: list[int]):
        self.emloyers_id = employers_id
        self.create_tables()
        self.fill_emloyers_table()
        self.fill_vacancies_table()

    def get_companies_and_vacancies_count(self):
        pass

    def get_all_vacancies(self):
        pass

    def get_avg_salary(self):
        pass

    def get_vacancies_with_higher_salary(self):
        pass

    def get_vacancies_with_keyword(self):
        pass

    def connect(self): return psycopg2.connect(**self.connect_params)

    def create_tables(self):
        conn = self.connect()
        cur = conn.cursor()

        cur.execute(
            """
            DROP TABLE IF EXISTS vacancies;

            CREATE TABLE vacancies(
                id int PRIMARY KEY,
                vacancy_name varchar,
                emp_id int,
                salary int,
                description text,
                vacancy_url varchar,
                FOREIGN KEY (emp_id) REFERENCES employers(id)

            );
            """
        )

        cur.execute(
            """
            DROP TABLE IF EXISTS employers CASCADE;

            CREATE TABLE employers(
                id int PRIMARY KEY,
                employer_name varchar NOT NULL,
                open_vacancies int NOT NULL,
                last_update TIMESTAMP default NOW()

            );
            """
        )

        conn.commit()
        conn.close()

    def fill_emloyers_table(self):
        threads_list = []

        conn = self.connect()

        for employer in self.emloyers_id:
            t_worker = EmpDataWorker(employer, conn)
            threads_list.append(t_worker)
            t_worker.start()

        for worker in threads_list:
            worker.join()

        conn.commit()
        conn.close()

    def fill_vacancies_table(self):
        base_url = 'https://api.hh.ru/vacancies'
        params = {
            'per_page': 100,
            'page': 0,
            'only_with_salary': True,
            'employer_id': int
        }

        conn = self.connect()

        for employer in self.emloyers_id:
            params['page'] = 0
            params['employer_id'] = employer
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
                        cur = conn.cursor()

                        cur.execute(
                            f"""
                            MERGE INTO vacancies USING
                                (VALUES({vac_id})) as src(id)
                            ON vacancies.id = src.id
                            WHEN NOT MATCHED THEN
                                INSERT VALUES(
                                {vac_id}, '{vac_name}', {employer}, {vac_salary}, '{vac_descr}', '{vac_url}');
                            """
                        )

                params['page'] += 1
            print(f'Обработан id {employer}')

        conn.commit()
        conn.close()


if __name__ == '__main__':

    employers_ids = [
        15478,
        1740,
        3192913,
        3529,
        80,
        1117026,
        1995278,
        223892,
        4566106,
        49357
    ]

    my_base = DBManager(employers_ids)

