import dotenv
import psycopg2
import os


from roots import ENV_DIR
from src.employers import EmpDataWorker
from src.vacancies import VacDataWorker


class DBManager:

    dotenv.load(ENV_DIR)
    connect_params = {
        'host': os.getenv('host'),
        'user': os.getenv('user'),
        'password': os.getenv('password'),
        'port': os.getenv('port')
    }

    def __init__(self, employers_id: list[int], auto_create=False):
        self.emloyers_id = employers_id

        if auto_create is True:
            self.create_tables()
            self.fill_emloyers_table()
            self.fill_vacancies_table()

    def get_companies_and_vacancies_count(self):

        return self.easy_querry(
            """
            SELECT employer_name, open_vacancies FROM employers
            """
        )

    def get_all_vacancies(self):

        return self.easy_querry(
            """
            SELECT vacancy_name, employer_name, salary FROM vacancies 
            JOIN employers ON employers.id = vacancies.emp_id
            """
        )

    def get_avg_salary(self):
        return self.easy_querry(
            """
            SELECT AVG(salary) FROM vacancies
            """, fetch='one'
        )[0]

    def get_vacancies_with_higher_salary(self):
        average = self.get_avg_salary()

        return self.easy_querry(
            f"""
            SELECT vacancy_name, employer_name, salary FROM vacancies 
            JOIN employers ON employers.id = vacancies.emp_id
            WHERE salary > {average} 
            ORDER BY salary DESC
            """
        )

    def get_vacancies_with_keyword(self, keyword):
        return self.easy_querry(
            f"""
                    SELECT vacancy_name, employer_name, salary FROM vacancies 
                    JOIN employers ON employers.id = vacancies.emp_id
                    WHERE LOWER(description) LIKE '%{keyword.lower()}%' or LOWER(vacancy_name) LIKE '%{keyword.lower()}%'
                    ORDER BY salary DESC
                    """
        )

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

        workers_list = []

        conn = self.connect()

        for emp_id in self.emloyers_id:
            worker = VacDataWorker(emp_id, conn)
            worker.start()
            workers_list.append(worker)

        for worker in workers_list:
            worker.join()

        conn.commit()
        conn.close()

    def easy_querry(self, querry, fetch='all'):
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(querry)

        if fetch == 'all':
            return cur.fetchall()

        elif fetch == 'one':
            return cur.fetchone()


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

    result = my_base.get_vacancies_with_keyword('python')

    print(result)

    # Estabilished time:
    # one thread = 43.16 sec
    # vs.
    # multi threads = 14.25 sec
