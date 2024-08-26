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
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT employer_name, open_vacancies FROM employers
                """
            )

            return cur.fetchall()

    def get_all_vacancies(self):
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT vacancy_name, employer_name, salary FROM vacancies 
                JOIN employers ON employers.id = vacancies.emp_id
                """
            )

            return cur.fetchall()

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

    print(my_base.get_companies_and_vacancies_count())



    # Estabilished time:
    # one thread = 43.16 sec
    # vs.
    # multi threads = 14.25 sec
