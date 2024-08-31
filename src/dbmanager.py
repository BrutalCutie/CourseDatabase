from typing import Literal

import dotenv
import psycopg2
import os


from roots import ENV_DIR
from src.thread_employers import EmpDataWorker
from src.thread_vacancies import VacDataWorker
from psycopg2._psycopg import Decimal
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DBManager:
    """
    Класс для взаимодействия с базой данных.
    При создании экземпляра необходимо указать список с id работадателей,
    по которым будет созданы таблицы с данными о работодателей и данными об их вакансиях.
    """

    dotenv.load(ENV_DIR)
    connect_params = {
        'host': os.getenv('host'),
        'user': os.getenv('user'),
        'password': os.getenv('password'),
        'port': os.getenv('port')
    }

    def __init__(self, employers_id: list[int], auto_create: bool = False):
        """
        Инициализация объекта.
        :param employers_id: Список с id работодателей
        :param auto_create: Если True - заполняет
        """
        self.emloyers_id = employers_id

        if auto_create is True:
            self.create_tables()
            self.fill_emloyers_table()
            self.fill_vacancies_table()

    def set_new_employers(self, new_employer_id_list: list[int]):
        """

        :param new_employer_id_list:
        :return:
        """
        self.__truncate_table('vacancies')
        self.__truncate_table('employers')
        self.emloyers_id = new_employer_id_list
        self.fill_emloyers_table()
        self.fill_vacancies_table()

    def get_companies_and_vacancies_count(self, full_data: bool = False) -> list[tuple]:
        """
        Функция для получения списка работодотелей и количества открытый вакансий
        :return:
        """

        if not full_data:
            data = 'employer_name, open_vacancies'
        else:
            data = 'id, employer_name, open_vacancies'

        return self.easy_querry(
            f"""
            SELECT {data} FROM employers
            """
        )

    def get_employer_vacancies(self, employer: str | int, full_data: bool = False) -> list[tuple]:
        """
        Функция для получения списка всех открытых вакансий у конкретного работодателя
        :param employer: ID работодателя
        :param full_data: Вывод полной информации, поступаемой с таблицы
        :return:
        """

        if not full_data:
            data = 'vacancy_name, employer_name, salary'
        else:
            data = 'vacancies.id, vacancy_name, employer_name, salary, description, vacancy_url'

        return self.easy_querry(
            f"""
                    SELECT {data} FROM vacancies 
                    JOIN employers ON employers.id = vacancies.emp_id
                    WHERE employers.id = {str(employer)}
                    """
        )

    def get_all_vacancies(self, full_data: bool = False) -> list[tuple]:
        """
        Функция для получения списка всех открытых вакансий у всех работодателей
        :return:
        """
        if not full_data:
            data = 'vacancy_name, employer_name, salary'
        else:
            data = 'vacancies.id, vacancy_name, employer_name, salary, description, vacancy_url'

        return self.easy_querry(
            f"""
            SELECT {data} FROM vacancies 
            JOIN employers ON employers.id = vacancies.emp_id
            """
        )

    def get_avg_salary(self) -> Decimal | int:
        """
        Функция возвращает среднюю зарплату по всем вакансиям
        :return:
        """

        return self.easy_querry(
            """
            SELECT AVG(salary) FROM vacancies
            """, fetch='one'
        )[0]

    def get_vacancies_with_higher_salary(self, full_data: bool = False) -> list[tuple]:
        """
        Функция возвращает список вакансий, которые по зарплате выше чем средняя з/п по всем вакансиям
        :param full_data: Вывод полной информации, поступаемой с таблицы
        :return:
        """

        average = self.get_avg_salary()

        if not full_data:
            data = 'vacancy_name, employer_name, salary'
        else:
            data = 'vacancies.id, vacancy_name, employer_name, salary, description, vacancy_url'

        return self.easy_querry(
            f"""
            SELECT {data} FROM vacancies 
            JOIN employers ON employers.id = vacancies.emp_id
            WHERE salary > {average} 
            ORDER BY salary ASC
            """
        )

    def get_vacancies_with_keyword(self, keyword: str, full_data: bool = False) -> list[tuple]:
        """
        Функция принимает строку на вход и возвращает список вакансий, где
        есть совпадение в названии или описании вакансии.
        :param keyword: Искомое слово среди вакансий. Строка
        :param full_data: Вывод полной информации, поступаемой с таблицы
        :return:
        """

        if not full_data:
            data = 'vacancy_name, employer_name, salary'
        else:
            data = 'vacancies.id, vacancy_name, employer_name, salary, description, vacancy_url'

        return self.easy_querry(
            f"""
                    SELECT {data} FROM vacancies 
                    JOIN employers ON employers.id = vacancies.emp_id
                    WHERE LOWER(description) LIKE '%{keyword.lower()}%' or LOWER(vacancy_name) LIKE '%{keyword.lower()}%'
                    ORDER BY salary DESC
                    """
        )

    def __connect(self) -> psycopg2.connect:
        """
        Функция создаёт соединение с базой данных.
        :return:
        """
        return psycopg2.connect(**self.connect_params)

    def create_database(self, database_name: str):
        """
        Функция создаёт базу данных, если она ещё не создана и дальнейшее подключение ведётся по ней.
        """

        with self.__connect() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            cur.execute(
                """
                SELECT datname FROM pg_database
                """
            )
        ctrl_list = [x[0] == f'{database_name}' for x in cur.fetchall()]

        if not any(ctrl_list):
            cur.execute(
                f"""
                CREATE DATABASE {database_name}
                """
            )
        conn.commit()

        self.connect_params['database'] = database_name

        self.create_tables()

    def create_tables(self) -> None:
        """
        Функция создаёт необходимые таблицы, если они ещё не созданы
        :return:
        """
        conn = self.__connect()
        cur = conn.cursor()

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

        cur.execute(
            """
            DROP TABLE IF EXISTS vacancies CASCADE;

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

        conn.commit()
        conn.close()

    def fill_emloyers_table(self) -> None:
        """
        Функция заполняет таблицы работодателей, на основании переданных id в экземпляр
        :return:
        """

        workers_list = []

        self.__truncate_table('employers')

        conn = self.__connect()

        for employer in self.emloyers_id:
            t_worker = EmpDataWorker(employer, conn)
            workers_list.append(t_worker)
            t_worker.start()

        for worker in workers_list:
            worker.join()

        conn.commit()
        conn.close()

    def fill_vacancies_table(self):
        """
        Функция заполняет таблицы вакансий, на основании записей таблицы работодателей.
        :return:
        """

        workers_list = []

        self.__truncate_table('vacancies')

        conn = self.__connect()

        for emp_id in self.emloyers_id:
            worker = VacDataWorker(emp_id, conn)
            worker.start()
            workers_list.append(worker)

        for worker in workers_list:
            worker.join()

        conn.commit()
        conn.close()

    def easy_querry(self, querry, fetch: Literal['all', 'one', 'without'] = 'all'):
        """
        Функция для простого запроса к базе данных. Вспомогательная функция
        чтобы не писать рутинный код. DRY(Don't Repeat Yourself).

        :param querry: Запрос к базе данных на языке SQL.
        :param fetch: Указывает на возвращаемые данные.
            all = .fetchall();
            one = .fetchone();
            without = без возврата данных
        :return:
        """

        with self.__connect() as conn:
            cur = conn.cursor()
            cur.execute(querry)

        if fetch == 'all':
            return cur.fetchall()

        elif fetch == 'one':
            return cur.fetchone()

        elif fetch == 'without':
            return None

    def __truncate_table(self, table_name: str) -> None:
        """
        Функция очищает переданную таблицу
        :param table_name: название таблицы
        :return:
        """

        self.easy_querry(
            f"""
            TRUNCATE TABLE {table_name} CASCADE
            """, fetch='without'
        )


# Estabilished time:
# one thread = 43.16 sec
# vs.
# multi threads = 14.25 sec
