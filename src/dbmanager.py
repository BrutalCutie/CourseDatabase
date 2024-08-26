from typing import Literal

import dotenv
import psycopg2
import os


from roots import ENV_DIR
from src.employers import EmpDataWorker
from src.vacancies import VacDataWorker
from psycopg2._psycopg import Decimal


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

    def get_companies_and_vacancies_count(self) -> list[tuple]:
        """
        Функция для получения списка работодотелей и количества открытый вакансий
        :return:
        """

        return self.easy_querry(
            """
            SELECT employer_name, open_vacancies FROM employers
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

    def get_vacancies_with_higher_salary(self) -> list[tuple]:
        """
        Функция возвращает список вакансий, которые по зарплате выше чем средняя з/п по всем вакансиям
        :return:
        """

        average = self.get_avg_salary()

        return self.easy_querry(
            f"""
            SELECT vacancy_name, employer_name, salary FROM vacancies 
            JOIN employers ON employers.id = vacancies.emp_id
            WHERE salary > {average} 
            ORDER BY salary DESC
            """
        )

    def get_vacancies_with_keyword(self, keyword: str) -> list[tuple]:
        """
        Функция принимает строку на вход и возвращает список вакансий, где
        есть совпадение в названии или описании вакансии.
        :param keyword: Искомое слово среди вакансий. Строка
        :return:
        """

        return self.easy_querry(
            f"""
                    SELECT vacancy_name, employer_name, salary FROM vacancies 
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

    def create_tables(self) -> None:
        """
        Функция создаёт необходимые таблицы, если они ещё не созданы
        :return:
        """
        conn = self.__connect()
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

    def __truncate_table(self, table_name: str) -> None:
        """
        Функция очищает переданную таблицу
        :param table_name: название таблицы
        :return:
        """

        self.easy_querry(
            f"""
            TRUNCATE TABLE {table_name}
            """, fetch='without'
        )


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

    # Estabilished time:
    # one thread = 43.16 sec
    # vs.
    # multi threads = 14.25 sec
