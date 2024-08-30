import time

from src.dbmanager import DBManager
from src.employer import Employer
from src.vacancy import Vacancy


class Interface:
    """Интерфейс для работы с пользователем"""
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
    own_employers = []

    def __init__(self):
        print('Приветствую пользователь!\n'
              'Я программа для получения данных о работодателей и их вакансиях.\n'
              'Следуй за инструкциями программы.')
        self.db = DBManager(self.own_employers, auto_create=True)
        self.main_menu()

    @staticmethod
    def is_digit(dig_str: str):
        """Проверка, что строка состоит только из чисел."""

        return dig_str.isdigit() is True

    def employers_menu(self):
        """Меню интерфейса для работы с работодателями"""

        print('\nМЕНЮ ВЫБОРА РАБОТОДАТЕЛЕЙ')
        print("1. Вернуться в главное меню")
        print("2. Ввод своих работодателей")
        print("3. Использовать предустановленные параметры")

        employers_menu_choice = input("\nВыберите действие: ")

        if employers_menu_choice not in list(map(str, range(1, 4))):
            print('\nТакого выбора не существует! Выберите из возможных, представленных ниже\n\n')
            return self.employers_menu()

        elif employers_menu_choice == '1':
            return self.main_menu()

        elif employers_menu_choice == '2':
            print("\nВвод своих ID работодателей осуществляется через пробел. "
                  "Например>|10124 421 4124|< Будут добавлены 3 ID")
            print("Для возврата в меню: back")

            user_employers_id = input("Введите ID: ")

            if user_employers_id.lower() == 'back':
                return self.employers_menu()

            chk_list = user_employers_id.split()

            if all(list(map(self.is_digit, chk_list))):
                print("\nИдёт поиск работодателей и их вакансий", end='')
                chk_list = list(map(int, chk_list))
                self.db.set_new_employers(chk_list)
                print('\rВсе работодатели и вакансии успешно добавлены')
                time.sleep(2)
                return self.main_menu()
            else:
                print("ID предполагает только наличие цифр. Попробуйте снова.")
                time.sleep(2)
                return self.employers_menu()

        elif employers_menu_choice == '3':
            print("\nИдёт поиск работодателей и их вакансий", end='')
            self.db.set_new_employers(self.employers_ids)
            print('\rВсе работодатели и вакансии успешно добавлены')
            time.sleep(2)
            return self.main_menu()

    def vacancies_menu(self):
        """Меню интерфейса для работы с вакансиями"""

        print("\nМЕНЮ РАБОТЫ С ВАКАНСИЯМИ")
        print("1. Вернуться в главное меню")
        print("2. Вывод всех вакансий")
        print("3. Вывод вакансий определенного работодателя")
        print("4. Вывод средней зарплаты по всем вакансиям")
        print("5. Вывод вакансий, у которых зарплата выше средней")
        print("6. Вывод вакансий по ключевому слову")

        employers_menu_choice = input("\nВыберите действие: ")

        if employers_menu_choice not in list(map(str, range(1, 7))):
            print('\nТакого выбора не существует! Выберите из возможных, представленных ниже\n\n')
            return self.vacancies_menu()

        elif employers_menu_choice == '1':
            return self.main_menu()

        elif employers_menu_choice == '2':
            result = self.db.get_all_vacancies(full_data=True)

            vacs_list = list(map(Vacancy, result))
            for vacancy in vacs_list:
                print(vacancy)

            return self.vacancies_menu()

        elif employers_menu_choice == '3':
            companies_and_vacancies_count = self.db.get_companies_and_vacancies_count(full_data=True)
            employers_list = list(map(Employer, companies_and_vacancies_count))

            print(f"| {'ID':^15} | {'Компания':^40} | {'Вакансий':^10}|\n")

            posible_values = []
            for employer in employers_list:
                print(employer)
                posible_values.append(str(employer.emp_id))

            print('\nДля возврата в меню: back')
            user_employer_id = input('Введите ID работодателя для вывода вакансий: ')

            if user_employer_id.lower() == 'back':
                return self.vacancies_menu()

            elif user_employer_id not in posible_values:
                print('ID такого работодателя не существует в базе данных.')
                print('Добавьте его через Главное меню > Выбор работодателей > Ввод своих работодателей\n')
                return self.vacancies_menu()

            else:
                result = self.db.get_employer_vacancies(user_employer_id, full_data=True)
                vacs_list = list(map(Vacancy, result))
                for vacancy in vacs_list:
                    print(vacancy)

                return self.vacancies_menu()

        elif employers_menu_choice == '4':
            average_salary = round(self.db.get_avg_salary(), 2)

            print(f'\nСредняя зарплата по всем вакансиям составляет {average_salary} RUB')

            return self.vacancies_menu()

        elif employers_menu_choice == '5':
            result = self.db.get_vacancies_with_higher_salary(full_data=True)

            vacs_list = list(map(Vacancy, result))
            for vacancy in vacs_list:
                print(vacancy)

            return self.vacancies_menu()

        elif employers_menu_choice == '6':

            print('Для возврата в меню: back')

            user_vacancy_keyword = input('\nВведите ключевое слово для вывода вакансий с совпадением: ')

            if user_vacancy_keyword.lower() == 'back':
                return self.vacancies_menu()

            result = self.db.get_vacancies_with_keyword(user_vacancy_keyword.lower(), full_data=True)

            vacs_list = list(map(Vacancy, result))
            for vacancy in vacs_list:
                print(vacancy)

            return self.vacancies_menu()

    def main_menu(self):
        """Главное меню интерфейса"""

        print('\nГЛАВНОЕ МЕНЮ')
        print("1. Выбор работодателей")
        print("2. Работа по выводу вакансий")
        print("3. Завершение работы")

        main_menu_choice = input("\nВыберите действие: ")

        if main_menu_choice not in list(map(str, range(1, 4))):
            res = list(map(str, range(1, 3)))
            print(res, type(res))
            print('\nТакого выбора не существует! Выберите из возможных, представленных ниже\n\n')
            return self.main_menu()
        elif main_menu_choice == '1':
            return self.employers_menu()

        elif main_menu_choice == '2':
            return self.vacancies_menu()

        elif main_menu_choice == '3':
            print('\nУдачи в поиске новых вакансий!')
            exit()
