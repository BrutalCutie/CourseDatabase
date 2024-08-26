from src.dbmanager import DBManager

# Для работы необходимо передать список с ID работодателей.
# Как пример используем отобранные мной работодателей

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


# Инициируем объект класса DBManager
my_base = DBManager(employers_id=employers_ids, auto_create=True)
# auto_create=True необходим для сокращения кода. Без указания данного параметра код будет выглядеть так:
# my_base = DBManager(employers_id=employers_ids)
# my_base.create_tables()  # Создание необходимых таблиц
# my_base.fill_emloyers_table()  # Заполнение данных о работодателях на основании ID работодателей
# my_base.fill_vacancies_table()  # Заполнение данных о вакансиях на основании заполненной таблицы


# Вывод всех работодателей и количесва открытых вакансий
companies_and_vacancies_count = my_base.get_companies_and_vacancies_count()
# print(companies_and_vacancies_count)

# Вывод всех вакансий всех работодателей
all_vacs = my_base.get_all_vacancies(full_data=True)
# print(all_vacs)

# Вывод всех вакансий конкретного работодателя
employer_vacancies = my_base.get_employer_vacancies(15478, full_data=True)
# print(employer_vacancies)

# Вывод средней зарплаты по всем работодателям
average_salary = my_base.get_avg_salary()
# print(average_salary)

# Вывод вакансий с зарплатой выше чем средняя
vacancies_with_higher_salary = my_base.get_vacancies_with_higher_salary()
# print(vacancies_with_higher_salary)

# Вывод вакансий, где в названии или описании есть совпадение по поисковому слову
vacancies_with_keyword = my_base.get_vacancies_with_keyword('python')
# print(vacancies_with_keyword)

