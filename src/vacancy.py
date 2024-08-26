class Vacancy:
    """
    Класс вакансии
    """

    def __init__(self, vacancy_data: tuple):
        (self.id,
         self.vac_name,
         self.employer,
         self.salary,
         self.descr,
         self.vac_url) = vacancy_data

    def __str__(self):
        return (
            f"{" Вакансия ":=^100}\n"
            f"Работодатель: {self.employer}\n"
            f"Название вакансии: {self.vac_name}\n"
            f"Зарплата: {f"{self.pretty_salary(self.salary)}"} RUB\n"
            f"Описание: {f"{self.descr}" if self.descr else "Отсутствует"}\n"
            f"Ссылка на вакансию: {self.vac_url}\n"
            f"{" Конец вакансии ":=^100}\n"
        )

    def __gt__(self, other):
        if isinstance(other, Vacancy):
            my_salary = self.salary
            other_salary = other.salary
            return my_salary > other_salary

        raise TypeError("Для сравнения должен быть использован Vacancy")

    def __ge__(self, other):
        if isinstance(other, Vacancy):
            my_salary = self.salary
            other_salary = other.salary
            return my_salary >= other_salary

        raise TypeError("Для сравнения должен быть использован Vacancy")

    def __eq__(self, other):
        if isinstance(other, Vacancy):
            my_salary = self.salary
            other_salary = other.salary
            return my_salary == other_salary
        raise TypeError("Для сравнения должен быть использован Vacancy")

    @staticmethod
    def pretty_salary(value: int | str | None) -> str | None:
        """
        Функция для красивого отображения зарплаты Пример: value=100000, преобразуем в 100 000, что приятнее глазу
        :param value: Число, которое будет оформлено
        :return:
        """
        if isinstance(value, int):
            value = str(value)[::-1]
            text = ""
            for index, numb in enumerate(value, 1):
                text += numb

                if index % 3 == 0 and len(value) != index:
                    text += " "
            return text[::-1]
        return value
