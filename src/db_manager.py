import psycopg2


class DBManager:
    """Инициализация класса DBManager."""

    def __init__(self, conn, cur):
        self.cur = cur
        self.conn = conn

    def get_companies_and_vacancies_count(self) -> list[dict]:
        """Получает список всех компаний и количество вакансий у каждой компании."""
        try:
            self.cur.execute(
                """
                SELECT vfc.company_id, c.company_name,
                COUNT(vfc.vacancy_id) AS all_vacancies
                FROM vacancies_from_company AS vfc
                JOIN companies AS c USING (company_id)
                GROUP BY company_id, c.company_name;
                """,
            )

            data_tuples = self.cur.fetchall()

            keys = ["company_id", "company_name", "count_open_vacancies"]
            dict_data = []
            for data in data_tuples:
                dict_data.append(dict(zip(keys, data)))

            return dict_data

        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")

    def get_all_vacancies(self) -> list[dict]:
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        try:
            self.cur.execute(
                """
                SELECT v.vacancy_id, c.company_name, v.vacancy_name,
                COALESCE(CONCAT(v.salary_in_currency, ' ', v.salary_currency), 'Нет данных.') as salary, v.url
                FROM vacancies AS v
                JOIN vacancies_from_company AS vfc USING (vacancy_id)
                JOIN companies AS c USING (company_id)
                ORDER BY company_name;
                """,
            )

            data_tuples = self.cur.fetchall()

            keys = ["vacancy_id", "company_name", "vacancy_name", "salary", "url"]
            dict_data = []
            for data in data_tuples:
                dict_data.append(dict(zip(keys, data)))

            return dict_data

        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")

    def get_avg_salary(self) -> float:
        """Получает среднюю зарплату по вакансиям."""
        try:
            self.cur.execute(
                f"""
                SELECT ROUND(AVG((COALESCE(salary_from, 0) + COALESCE(salary_to, 0))/2)) AS average_salary
                FROM vacancies
                WHERE CONCAT(COALESCE(salary_from, 0), COALESCE(salary_to, 0)) != '0 0';
                """,
            )

            result = self.cur.fetchone()
            return int(result[0])

        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return 0

    def get_vacancies_with_higher_salary(self) -> list[dict]:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        Отсортирован по убыванию средней зп.
        """
        try:
            self.cur.execute(
                f"""
                SELECT v.vacancy_id, v.vacancy_name, c.company_name,
                CONCAT(v.salary_in_currency, ' ', v.salary_currency) AS salary,
                CONCAT((v.salary_from + v.salary_to)/ 2, ' ', 'RUB') AS avg_salary_in_rub, 
                v.city, v.url, v.responsibility, v.experience, v.employment
                FROM vacancies AS v
                JOIN companies AS c ON v.employer_id = c.company_id
                WHERE ((v.salary_to + v.salary_from)/ 2) > ({self.get_avg_salary()}) 
                ORDER BY (v.salary_from + v.salary_to)/ 2 DESC;
                """,
            )

            data_tuples = self.cur.fetchall()

            keys = [
                "vacancy_id",
                "vacancy_name",
                "company_name",
                "salary",
                "avg_salary_in_rub",
                "city",
                "url",
                "responsibility",
                "experience",
                "employment",
            ]
            dict_data = []
            for data in data_tuples:
                dict_data.append(dict(zip(keys, data)))

            return dict_data

        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")

    def get_vacancies_with_keyword(self, word: str) -> list[dict]:
        """Получает список всех вакансий,
        в названии которых содержатся переданные в метод слова, например 'python'."""
        try:
            self.cur.execute(
                f"""
                SELECT v.vacancy_id, v.vacancy_name, c.company_name,
                CONCAT(v.salary_in_currency, ' ', v.salary_currency) AS salary,
                CONCAT((v.salary_from + v.salary_to)/2, ' ', 'RUB') AS avg_salary_in_rub,
                v.city, v.url, v.responsibility, v.experience, v.employment
                FROM vacancies AS v
                JOIN companies AS c ON employer_id = company_id
                WHERE v.vacancy_name LIKE '%{word.title()}%'
                ORDER BY v.vacancy_name;

                """
            )

            data_tuples = self.cur.fetchall()

            keys = [
                "vacancy_id",
                "vacancy_name",
                "company_name",
                "salary",
                "avg_salary_in_rub",
                "city",
                "url",
                "responsibility",
                "experience",
                "employment",
            ]
            dict_data = []
            for data in data_tuples:
                dict_data.append(dict(zip(keys, data)))

            return dict_data

        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")
