from pprint import pprint

from src.config import config
from src.hh_parser import HeadHunterAPI
from src.json_saver import JSONSaver
from src.utils import (
    connect_to_database,
    create_databases,
    load_data_to_table_companies,
    load_data_to_table_vacancies,
    load_data_to_table_vacancies_in_company,
)

from src.db_manager import DBManager


def main():
    """
    Подключается к сайту для получения нужной информации.
    Создаёт БД с таблицами, подключается к ней, записывает необходимую
    информацию в таблицы и связывает их.
    """

    hh = HeadHunterAPI()

    json_companies = JSONSaver(hh.get_companies(), "companies")
    json_vacancies = JSONSaver(hh.get_vacancies(), "vacancies")
    json_companies.save_data()
    json_vacancies.save_data()

    params = config()

    create_databases(params, "db_hh_vcs")

    conn, cur = connect_to_database("db_hh_vcs")

    if conn is not None and cur is not None:
        load_data_to_table_companies(conn, cur)

        load_data_to_table_vacancies(conn, cur)

        load_data_to_table_vacancies_in_company(conn)
        print("Данные загружены в таблицы.\n")

        db_manager = DBManager(conn, cur)
        print("\nСписок всех компаний и количество вакансий у каждой компании.")
        count_vacancy_in_company = db_manager.get_companies_and_vacancies_count()
        print(f"Всего вакансий: {len(count_vacancy_in_company)}\n")
        pprint(count_vacancy_in_company)

        print(
            "\n\nСписок всех вакансий с указанием названия компании, "
            "названия вакансии, зарплаты и ссылки на вакансию."
        )
        all_vacancy = db_manager.get_all_vacancies()
        print(f"Всего вакансий: {len(all_vacancy)}\n")
        print(all_vacancy)

        print(f"Средняя зарплата = {db_manager.get_avg_salary()} руб.\n")

        print("\nСписок всех вакансий, у которых зарплата выше средней по всем вакансиям, по убыванию средней зп.")
        vacancies_with_higher_salary = db_manager.get_vacancies_with_higher_salary()
        print(f"Всего вакансий: {len(vacancies_with_higher_salary)}\n")
        pprint(vacancies_with_higher_salary)

        print("\n\nСписок всех вакансий, в названии которых содержатся переданные в метод слова")
        keyword = "java"
        vacancies_with_keyword = db_manager.get_vacancies_with_keyword(keyword)
        print(f"Всего вакансий: {len(vacancies_with_keyword)}\n")
        pprint(vacancies_with_keyword)

        cur.close()
        conn.close()

    else:
        print(f"Ошибка при подключении к базе данных.")


if __name__ == "__main__":
    main()
