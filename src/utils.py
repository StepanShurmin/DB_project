import json
import psycopg2
from src.config import config


def connect_to_database(db_name: str):
    """Подключается к базе данных."""
    try:
        params = config(dbname=db_name)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        return conn, conn.cursor()
    except psycopg2.Error as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None, None


def create_databases(params: dict, db_name: str) -> None:
    """Создает новую базу данных с таблицами в ней."""
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")

    cur.close()
    conn.close()

    params["dbname"] = db_name

    conn = psycopg2.connect(**params)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE companies (
                company_id INT PRIMARY KEY,
                company_name VARCHAR(100) NOT NULL,
                site_url VARCHAR(300),
                hh_url VARCHAR(300) NOT NULL,
                hh_vacancies_url VARCHAR(300) NOT NULL,
                area VARCHAR,
                count_open_vacancies INT,
                description TEXT
            );
        """
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE vacancies (
                vacancy_id INT PRIMARY KEY,
                vacancy_name VARCHAR(300) NOT NULL,
                salary_from INT,
                salary_to INT,
                salary_in_currency VARCHAR,
                salary_currency VARCHAR(50),
                city VARCHAR(100),
                url VARCHAR(300),
                employer_id SERIAL,
                responsibility TEXT,
                experience VARCHAR(100),
                employment VARCHAR(100)
            );
        """
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE vacancies_from_company (
                vacancy_id INT PRIMARY KEY,
                company_id INT,
                FOREIGN KEY (vacancy_id) REFERENCES vacancies (vacancy_id),
                FOREIGN KEY (company_id) REFERENCES companies (company_id)
            );
        """
        )

    conn.commit()
    conn.close()


def load_data_to_table_companies(conn, cur):
    """Загружает данные в таблицу companies."""
    try:
        with open("data_companies.json") as file:
            json_data = json.load(file)

            for company in json_data:
                company_id = int(company["id"])
                company_name = company["name"]
                description = company["description"]
                site_url = company["site_url"]
                hh_url = company["alternate_url"]
                hh_vacancies_url = company["vacancies_url"]
                area = company["area"]["name"]
                count_open_vacancies = int(company["open_vacancies"])

                cur.execute(
                    """
                    INSERT INTO companies (company_id, company_name, site_url, hh_url, hh_vacancies_url, area, 
                    count_open_vacancies, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        company_id,
                        company_name,
                        site_url,
                        hh_url,
                        hh_vacancies_url,
                        area,
                        count_open_vacancies,
                        description,
                    ),
                )

            conn.commit()

    except psycopg2.Error as e:
        print(f"Ошибка при выполнении запроса: {e}")


def load_data_to_table_vacancies(conn, cur):
    """Загружает данные в таблицу vacancies."""

    try:
        with open("data_vacancies.json") as file:
            json_data = json.load(file)

            for vacancy in json_data:
                vacancy_id = int(vacancy["id"])
                vacancy_name = vacancy["name"]

                salary_from = (
                    0
                    if vacancy["salary"] is None
                    or vacancy["salary"]["from"] is None
                    or vacancy["salary"]["from"] == "null"
                    else vacancy["salary"]["from"]
                )

                salary_to = (
                    salary_from
                    if vacancy["salary"] is None or vacancy["salary"]["to"] is None
                    else vacancy["salary"]["to"]
                )

                salary_in_currency = f"{salary_from} - {salary_to}"
                salary_currency = vacancy["salary"]["currency"] if vacancy["salary"] is not None else None

                city = vacancy["address"]["city"] if vacancy["address"] is not None else None
                url = vacancy["alternate_url"]
                employer_id = int(vacancy["employer"]["id"])  # связь с таблицей companies: company_id
                responsibility = vacancy["snippet"]["responsibility"]
                experience = vacancy["experience"]["name"]
                employment = vacancy["employment"]["name"]

                cur.execute(
                    """
                    INSERT INTO vacancies (vacancy_id, vacancy_name, salary_from, salary_to,
                    salary_currency, salary_in_currency, city, url, employer_id, responsibility, experience, employment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        vacancy_id,
                        vacancy_name,
                        salary_from,
                        salary_to,
                        salary_currency,
                        salary_in_currency,
                        city,
                        url,
                        employer_id,
                        responsibility,
                        experience,
                        employment,
                    ),
                )

            conn.commit()

    except FileNotFoundError:
        print("Файл не найден.")
    except psycopg2.Error as e:
        print(f"Ошибка при выполнении запроса: {e}")


def load_data_to_table_vacancies_in_company(conn):
    """Создаёт связывающюю таблицу по id вакансий и компаний."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vacancies_from_company (vacancy_id, company_id)
                SELECT v.vacancy_id, c.company_id
                FROM vacancies AS v 
                INNER JOIN companies AS c ON v.employer_id = c.company_id
            """
            )
            conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка при выполнении запроса: {e}")
