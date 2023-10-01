import time

import requests

from configparser import ParsingError

from src.abstract_classes import ApiJobSites


class HeadHunterAPI(ApiJobSites):
    """Класс для запроса вакансий через HH API."""

    def __init__(self):
        """
        Инициализация класса HeadHunterAPI.
        """
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.2 Safari/605.1.15"
        }
        self.params = {"per_page": 10, "page": None, "archive": False, "only_with_salary": True}
        self.vacancies = []
        self.companies = []
        self.company_ids = [1740, 78638, 3529, 2748, 2180, 4716984, 41862, 87021, 78817, 1684993]

    def get_request(self, url):
        """Отправляет GET-запрос к HH API."""
        response = requests.get(url, headers=self.headers, params=self.params)
        if response.status_code == 200:
            return response.json()
        else:
            raise ParsingError(f"Ошибка получения! Статус: {response.status_code}")

    def get_companies(self):
        """Получаем данные о компаниях через HH API."""
        for company_id in self.company_ids:
            time.sleep(1)
            url_company = f"https://api.hh.ru/employers/{company_id}"
            self.companies.append(self.get_request(url_company))
        return self.companies

    def get_vacancies(self):
        """Получаем данные о вакансиях для каждой компании"""
        for company_id in self.company_ids:
            time.sleep(1)
            url_vacancy = f"https://api.hh.ru/vacancies?employer_id={company_id}"
            self.vacancies.append(self.get_request(url_vacancy)["items"])
        dict_vacancies = [dict(item) for list in self.vacancies for item in list]
        return dict_vacancies
