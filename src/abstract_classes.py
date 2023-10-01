from abc import abstractmethod, ABC


class FILESaver(ABC):
    """Абстрактный класс для работы с полученными данными с сайтов с вакансиями."""

    @abstractmethod
    def save_data(self, json_file):
        pass


class ApiJobSites(ABC):
    """Абстрактный класс для работы с API сайтов с вакансиями."""

    @abstractmethod
    def get_request(self, url):
        pass

    @abstractmethod
    def get_vacancies(self):
        pass

    @abstractmethod
    def get_companies(self):
        pass
